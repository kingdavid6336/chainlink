package services

import (
	"fmt"
	"math/big"

	"github.com/pkg/errors"
	"github.com/smartcontractkit/chainlink/core/logger"
	"github.com/smartcontractkit/chainlink/core/store"
	"github.com/smartcontractkit/chainlink/core/store/models"
)

//go:generate mockery -name JobManager -output ../internal/mocks/ -case=underscore

// JobManager supplies methods for queueing, resuming and cancelling jobs in
// the JobRunner
type JobManager interface {
	ExecuteJob(
		jobSpecID *models.ID,
		initiator *models.Initiator,
		input *models.RunResult,
		creationHeight *big.Int) (*models.JobRun, error)
	ExecuteJobWithRunRequest(
		jobSpecID *models.ID,
		initiator *models.Initiator,
		input *models.RunResult,
		creationHeight *big.Int,
		runRequest *models.RunRequest) (*models.JobRun, error)

	ResumeConfirmingTasks(currentBlockHeight *big.Int) error
	ResumeConnectingTasks() error
	ResumePendingTask(runID *models.ID, input models.RunResult) error
	CancelTask(runID *models.ID) error
}

// jobManager implements JobManager
type jobManager struct {
	store   *store.Store
	updates chan *runUpdate
}

type runUpdate struct {
	run    *models.JobRun
	result chan error
}

// NewJobManager returns a new job manager
func NewJobManager(store *store.Store) JobManager {
	jm := &jobManager{store: store, updates: make(chan *runUpdate)}
	go jm.run()
	return jm
}

// ExecuteJob immediately sends a new job to the JobRunner for execution
func (jm *jobManager) ExecuteJob(
	jobSpecID *models.ID,
	initiator *models.Initiator,
	input *models.RunResult,
	creationHeight *big.Int,
) (*models.JobRun, error) {
	return jm.ExecuteJobWithRunRequest(
		jobSpecID,
		initiator,
		input,
		creationHeight,
		models.NewRunRequest(),
	)
}

// ExecuteJob immediately sends a new job to the JobRunner for execution given
// a RunRequest.
func (jm *jobManager) ExecuteJobWithRunRequest(
	jobSpecID *models.ID,
	initiator *models.Initiator,
	input *models.RunResult,
	creationHeight *big.Int,
	runRequest *models.RunRequest,
) (*models.JobRun, error) {
	logger.Debugw(fmt.Sprintf("New run triggered by %s", initiator.Type),
		"job", jobSpecID,
		"input_status", input.Status,
		"creation_height", creationHeight,
	)

	job, err := jm.store.Unscoped().FindJob(jobSpecID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to find job spec")
	}

	if job.Archived() {
		return nil, RecurringScheduleJobError{
			msg: fmt.Sprintf("Trying to run archived job %s", job.ID),
		}
	}

	run, err := NewRun(&job, initiator, input, creationHeight, jm.store, runRequest.Payment)
	if err != nil {
		return nil, errors.Wrap(err, "NewRun failed")
	}

	run.RunRequest = *runRequest
	return run, createAndTrigger(run, jm.store)
}

// ResumeConfirmingTasks wakes up all jobs that were sleeping because they were
// waiting for block confirmations.
func (jm *jobManager) ResumeConfirmingTasks(currentBlockHeight *big.Int) error {
	return jm.store.UnscopedJobRunsWithStatus(func(run *models.JobRun) {
		logger.Debugw("New head resuming run", run.ForLogger()...)
		err := ResumeConfirmingTask(run, currentBlockHeight, jm.store.TxManager)
		if err != nil {
			logger.Error(err)
		}

		jm.updateAndTrigger(run)
	}, models.RunStatusPendingConnection, models.RunStatusPendingConfirmations)
}

func ResumeConfirmingTask(run *models.JobRun, currentBlockHeight *big.Int, txManager store.TxManager) error {
	currentTaskRun := run.NextTaskRun()
	if currentTaskRun == nil {
		return fmt.Errorf("Attempting to resume confirming run with no remaining tasks %s", run.ID)
	}

	run.ObservedHeight = models.NewBig(currentBlockHeight)

	validateMinimumConfirmations(run, currentTaskRun, run.ObservedHeight, txManager)
	return nil
}

// ResumeConnectingTasks wakes up all tasks that have gone to sleep because
// they needed an ethereum client connection.
func (jm *jobManager) ResumeConnectingTasks() error {
	return jm.store.UnscopedJobRunsWithStatus(func(run *models.JobRun) {
		logger.Debugw("New connection resuming run", run.ForLogger()...)

		currentTaskRun := run.NextTaskRun()
		if currentTaskRun == nil {
			logger.Error("Attempting to resume connecting run with no remaining tasks %s", run.ID)
			return
		}

		run.Status = models.RunStatusInProgress
		jm.updateAndTrigger(run)
	}, models.RunStatusPendingConnection, models.RunStatusPendingConfirmations)
}

func ResumeConnectingTask(run *models.JobRun) error {
	logger.Debugw("New connection resuming run", run.ForLogger()...)

	currentTaskRun := run.NextTaskRun()
	if currentTaskRun == nil {
		return fmt.Errorf("Attempting to resume connecting run with no remaining tasks %s", run.ID)
	}

	run.Status = models.RunStatusInProgress
	return nil
}

// ResumePendingTask wakes up a task that required a response from a bridge adapter.
func (jm *jobManager) ResumePendingTask(
	runID *models.ID,
	input models.RunResult,
) error {
	run, err := jm.store.Unscoped().FindJobRun(runID)
	if err != nil {
		return err
	}

	err = ResumePendingTask(&run, input)
	if err != nil {
		return err
	}

	return jm.updateAndTrigger(&run)
}

func ResumePendingTask(
	run *models.JobRun,
	input models.RunResult,
) error {
	logger.Debugw("External adapter resuming job", []interface{}{
		"run", run.ID,
		"job", run.JobSpecID,
		"status", run.Status,
		"input_data", input.Data,
		"input_result", input.Status,
	}...)

	if !run.Status.PendingBridge() {
		return fmt.Errorf("Attempting to resume non pending run %s", run.ID)
	}

	currentTaskRun := run.NextTaskRun()
	if currentTaskRun == nil {
		return fmt.Errorf("Attempting to resume pending run with no remaining tasks %s", run.ID)
	}

	run.Overrides.Merge(input)

	currentTaskRun.ApplyResult(input)
	if currentTaskRun.Status.Finished() && run.TasksRemain() {
		run.Status = models.RunStatusInProgress
	} else if currentTaskRun.Status.Finished() {
		run.ApplyResult(input)
		run.SetFinishedAt()
	} else {
		run.ApplyResult(input)
	}

	return nil
}

// CancelTask suspends a running task.
func (jm *jobManager) CancelTask(
	runID *models.ID,
) error {
	run, err := jm.store.FindJobRun(runID)
	if err != nil {
		return err
	}

	logger.Debugw("Cancelling job", []interface{}{
		"run", run.ID,
		"job", run.JobSpecID,
		"status", run.Status,
	}...)

	return nil
}

func (jm *jobManager) run() {
	for {
		select {
		case update := <-jm.updates:
			if err := jm.store.SaveJobRun(update.run); err != nil {
				update.result <- err
				continue
			}
			update.result <- triggerIfReady(update.run, jm.store)
		}
	}
}

func (jm *jobManager) updateAndTrigger(run *models.JobRun) error {
	update := &runUpdate{
		run:    run,
		result: make(chan error),
	}
	jm.updates <- update
	return <-update.result
}
