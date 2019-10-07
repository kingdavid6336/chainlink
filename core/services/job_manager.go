package services

import (
	"fmt"
	"math/big"

	"github.com/pkg/errors"
	"github.com/smartcontractkit/chainlink/core/adapters"
	"github.com/smartcontractkit/chainlink/core/logger"
	clnull "github.com/smartcontractkit/chainlink/core/null"
	"github.com/smartcontractkit/chainlink/core/store"
	"github.com/smartcontractkit/chainlink/core/store/assets"
	"github.com/smartcontractkit/chainlink/core/store/models"
	"github.com/smartcontractkit/chainlink/core/store/orm"
	"github.com/smartcontractkit/chainlink/core/utils"
)

// RecurringScheduleJobError contains the field for the error message.
type RecurringScheduleJobError struct {
	msg string
}

// Error returns the error message for the run.
func (err RecurringScheduleJobError) Error() string {
	return err.msg
}

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

	ResumeInProgressTasks() error

	CancelTask(runID *models.ID) error
}

// jobManager implements JobManager
type jobManager struct {
	orm       *orm.ORM
	jobRunner JobRunner
	txManager store.TxManager
	config    orm.ConfigReader
	clock     utils.AfterNower
}

// NewRun returns a run from an input job, in an initial state ready for
// processing by the job runner system
func NewRun(
	job *models.JobSpec,
	initiator *models.Initiator,
	input *models.RunResult,
	currentHeight *big.Int,
	payment *assets.Link,
	config orm.ConfigReader,
	orm *orm.ORM,
	clock utils.AfterNower,
	txManager store.TxManager) (*models.JobRun, error) {

	now := clock.Now()
	if !job.Started(now) {
		return nil, RecurringScheduleJobError{
			msg: fmt.Sprintf("Job runner: Job %v unstarted: %v before job's start time %v", job.ID, now, job.EndAt),
		}
	}

	if job.Ended(now) {
		return nil, RecurringScheduleJobError{
			msg: fmt.Sprintf("Job runner: Job %v ended: %v past job's end time %v", job.ID, now, job.EndAt),
		}
	}

	run := job.NewRun(*initiator)

	run.Overrides = *input
	run.ApplyResult(*input)
	run.CreationHeight = models.NewBig(currentHeight)
	run.ObservedHeight = models.NewBig(currentHeight)

	if !MeetsMinimumPayment(job.MinPayment, payment) {
		logger.Infow("Rejecting run with insufficient payment", []interface{}{
			"run", run.ID,
			"job", run.JobSpecID,
			"input_payment", payment,
			"required_payment", job.MinPayment,
		}...)

		err := fmt.Errorf(
			"Rejecting job %s with payment %s below job-specific-minimum threshold (%s)",
			job.ID,
			payment,
			job.MinPayment.Text(10))
		run.SetError(err)
	}

	cost := assets.NewLink(0)
	for i, taskRun := range run.TaskRuns {
		adapter, err := adapters.For(taskRun.TaskSpec, config, orm)

		if err != nil {
			run.SetError(err)
			return &run, nil
		}

		mp := adapter.MinContractPayment()
		if mp != nil {
			cost.Add(cost, mp)
		}

		if currentHeight != nil {
			run.TaskRuns[i].MinimumConfirmations = clnull.Uint32From(
				utils.MaxUint32(
					config.MinIncomingConfirmations(),
					taskRun.TaskSpec.Confirmations.Uint32,
					adapter.MinConfs()),
			)
		}
	}

	// payment is always present for runs triggered by ethlogs
	if payment != nil {
		if cost.Cmp(payment) > 0 {
			logger.Debugw("Rejecting run with insufficient payment", []interface{}{
				"run", run.ID,
				"job", run.JobSpecID,
				"input_payment", payment,
				"required_payment", cost,
			}...)

			err := fmt.Errorf(
				"Rejecting job %s with payment %s below minimum threshold (%s)",
				job.ID,
				payment,
				config.MinimumContractPayment().Text(10))
			run.SetError(err)
		}
	}

	if len(run.TaskRuns) == 0 {
		run.SetError(fmt.Errorf("invariant for job %s: no tasks to run in NewRun", job.ID))
	}

	if !run.Status.Runnable() {
		return &run, nil
	}

	initialTask := run.TaskRuns[0]
	validateMinimumConfirmations(&run, &initialTask, run.CreationHeight, txManager)
	return &run, nil
}

// NewJobManager returns a new job manager
func NewJobManager(jobRunner JobRunner, config orm.ConfigReader, orm *orm.ORM, txManager store.TxManager, clock utils.AfterNower) JobManager {
	return &jobManager{
		orm:       orm,
		jobRunner: jobRunner,
		txManager: txManager,
		config:    config,
		clock:     clock,
	}
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

	job, err := jm.orm.Unscoped().FindJob(jobSpecID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to find job spec")
	}

	if job.Archived() {
		return nil, RecurringScheduleJobError{
			msg: fmt.Sprintf("Trying to run archived job %s", job.ID),
		}
	}

	run, err := NewRun(&job, initiator, input, creationHeight, runRequest.Payment, jm.config, jm.orm, jm.clock, jm.txManager)
	if err != nil {
		return nil, errors.Wrap(err, "NewRun failed")
	}

	run.RunRequest = *runRequest

	if err := jm.orm.CreateJobRun(run); err != nil {
		return nil, errors.Wrap(err, "CreateJobRun failed")
	}

	if run.Status == models.RunStatusInProgress {
		logger.Debugw(
			fmt.Sprintf("Executing run originally initiated by %s", run.Initiator.Type),
			run.ForLogger()...,
		)
		return run, jm.jobRunner.Run(run)
	}

	return run, nil
}

// ResumeConfirmingTasks wakes up all jobs that were sleeping because they were
// waiting for block confirmations.
func (jm *jobManager) ResumeConfirmingTasks(currentBlockHeight *big.Int) error {
	return jm.orm.UnscopedJobRunsWithStatus(func(run *models.JobRun) {
		err := ResumeConfirmingTask(run, currentBlockHeight, jm.txManager)
		if err != nil {
			logger.Error(err)
			return
		}

		jm.updateAndTrigger(run)
	}, models.RunStatusPendingConnection, models.RunStatusPendingConfirmations)
}

// ResumeConfirmingTask wakes up a task that was sleeping because it is waiting
// for block confirmations.
func ResumeConfirmingTask(run *models.JobRun, currentBlockHeight *big.Int, txManager store.TxManager) error {
	currentTaskRun := run.NextTaskRun()
	if currentTaskRun == nil {
		return fmt.Errorf("Attempting to resume confirming run with no remaining tasks %s", run.ID)
	}

	run.ObservedHeight = models.NewBig(currentBlockHeight)
	logger.Debugw(fmt.Sprintf("New head #%s resuming run", currentBlockHeight), run.ForLogger()...)

	validateMinimumConfirmations(run, currentTaskRun, run.ObservedHeight, txManager)
	return nil
}

// ResumeConnectingTasks wakes up all tasks that have gone to sleep because
// they needed an ethereum client connection.
func (jm *jobManager) ResumeConnectingTasks() error {
	return jm.orm.UnscopedJobRunsWithStatus(func(run *models.JobRun) {
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

// ResumeConnectingTask wakes up a task that has gone to sleep because
// it required an ethereum connection
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
	run, err := jm.orm.Unscoped().FindJobRun(runID)
	if err != nil {
		return err
	}

	err = ResumePendingTask(&run, input)
	if err != nil {
		return err
	}

	return jm.updateAndTrigger(&run)
}

// ResumePendingTask wakes up a task that required a response from a bridge adapter.
func ResumePendingTask(run *models.JobRun, input models.RunResult) error {
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
	} else {
		run.ApplyResult(input)
	}

	return nil
}

// ResumeInProgressTasks queries the db for job runs that should be resumed
// since a previous node shutdown.
//
// As a result of its reliance on the database, it must run before anything
// persists a job RunStatus to the db to ensure that it only captures pending and in progress
// jobs as a result of the last shutdown, and not as a result of what's happening now.
//
// To recap: This must run before anything else writes job run status to the db,
// ie. tries to run a job.
// https://github.com/smartcontractkit/chainlink/pull/807
func (jm *jobManager) ResumeInProgressTasks() error {
	return jm.orm.UnscopedJobRunsWithStatus(func(run *models.JobRun) {
		if err := jm.jobRunner.Run(run); err != nil {
			logger.Errorw("Error resuming job run", "error", err, "run", run.ID.String())
		}
	}, models.RunStatusInProgress, models.RunStatusPendingSleep)
}

// CancelTask suspends a running task.
func (jm *jobManager) CancelTask(runID *models.ID) error {
	run, err := jm.orm.FindJobRun(runID)
	if err != nil {
		return err
	}

	logger.Debugw("Cancelling job", []interface{}{
		"run", run.ID,
		"job", run.JobSpecID,
		"status", run.Status,
	}...)

	if !run.Status.Runnable() {
		return fmt.Errorf("Cannot cancell a non runnable job")
	}

	run.Status = models.RunStatusCancelled
	return jm.orm.SaveJobRun(&run)
}

func (jm *jobManager) updateAndTrigger(run *models.JobRun) error {
	if err := jm.orm.SaveJobRun(run); err != nil {
		return err
	}
	if run.Status == models.RunStatusInProgress {
		return jm.jobRunner.Run(run)
	}
	return nil
}
