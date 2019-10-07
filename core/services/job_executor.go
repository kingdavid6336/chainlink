package services

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/smartcontractkit/chainlink/core/adapters"
	"github.com/smartcontractkit/chainlink/core/logger"
	"github.com/smartcontractkit/chainlink/core/store"
	"github.com/smartcontractkit/chainlink/core/store/models"
)

//go:generate mockery -name JobExecutor -output ../internal/mocks/ -case=underscore

// JobExecutor handles the actual running of the job tasks
type JobExecutor interface {
	Execute(*models.ID) error
}

type jobExecutor struct {
	store *store.Store
}

// NewJobExecutor initializes a JobExecutor.
func NewJobExecutor(store *store.Store) JobExecutor {
	return &jobExecutor{
		store: store,
	}
}

// Execute performs the work associate with a job run
func (je *jobExecutor) Execute(runID *models.ID) error {
	run, err := je.store.Unscoped().FindJobRun(runID)
	if err != nil {
		return fmt.Errorf("Error finding run %s", runID.String())
	}

	if !run.Status.Runnable() {
		return fmt.Errorf("Run triggered in non runnable state %s", run.Status)
	}

	for run.Status.Runnable() {
		currentTaskRun := run.NextTaskRun()
		if currentTaskRun == nil {
			return errors.New("Run triggered with no remaining tasks")
		}

		result := je.executeTask(&run, currentTaskRun)

		currentTaskRun.ApplyResult(result)
		run.ApplyResult(result)

		if !currentTaskRun.Status.Runnable() {
			logger.Debugw("Task execution blocked", []interface{}{"run", run.ID, "task", currentTaskRun.ID, "state", currentTaskRun.Result.Status}...)
		} else if currentTaskRun.Status.Unstarted() {
			return fmt.Errorf("run %s task %s cannot return a status of empty string or Unstarted", run.ID, currentTaskRun.TaskSpec.Type)
		} else if futureTaskRun := run.NextTaskRun(); futureTaskRun != nil {
			validateMinimumConfirmations(&run, futureTaskRun, run.ObservedHeight, je.store.TxManager)
		}

		if err := je.store.ORM.SaveJobRun(&run); err != nil {
			return err
		}

		if run.Status.Finished() {
			logger.Debugw("All tasks complete for run", run.ForLogger()...)
		}
	}

	return nil
}

func (je *jobExecutor) executeTask(run *models.JobRun, currentTaskRun *models.TaskRun) models.RunResult {
	taskCopy := currentTaskRun.TaskSpec // deliberately copied to keep mutations local

	var err error
	if taskCopy.Params, err = taskCopy.Params.Merge(run.Overrides.Data); err != nil {
		currentTaskRun.Result.SetError(err)
		return currentTaskRun.Result
	}

	adapter, err := adapters.For(taskCopy, je.store.Config, je.store.ORM)
	if err != nil {
		currentTaskRun.Result.SetError(err)
		return currentTaskRun.Result
	}

	start := time.Now()
	data, err := prepareTaskInput(run, currentTaskRun.Result.Data)
	if err != nil {
		currentTaskRun.Result.SetError(err)
		return currentTaskRun.Result
	}

	currentTaskRun.Result.CachedJobRunID = run.ID
	currentTaskRun.Result.Data = data

	result := adapter.Perform(currentTaskRun.Result, je.store)

	logger.Debugw(fmt.Sprintf("Executed task %s", taskCopy.Type), []interface{}{
		"task", currentTaskRun.ID,
		"result", result.Status,
		"result_data", result.Data,
		"elapsed", time.Since(start).Seconds(),
	}...)

	return result
}
