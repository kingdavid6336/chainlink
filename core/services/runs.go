package services

import (
	"fmt"
	"math/big"

	"github.com/smartcontractkit/chainlink/core/adapters"
	"github.com/smartcontractkit/chainlink/core/logger"
	clnull "github.com/smartcontractkit/chainlink/core/null"
	"github.com/smartcontractkit/chainlink/core/store"
	"github.com/smartcontractkit/chainlink/core/store/assets"
	"github.com/smartcontractkit/chainlink/core/store/models"
	"github.com/smartcontractkit/chainlink/core/store/orm"
	"github.com/smartcontractkit/chainlink/core/utils"
)

// MeetsMinimumPayment is a helper that returns true if jobrun received
// sufficient payment (more than jobspec's MinimumPayment) to be considered successful
func MeetsMinimumPayment(
	expectedMinJobPayment *assets.Link,
	actualRunPayment *assets.Link) bool {
	// input.Payment is always present for runs triggered by ethlogs
	if actualRunPayment == nil || expectedMinJobPayment == nil || expectedMinJobPayment.IsZero() {
		return true
	}
	return expectedMinJobPayment.Cmp(actualRunPayment) < 1
}

func prepareAdapter(
	taskRun *models.TaskRun,
	data models.JSON,
	config orm.ConfigReader,
	orm *orm.ORM) (*adapters.PipelineAdapter, error) {
	taskCopy := taskRun.TaskSpec // deliberately copied to keep mutations local

	merged, err := taskCopy.Params.Merge(data)
	if err != nil {
		return nil, err
	}
	taskCopy.Params = merged

	return adapters.For(taskCopy, config, orm)
}

func validateMinimumConfirmations(run *models.JobRun, taskRun *models.TaskRun, currentHeight *models.Big, txManager store.TxManager) {
	updateTaskRunConfirmations(currentHeight, run, taskRun)
	if !meetsMinimumConfirmations(run, taskRun, run.ObservedHeight) {
		logger.Debugw("Run cannot continue because it lacks sufficient confirmations", []interface{}{"run", run.ID.String(), "required_height", taskRun.MinimumConfirmations}...)
		run.Status = models.RunStatusPendingConfirmations
	} else if err := validateOnMainChain(run, taskRun, txManager); err != nil {
		run.SetError(err)
	} else {
		logger.Debugw("Adding next task to job run queue", []interface{}{"run", run.ID.String(), "nextTask", taskRun.TaskSpec.Type}...)
		run.Status = models.RunStatusInProgress
	}
}

func validateOnMainChain(run *models.JobRun, taskRun *models.TaskRun, txManager store.TxManager) error {
	txhash := run.RunRequest.TxHash
	if txhash == nil || !taskRun.MinimumConfirmations.Valid || taskRun.MinimumConfirmations.Uint32 == 0 {
		return nil
	}

	receipt, err := txManager.GetTxReceipt(*txhash)
	if err != nil {
		return err
	}
	if invalidRequest(run.RunRequest, receipt) {
		return fmt.Errorf(
			"TxHash %s initiating run %s not on main chain; presumably has been uncled",
			txhash.Hex(),
			run.ID.String(),
		)
	}
	return nil
}

func updateTaskRunConfirmations(currentHeight *models.Big, jr *models.JobRun, taskRun *models.TaskRun) {
	if !taskRun.MinimumConfirmations.Valid || jr.CreationHeight == nil || currentHeight == nil {
		return
	}

	confs := blockConfirmations(currentHeight, jr.CreationHeight)
	diff := utils.MinBigs(confs, big.NewInt(int64(taskRun.MinimumConfirmations.Uint32)))

	// diff's ceiling is guaranteed to be MaxUint32 since MinimumConfirmations
	// ceiling is MaxUint32.
	taskRun.Confirmations = clnull.Uint32From(uint32(diff.Int64()))
}

func invalidRequest(request models.RunRequest, receipt *models.TxReceipt) bool {
	return receipt.Unconfirmed() ||
		(request.BlockHash != nil && *request.BlockHash != *receipt.BlockHash)
}

func meetsMinimumConfirmations(
	run *models.JobRun,
	taskRun *models.TaskRun,
	currentHeight *models.Big) bool {
	if !taskRun.MinimumConfirmations.Valid || run.CreationHeight == nil || currentHeight == nil {
		return true
	}

	diff := blockConfirmations(currentHeight, run.CreationHeight)
	return diff.Cmp(big.NewInt(int64(taskRun.MinimumConfirmations.Uint32))) >= 0
}

func blockConfirmations(currentHeight, creationHeight *models.Big) *big.Int {
	bigDiff := new(big.Int).Sub(currentHeight.ToInt(), creationHeight.ToInt())
	confs := bigDiff.Add(bigDiff, big.NewInt(1)) // creation of runlog alone warrants 1 confirmation
	if confs.Cmp(big.NewInt(0)) < 0 {            // negative, so floor at 0
		confs.SetUint64(0)
	}
	return confs
}

func prepareTaskInput(run *models.JobRun, input models.JSON) (models.JSON, error) {
	previousTaskRun := run.PreviousTaskRun()

	var err error
	if previousTaskRun != nil {
		if input, err = previousTaskRun.Result.Data.Merge(input); err != nil {
			return models.JSON{}, err
		}
	}

	if input, err = run.Overrides.Data.Merge(input); err != nil {
		return models.JSON{}, err
	}
	return input, nil
}

func performTaskSleep(run *models.JobRun, task *models.TaskRun, adapter *adapters.Sleep, clock utils.AfterNower, jobExecutor *jobExecutor) error {
	//duration := adapter.Duration()
	//if duration <= 0 {
	//logger.Debugw("Sleep duration has already elapsed, completing task", run.ForLogger()...)
	//task.Status = models.RunStatusCompleted
	//run.Status = models.RunStatusInProgress
	//return jobRunner.Run(run)
	//}

	//// XXX: This is to eliminate data race that occurs because slices share their
	//// underlying array even in copies
	//runCopy := *run
	//runCopy.TaskRuns = make([]models.TaskRun, len(run.TaskRuns))
	//copy(runCopy.TaskRuns, run.TaskRuns)

	//go func(run models.JobRun) {
	//logger.Debugw("Task sleeping...", run.ForLogger()...)

	//<-clock.After(duration)

	//task := run.NextTaskRun()
	//task.Status = models.RunStatusCompleted
	//run.Status = models.RunStatusInProgress

	//logger.Debugw("Waking job up after sleep", run.ForLogger()...)

	//if err := jobRunner.updateAndTrigger(&run); err != nil {
	//logger.Errorw("Error resuming sleeping job:", "error", err)
	//}
	//}(runCopy)

	return nil
}
