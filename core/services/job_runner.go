package services

import (
	"errors"
	"fmt"
	"sync"

	"github.com/smartcontractkit/chainlink/core/adapters"
	"github.com/smartcontractkit/chainlink/core/logger"
	"github.com/smartcontractkit/chainlink/core/store"
	"github.com/smartcontractkit/chainlink/core/store/models"
)

//go:generate mockery -name JobRunner -output ../internal/mocks/ -case=underscore

// JobRunner safely handles coordinating job runs.
type JobRunner interface {
	Start() error
	Stop()
	Run(*models.JobRun) error

	workerCount() int
}

type jobRunner struct {
	started              bool
	done                 chan struct{}
	bootMutex            sync.Mutex
	workerMutex          sync.RWMutex
	workers              map[string]chan struct{}
	workersWg            sync.WaitGroup
	demultiplexStopperWg sync.WaitGroup
	runChannel           chan string

	store *store.Store
}

// NewJobRunner initializes a JobRunner.
func NewJobRunner(str *store.Store) JobRunner {
	return &jobRunner{
		// Unscoped allows the processing of runs that are soft deleted asynchronously
		store:      str.Unscoped(),
		workers:    make(map[string]chan struct{}, 1),
		runChannel: make(chan string, 1000),
	}
}

// Start reinitializes runs and starts the execution of the store's runs.
func (jr *jobRunner) Start() error {
	jr.bootMutex.Lock()
	defer jr.bootMutex.Unlock()

	if jr.started {
		return errors.New("JobRunner already started")
	}
	jr.done = make(chan struct{})
	jr.started = true

	var starterWg sync.WaitGroup
	starterWg.Add(1)
	go jr.demultiplexRuns(&starterWg)
	starterWg.Wait()

	jr.demultiplexStopperWg.Add(1)
	return nil
}

// Stop closes all open worker channels.
func (jr *jobRunner) Stop() {
	jr.bootMutex.Lock()
	defer jr.bootMutex.Unlock()

	if !jr.started {
		return
	}
	close(jr.done)
	jr.started = false
	jr.demultiplexStopperWg.Wait()
}

// Run tells the job runner to start executing a job
func (jr *jobRunner) Run(run *models.JobRun) error {
	if run.Status.PendingSleep() {
		return jr.queueSleepingTask(run)
	}

	jr.runChannel <- run.ID.String()
	return nil
}

func (jr *jobRunner) demultiplexRuns(starterWg *sync.WaitGroup) {
	starterWg.Done()
	defer jr.demultiplexStopperWg.Done()
	for {
		select {
		case <-jr.done:
			logger.Debug("JobRunner demultiplexing of job runs finished")
			jr.workersWg.Wait()
			return
		case runID, ok := <-jr.runChannel:
			if !ok {
				logger.Panic("RunChannel closed before JobRunner, can no longer demultiplexing job runs")
				return
			}
			jr.channelForRun(runID) <- struct{}{}
		}
	}
}

func (jr *jobRunner) channelForRun(runID string) chan<- struct{} {
	jr.workerMutex.Lock()
	defer jr.workerMutex.Unlock()

	workerChannel, present := jr.workers[runID]
	if !present {
		workerChannel = make(chan struct{}, 1)
		jr.workers[runID] = workerChannel
		jr.workersWg.Add(1)

		go func() {
			id, _ := models.NewIDFromString(runID)
			jr.workerLoop(id, workerChannel)

			jr.workerMutex.Lock()
			delete(jr.workers, runID)
			jr.workersWg.Done()
			jr.workerMutex.Unlock()

			logger.Debug("Worker finished for ", runID)
		}()
	}
	return workerChannel
}

func (jr *jobRunner) workerLoop(runID *models.ID, workerChannel chan struct{}) {
	for {
		select {
		case <-workerChannel:
			run, err := jr.store.FindJobRun(runID)
			if err != nil {
				logger.Errorw(fmt.Sprint("Error finding run ", runID), run.ForLogger("error", err)...)
			}

			if err := jr.executeRun(&run); err != nil {
				logger.Errorw(fmt.Sprint("Error executing run ", runID), run.ForLogger("error", err)...)
				return
			}

			if run.Status.Finished() {
				logger.Debugw("All tasks complete for run", "run", run.ID)
				return
			}

		case <-jr.done:
			logger.Debug("JobRunner worker loop for ", runID, " finished")
			return
		}
	}
}

func (jr *jobRunner) workerCount() int {
	jr.workerMutex.RLock()
	defer jr.workerMutex.RUnlock()

	return len(jr.workers)
}

func (jr *jobRunner) executeTask(run *models.JobRun, currentTaskRun *models.TaskRun) models.RunResult {
	taskCopy := currentTaskRun.TaskSpec // deliberately copied to keep mutations local

	var err error
	if taskCopy.Params, err = taskCopy.Params.Merge(run.Overrides.Data); err != nil {
		currentTaskRun.Result.SetError(err)
		return currentTaskRun.Result
	}

	adapter, err := adapters.For(taskCopy, jr.store.Config, jr.store.ORM)
	if err != nil {
		currentTaskRun.Result.SetError(err)
		return currentTaskRun.Result
	}

	logger.Infow(fmt.Sprintf("Processing task %s", taskCopy.Type), []interface{}{"task", currentTaskRun.ID}...)

	data, err := prepareTaskInput(run, currentTaskRun.Result.Data)
	if err != nil {
		currentTaskRun.Result.SetError(err)
		return currentTaskRun.Result
	}

	currentTaskRun.Result.CachedJobRunID = run.ID
	currentTaskRun.Result.Data = data
	result := adapter.Perform(currentTaskRun.Result, jr.store)
	result.ID = currentTaskRun.Result.ID

	logger.Infow(fmt.Sprintf("Finished processing task %s", taskCopy.Type), []interface{}{
		"task", currentTaskRun.ID,
		"result", result.Status,
		"result_data", result.Data,
	}...)

	return result
}

func (jr *jobRunner) executeRun(run *models.JobRun) error {
	logger.Infow("Processing run", run.ForLogger()...)

	if !run.Status.Runnable() {
		return fmt.Errorf("Run triggered in non runnable state %s", run.Status)
	}

	currentTaskRun := run.NextTaskRun()
	if currentTaskRun == nil {
		return errors.New("Run triggered with no remaining tasks")
	}

	result := jr.executeTask(run, currentTaskRun)

	currentTaskRun.ApplyResult(result)
	run.ApplyResult(result)

	if currentTaskRun.Status.PendingSleep() {
		logger.Debugw("Task is sleeping", []interface{}{"run", run.ID}...)
	} else if !currentTaskRun.Status.Runnable() {
		logger.Debugw("Task execution blocked", []interface{}{"run", run.ID, "task", currentTaskRun.ID, "state", currentTaskRun.Result.Status}...)
	} else if currentTaskRun.Status.Unstarted() {
		return fmt.Errorf("run %s task %s cannot return a status of empty string or Unstarted", run.ID, currentTaskRun.TaskSpec.Type)
	} else if futureTaskRun := run.NextTaskRun(); futureTaskRun != nil {
		validateMinimumConfirmations(run, futureTaskRun, run.ObservedHeight, jr.store.TxManager)
	}

	if err := jr.updateAndTrigger(run); err != nil {
		return err
	}
	logger.Infow("Run finished processing", run.ForLogger()...)

	return nil
}

// QueueSleepingTask creates a go routine which will wake up the job runner
// once the sleep's time has elapsed
func (jr *jobRunner) queueSleepingTask(run *models.JobRun) error {
	if !run.Status.PendingSleep() {
		return fmt.Errorf("Attempting to resume non sleeping run %s", run.ID)
	}

	currentTaskRun := run.NextTaskRun()
	if currentTaskRun == nil {
		return fmt.Errorf("Attempting to resume sleeping run with no remaining tasks %s", run.ID)
	}

	if !currentTaskRun.Status.PendingSleep() {
		return fmt.Errorf("Attempting to resume sleeping run with non sleeping task %s", run.ID)
	}

	adapter, err := prepareAdapter(currentTaskRun, run.Overrides.Data, jr.store.Config, jr.store.ORM)
	if err != nil {
		currentTaskRun.SetError(err)
		run.SetError(err)
		return jr.store.SaveJobRun(run)
	}

	if sleepAdapter, ok := adapter.BaseAdapter.(*adapters.Sleep); ok {
		return performTaskSleep(run, currentTaskRun, sleepAdapter, jr.store.Clock, jr)
	}

	return fmt.Errorf("Attempting to resume non sleeping task for run %s (%s)", run.ID, currentTaskRun.TaskSpec.Type)
}

func (jr *jobRunner) updateAndTrigger(run *models.JobRun) error {
	if err := jr.store.ORM.SaveJobRun(run); err != nil {
		return err
	}

	if run.Status == models.RunStatusInProgress || run.Status == models.RunStatusPendingSleep {
		return jr.Run(run)
	}

	return nil
}
