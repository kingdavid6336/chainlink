package services

import (
	"errors"
	"fmt"
	"sync"

	"github.com/smartcontractkit/chainlink/core/logger"
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
	jobExecutor          JobExecutor
}

// NewJobRunner initializes a JobRunner.
func NewJobRunner(jobExecutor JobExecutor) JobRunner {
	return &jobRunner{
		workers:     make(map[string]chan struct{}, 1),
		runChannel:  make(chan string, 1000),
		jobExecutor: jobExecutor,
	}
}

// Start prepares the job runner for accepting runs to execute.
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
			if err := jr.jobExecutor.Execute(runID); err != nil {
				logger.Errorw(fmt.Sprint("Error executing run ", runID))
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
