package services

import (
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

	WorkerCount() int
}

type jobRunner struct {
	workersMutex sync.RWMutex
	workers      map[string]struct{}
	workersWg    sync.WaitGroup

	jobExecutor JobExecutor
}

// NewJobRunner initializes a JobRunner.
func NewJobRunner(jobExecutor JobExecutor) JobRunner {
	return &jobRunner{
		workers:     make(map[string]struct{}),
		jobExecutor: jobExecutor,
	}
}

// Start prepares the job runner for accepting runs to execute.
func (jr *jobRunner) Start() error {
	return nil
}

// Stop closes all open worker channels.
func (jr *jobRunner) Stop() {
	jr.workersWg.Wait()
}

// Run tells the job runner to start executing a job
func (jr *jobRunner) Run(run *models.JobRun) error {
	runID := run.ID.String()

	jr.workersMutex.Lock()
	_, present := jr.workers[runID]
	if present {
		jr.workersMutex.Unlock()
		return fmt.Errorf("Run %s is already working", runID)
	}
	jr.workers[runID] = struct{}{}
	jr.workersMutex.Unlock()

	jr.workersWg.Add(1)
	go func() {
		if err := jr.jobExecutor.Execute(run.ID); err != nil {
			logger.Errorw(fmt.Sprint("Error executing run ", runID))
		}

		jr.workersMutex.Lock()
		delete(jr.workers, runID)
		jr.workersMutex.Unlock()

		jr.workersWg.Done()
	}()

	return nil
}

// WorkerCount returns the number of workers currently processing a job run
func (jr *jobRunner) WorkerCount() int {
	jr.workersMutex.RLock()
	defer jr.workersMutex.RUnlock()

	return len(jr.workers)
}
