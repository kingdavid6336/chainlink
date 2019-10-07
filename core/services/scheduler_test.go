package services_test

import (
	"testing"
	"time"

	"github.com/smartcontractkit/chainlink/core/internal/cltest"
	"github.com/smartcontractkit/chainlink/core/internal/mocks"
	"github.com/smartcontractkit/chainlink/core/services"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestScheduler_Start_LoadingRecurringJobs(t *testing.T) {
	t.Parallel()

	store, cleanup := cltest.NewStore(t)
	defer cleanup()

	jobWCron := cltest.NewJobWithSchedule("* * * * * *")
	require.NoError(t, store.CreateJob(&jobWCron))
	jobWoCron := cltest.NewJob()
	require.NoError(t, store.CreateJob(&jobWoCron))

	executeJobChannel := make(chan struct{})
	jm := new(mocks.JobManager)
	jm.On("ExecuteJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil).
		Twice().
		Run(func(mock.Arguments) {
			executeJobChannel <- struct{}{}
		})

	sched := services.NewScheduler(store, jm)
	require.NoError(t, sched.Start())

	cltest.CallbackOrTimeout(t, "ExecuteJob", func() {
		<-executeJobChannel
		<-executeJobChannel
	}, 3*time.Second)

	sched.Stop()

	jm.AssertExpectations(t)
}

func TestRecurring_AddJob(t *testing.T) {
	executeJobChannel := make(chan struct{}, 1)
	jm := new(mocks.JobManager)
	jm.On("ExecuteJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil).
		Run(func(mock.Arguments) {
			executeJobChannel <- struct{}{}
		}).
		Twice()

	r := services.NewRecurring(jm)
	cron := cltest.NewMockCron()
	r.Cron = cron

	job := cltest.NewJobWithSchedule("* * * * *")
	r.AddJob(job)

	cron.RunEntries()

	cltest.CallbackOrTimeout(t, "ExecuteJob", func() {
		<-executeJobChannel
	}, 3*time.Second)

	cron.RunEntries()

	cltest.CallbackOrTimeout(t, "ExecuteJob", func() {
		<-executeJobChannel
	}, 3*time.Second)

	r.Stop()

	jm.AssertExpectations(t)
}

func TestOneTime_AddJob(t *testing.T) {
	store, cleanup := cltest.NewStore(t)
	defer cleanup()

	executeJobChannel := make(chan struct{})
	jm := new(mocks.JobManager)
	jm.On("ExecuteJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil).
		Once().
		Run(func(mock.Arguments) {
			executeJobChannel <- struct{}{}
		})

	clock := cltest.NewTriggerClock(t)

	ot := services.OneTime{
		Clock:      clock,
		Store:      store,
		JobManager: jm,
	}
	require.NoError(t, ot.Start())

	j := cltest.NewJobWithRunAtInitiator(time.Now())
	require.Nil(t, store.CreateJob(&j))

	ot.AddJob(j)

	clock.Trigger()

	cltest.CallbackOrTimeout(t, "ws client restarts", func() {
		<-executeJobChannel
	}, 3*time.Second)

	// This should block because if OneTime works it won't listen on the channel again
	go clock.Trigger()

	// Sleep for some time to make sure another call isn't made
	time.Sleep(1 * time.Second)

	ot.Stop()

	jm.AssertExpectations(t)
}
