package services_test

import (
	"testing"
	"time"

	"github.com/onsi/gomega"
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

	jm := new(mocks.JobManager)
	jm.On("ExecuteJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Twice()

	sched := services.NewScheduler(store, jm)
	require.NoError(t, sched.Start())

	gomega.NewGomegaWithT(t).Eventually(func() int {
		return len(jm.Calls)
	}).Should(gomega.Equal(2))

	sched.Stop()

	jm.AssertExpectations(t)
}

func TestRecurring_AddJob(t *testing.T) {
	jm := new(mocks.JobManager)
	jm.On("ExecuteJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Twice()

	r := services.NewRecurring(jm)
	cron := cltest.NewMockCron()
	r.Cron = cron

	job := cltest.NewJobWithSchedule("* * * * *")
	r.AddJob(job)

	cron.RunEntries()

	gomega.NewGomegaWithT(t).Eventually(func() int {
		return len(jm.Calls)
	}).Should(gomega.Equal(1))

	cron.RunEntries()

	gomega.NewGomegaWithT(t).Eventually(func() int {
		return len(jm.Calls)
	}).Should(gomega.Equal(2))

	r.Stop()

	jm.AssertExpectations(t)
}

func TestOneTime_AddJob(t *testing.T) {
	store, cleanup := cltest.NewStore(t)
	defer cleanup()

	jm := new(mocks.JobManager)
	jm.On("ExecuteJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()

	clock := cltest.NewTriggerClock()

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

	gomega.NewGomegaWithT(t).Eventually(func() int {
		return len(jm.Calls)
	}).Should(gomega.Equal(1))

	// This should block because if OneTime works it won't listen on the channel again
	go clock.Trigger()

	gomega.NewGomegaWithT(t).Consistently(func() int {
		return len(jm.Calls)
	}).Should(gomega.Equal(1))

	ot.Stop()

	jm.AssertExpectations(t)
}
