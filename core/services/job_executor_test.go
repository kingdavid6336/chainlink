package services_test

import (
	"testing"

	"github.com/smartcontractkit/chainlink/core/internal/cltest"
	"github.com/smartcontractkit/chainlink/core/services"
	"github.com/smartcontractkit/chainlink/core/store/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobExecutor_Execute(t *testing.T) {
	t.Parallel()

	store, cleanup := cltest.NewStore(t)
	defer cleanup()

	je := services.NewJobExecutor(store)

	j := models.NewJob()
	i := models.Initiator{Type: models.InitiatorWeb}
	j.Initiators = []models.Initiator{i}
	j.Tasks = []models.TaskSpec{
		cltest.NewTask(t, "noop"),
		cltest.NewTask(t, "nooppend"),
	}
	assert.NoError(t, store.CreateJob(&j))

	run := j.NewRun(i)
	require.NoError(t, store.CreateJobRun(&run))

	err := je.Execute(run.ID)
	require.NoError(t, err)

	run, err = store.FindJobRun(run.ID)
	require.NoError(t, err)
	assert.Equal(t, models.RunStatusInProgress, run.Status)
	require.Len(t, run.TaskRuns, 2)
	assert.Equal(t, models.RunStatusCompleted, run.TaskRuns[0].Status)
	assert.Equal(t, models.RunStatusUnstarted, run.TaskRuns[1].Status)
}

func TestJobExecutor_Execute_RunNotFoundError(t *testing.T) {
	t.Parallel()

	store, cleanup := cltest.NewStore(t)
	defer cleanup()

	je := services.NewJobExecutor(store)

	err := je.Execute(models.NewID())
	require.Error(t, err)
}

func TestJobExecutor_Execute_RunNotRunnableError(t *testing.T) {
	t.Parallel()

	store, cleanup := cltest.NewStore(t)
	defer cleanup()

	je := services.NewJobExecutor(store)

	j := models.NewJob()
	i := models.Initiator{Type: models.InitiatorWeb}
	j.Initiators = []models.Initiator{i}
	j.Tasks = []models.TaskSpec{
		cltest.NewTask(t, "noop"),
	}
	assert.NoError(t, store.CreateJob(&j))

	run := j.NewRun(i)
	run.Status = models.RunStatusPendingConfirmations
	require.NoError(t, store.CreateJobRun(&run))

	err := je.Execute(run.ID)
	require.Error(t, err)
}

// FIXME: can be tested on RunResult
//func TestJobRunner_executeRun_correctlyPopulatesFinishedAt(t *testing.T) {
//store, cleanup := cltest.NewStore(t)
//defer cleanup()

//j := models.NewJob()
//i := models.Initiator{Type: models.InitiatorWeb}
//j.Initiators = []models.Initiator{i}
//j.Tasks = []models.TaskSpec{
//cltest.NewTask(t, "noop"),
//cltest.NewTask(t, "nooppend"),
//}
//assert.NoError(t, store.CreateJob(&j))

//run := j.NewRun(i)
//require.NoError(t, store.CreateJobRun(&run))

//require.NoError(t, services.ExportedExecuteRun(&run, store))
//assert.False(t, run.FinishedAt.Valid)
//assert.Equal(t, models.RunStatusInProgress, run.Status)

//require.NoError(t, services.ExportedExecuteRun(&run, store))
//assert.False(t, run.FinishedAt.Valid)
//assert.Equal(t, models.RunStatusPendingConfirmations, run.Status)
//}

//func TestJobRunner_executeRun_correctlyAddsLinkEarnings(t *testing.T) {
//store, cleanup := cltest.NewStore(t)
//defer cleanup()

//j := models.NewJob()
//i := models.Initiator{Type: models.InitiatorWeb}
//j.Initiators = []models.Initiator{i}
//j.Tasks = []models.TaskSpec{
//cltest.NewTask(t, "noop"),
//}
//assert.NoError(t, store.CreateJob(&j))
//run := j.NewRun(i)
//run.Payment = assets.NewLink(1)
//require.NoError(t, store.CreateJobRun(&run))
//require.NoError(t, services.ExportedExecuteRun(&run, store))

//actual, err := store.LinkEarnedFor(&j)
//require.NoError(t, err)
//assert.Equal(t, assets.NewLink(1), actual)
//}

//func TestJobRunner_ChannelForRun_equalityBetweenRuns(t *testing.T) {
//t.Parallel()

//store, cleanup := cltest.NewStore(t)
//defer cleanup()
//rm, cleanup := cltest.NewJobRunner(store)
//defer cleanup()

//job := cltest.NewJobWithWebInitiator()
//initr := job.Initiators[0]
//run1 := job.NewRun(initr)
//run2 := job.NewRun(initr)

//chan1a := services.ExportedChannelForRun(rm, run1.ID)
//chan2 := services.ExportedChannelForRun(rm, run2.ID)
//chan1b := services.ExportedChannelForRun(rm, run1.ID)

//assert.NotEqual(t, chan1a, chan2)
//assert.Equal(t, chan1a, chan1b)
//assert.NotEqual(t, chan2, chan1b)
//}

//func TestJobRunner_ChannelForRun_sendAfterClosing(t *testing.T) {
//t.Parallel()

//s, cleanup := cltest.NewStore(t)
//defer cleanup()
//rm, cleanup := cltest.NewJobRunner(s)
//defer cleanup()
//assert.NoError(t, rm.Start())

//j := cltest.NewJobWithWebInitiator()
//assert.NoError(t, s.CreateJob(&j))
//initr := j.Initiators[0]
//jr := j.NewRun(initr)
//assert.NoError(t, s.CreateJobRun(&jr))

//chan1 := services.ExportedChannelForRun(rm, jr.ID)
//chan1 <- struct{}{}
//cltest.WaitForJobRunToComplete(t, s, jr)

//gomega.NewGomegaWithT(t).Eventually(func() chan<- struct{} {
//return services.ExportedChannelForRun(rm, jr.ID)
//}).Should(gomega.Not(gomega.Equal(chan1))) // eventually deletes the channel

//chan2 := services.ExportedChannelForRun(rm, jr.ID)
//chan2 <- struct{}{} // does not panic
//}

//func TestJobRunner_ChannelForRun_equalityWithoutClosing(t *testing.T) {
//t.Parallel()

//s, cleanup := cltest.NewStore(t)
//defer cleanup()
//rm, cleanup := cltest.NewJobRunner(s)
//defer cleanup()
//assert.NoError(t, rm.Start())

//j := cltest.NewJobWithWebInitiator()
//j.Tasks = []models.TaskSpec{cltest.NewTask(t, "nooppend")}
//assert.NoError(t, s.CreateJob(&j))
//initr := j.Initiators[0]
//jr := j.NewRun(initr)
//assert.NoError(t, s.CreateJobRun(&jr))

//chan1 := services.ExportedChannelForRun(rm, jr.ID)

//chan1 <- struct{}{}
//cltest.WaitForJobRunToPendConfirmations(t, s, jr)

//chan2 := services.ExportedChannelForRun(rm, jr.ID)
//assert.Equal(t, chan1, chan2)
//}
