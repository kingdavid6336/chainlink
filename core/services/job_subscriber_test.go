package services_test

import (
	"math/big"
	"testing"

	"github.com/smartcontractkit/chainlink/core/internal/cltest"
	"github.com/smartcontractkit/chainlink/core/internal/mocks"
	"github.com/smartcontractkit/chainlink/core/services"
	"github.com/smartcontractkit/chainlink/core/store/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobSubscriber_OnNewHead(t *testing.T) {
	t.Parallel()

	store, cleanup := cltest.NewStore(t)
	defer cleanup()

	jobManager := new(mocks.JobManager)
	jobSubscriber := services.NewJobSubscriber(store, jobManager)

	jobManager.On("ResumeConfirmingTasks", big.NewInt(1337)).Return(nil)

	jobSubscriber.OnNewHead(cltest.Head(1337))

	jobManager.AssertExpectations(t)
}

func TestJobSubscriber_AddJob_RemoveJob(t *testing.T) {
	t.Parallel()

	store, cleanup := cltest.NewStore(t)
	defer cleanup()
	cltest.MockEthOnStore(t, store)

	jobManager := new(mocks.JobManager)
	jobSubscriber := services.NewJobSubscriber(store, jobManager)

	jobSpec := cltest.NewJobWithLogInitiator()
	err := jobSubscriber.AddJob(jobSpec, cltest.Head(321))
	require.NoError(t, err)

	assert.Len(t, jobSubscriber.Jobs(), 1)

	err = jobSubscriber.RemoveJob(jobSpec.ID)
	require.NoError(t, err)

	assert.Len(t, jobSubscriber.Jobs(), 0)

	jobManager.AssertExpectations(t)

}

func TestJobSubscriber_AddJob_NotLogInitiatedError(t *testing.T) {
	t.Parallel()

	store, cleanup := cltest.NewStore(t)
	defer cleanup()

	jobManager := new(mocks.JobManager)
	jobSubscriber := services.NewJobSubscriber(store, jobManager)

	job := models.JobSpec{}
	err := jobSubscriber.AddJob(job, cltest.Head(1))
	require.NoError(t, err)
}

func TestJobSubscriber_RemoveJob_NotFoundError(t *testing.T) {
	t.Parallel()

	store, cleanup := cltest.NewStore(t)
	defer cleanup()

	jobManager := new(mocks.JobManager)
	jobSubscriber := services.NewJobSubscriber(store, jobManager)

	err := jobSubscriber.RemoveJob(models.NewID())
	require.Error(t, err)
}

func TestJobSubscriber_Connect_Disconnect(t *testing.T) {
	t.Parallel()

	store, cleanup := cltest.NewStore(t)
	defer cleanup()

	jobManager := new(mocks.JobManager)
	jobSubscriber := services.NewJobSubscriber(store, jobManager)

	eth := cltest.MockEthOnStore(t, store)
	eth.Register("eth_getLogs", []models.Log{})
	eth.Register("eth_getLogs", []models.Log{})

	jobSpec1 := cltest.NewJobWithLogInitiator()
	jobSpec2 := cltest.NewJobWithLogInitiator()
	assert.Nil(t, store.CreateJob(&jobSpec1))
	assert.Nil(t, store.CreateJob(&jobSpec2))
	eth.RegisterSubscription("logs")
	eth.RegisterSubscription("logs")

	assert.Nil(t, jobSubscriber.Connect(cltest.Head(491)))
	eth.EventuallyAllCalled(t)

	assert.Len(t, jobSubscriber.Jobs(), 2)

	jobSubscriber.Disconnect()

	assert.Len(t, jobSubscriber.Jobs(), 0)
}

// FIXME: fix these

//func TestJobSubscriber_RemoveJob_RunLog(t *testing.T) {
//t.Parallel()

//store, el, cleanup := cltest.NewJobSubscriber(t)
//defer cleanup()

//eth := cltest.MockEthOnStore(t, store)
//eth.Register("eth_getLogs", []models.Log{})
//eth.Register("eth_chainId", store.Config.ChainID())
//logChan := make(chan models.Log, 1)
//eth.RegisterSubscription("logs", logChan)

//addr := newAddr()
//job := cltest.NewJob()
//initr := models.Initiator{Type: "runlog"}
//initr.Address = addr
//job.Initiators = []models.Initiator{initr}
//require.NoError(t, store.CreateJob(&job))
//el.AddJob(job, cltest.Head(1))
//require.Len(t, el.Jobs(), 1)

//require.NoError(t, el.RemoveJob(job.ID))
//require.Len(t, el.Jobs(), 0)

//ht := services.NewHeadTracker(store, []strpkg.HeadTrackable{el})
//require.NoError(t, ht.Start())

//// asserts that JobSubscriber unsubscribed the job specific channel
//require.True(t, sendingOnClosedChannel(func() {
//logChan <- models.Log{}
//}))

//cltest.WaitForRuns(t, job, store, 0)
//eth.EventuallyAllCalled(t)
//}

//func TestJobSubscriber_RemoveJob_EthLog(t *testing.T) {
//t.Parallel()

//store, el, cleanup := cltest.NewJobSubscriber(t)
//defer cleanup()

//eth := cltest.MockEthOnStore(t, store)
//eth.Register("eth_getLogs", []models.Log{})
//eth.Register("eth_chainId", store.Config.ChainID())
//logChan := make(chan models.Log, 1)
//eth.RegisterSubscription("logs", logChan)

//addr := newAddr()
//job := cltest.NewJob()
//initr := models.Initiator{Type: "ethlog"}
//initr.Address = addr
//job.Initiators = []models.Initiator{initr}
//require.NoError(t, store.CreateJob(&job))
//el.AddJob(job, cltest.Head(1))
//require.Len(t, el.Jobs(), 1)

//require.NoError(t, el.RemoveJob(job.ID))
//require.Len(t, el.Jobs(), 0)

//ht := services.NewHeadTracker(store, []strpkg.HeadTrackable{el})
//require.NoError(t, ht.Start())

//// asserts that JobSubscriber unsubscribed the job specific channel
//require.True(t, sendingOnClosedChannel(func() {
//logChan <- models.Log{}
//}))

//cltest.WaitForRuns(t, job, store, 0)
//eth.EventuallyAllCalled(t)
//}

//func sendingOnClosedChannel(callback func()) (rval bool) {
//defer func() {
//if r := recover(); r != nil {
//rerror := r.(error)
//rval = rerror.Error() == "send on closed channel"
//}
//}()
//callback()
//return false
//}

////func TestJobSubscriber_OnNewHead_ResumePendingConfirmationsAndPendingConnections(t *testing.T) {
////t.Parallel()

////block := cltest.NewBlockHeader(10)
////prettyLabel := func(archived bool, rs models.RunStatus) string {
////if archived {
////return fmt.Sprintf("archived:%s", string(rs))
////}
////return string(rs)
////}

////tests := []struct {
////status   models.RunStatus
////archived bool
////wantSend bool
////}{
////{models.RunStatusPendingConnection, false, true},
////{models.RunStatusPendingConnection, true, true},
////{models.RunStatusPendingConfirmations, false, true},
////{models.RunStatusPendingConfirmations, true, true},
////{models.RunStatusInProgress, false, false},
////{models.RunStatusInProgress, true, false},
////{models.RunStatusPendingBridge, false, false},
////{models.RunStatusPendingBridge, true, false},
////{models.RunStatusPendingSleep, false, false},
////{models.RunStatusPendingSleep, true, false},
////{models.RunStatusCompleted, false, false},
////{models.RunStatusCompleted, true, false},
////}

////for _, test := range tests {
////t.Run(prettyLabel(test.archived, test.status), func(t *testing.T) {
////store, js, cleanup := cltest.NewJobSubscriber(t)
////defer cleanup()

////job := cltest.NewJobWithWebInitiator()
////require.NoError(t, store.CreateJob(&job))
////initr := job.Initiators[0]
////run := job.NewRun(initr)
////run.ApplyResult(models.RunResult{Status: test.status})
////require.NoError(t, store.CreateJobRun(&run))

////if test.archived {
////require.NoError(t, store.ArchiveJob(job.ID))
////}

////js.OnNewHead(block.ToHead())

////if test.wantSend {
////assert.Equal(t, 1, len(mockRunChannel.Runs))
////} else {
////assert.Equal(t, 0, len(mockRunChannel.Runs))
////}
////})
////}
////}
