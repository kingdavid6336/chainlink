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

//func TestJobSubscriber_reconnectLoop_Resubscribing(t *testing.T) {
//t.Parallel()

//store, cleanup := cltest.NewStore(t)
//defer cleanup()

//jobRunner := new(mocks.JobRunner)
//jobManager := new(mocks.JobManager)
//js := services.NewJobSubscriber(store, jobManager)

//eth := cltest.MockEthOnStore(t, store)
//eth.Register("eth_getLogs", []models.Log{})
//eth.Register("eth_getLogs", []models.Log{})
//eth.Register("eth_getLogs", []models.Log{})
//eth.Register("eth_getLogs", []models.Log{})

//j1 := cltest.NewJobWithLogInitiator()
//j2 := cltest.NewJobWithLogInitiator()
//assert.Nil(t, store.CreateJob(&j1))
//assert.Nil(t, store.CreateJob(&j2))

//eth.RegisterSubscription("logs")
//eth.RegisterSubscription("logs")

//el := services.NewJobSubscriber(store, jobManager)
//assert.Nil(t, el.Connect(cltest.Head(1)))
//assert.Len(t, el.Jobs(), 2)
//el.Disconnect()
//assert.Len(t, el.Jobs(), 0)

//eth.RegisterSubscription("logs")
//eth.RegisterSubscription("logs")
//assert.Nil(t, el.Connect(cltest.Head(2)))
//assert.Len(t, el.Jobs(), 2)
//el.Disconnect()
//assert.Len(t, el.Jobs(), 0)
//eth.EventuallyAllCalled(t)
//}

//func TestJobSubscriber_AddJob_Listening(t *testing.T) {
//t.Parallel()
//sharedAddr := newAddr()
//noAddr := common.Address{}

//tests := []struct {
//name      string
//initType  string
//initrAddr common.Address
//logAddr   common.Address
//wantCount int
//topic0    common.Hash
//data      hexutil.Bytes
//}{
//{"ethlog matching address", "ethlog", sharedAddr, sharedAddr, 1, common.Hash{}, hexutil.Bytes{}},
//{"ethlog all address", "ethlog", noAddr, newAddr(), 1, common.Hash{}, hexutil.Bytes{}},
//{"runlog v0 matching address", "runlog", sharedAddr, sharedAddr, 1, models.RunLogTopic0original, cltest.StringToVersionedLogData0(t, "id", `{"value":"100"}`)},
//{"runlog v20190123 w/o address", "runlog", noAddr, newAddr(), 1, models.RunLogTopic20190123withFullfillmentParams, cltest.StringToVersionedLogData20190123withFulfillmentParams(t, "id", `{"value":"100"}`)},
//{"runlog v20190123 matching address", "runlog", sharedAddr, sharedAddr, 1, models.RunLogTopic20190123withFullfillmentParams, cltest.StringToVersionedLogData20190123withFulfillmentParams(t, "id", `{"value":"100"}`)},
//{"runlog w non-matching topic", "runlog", sharedAddr, sharedAddr, 0, common.Hash{}, cltest.StringToVersionedLogData20190123withFulfillmentParams(t, "id", `{"value":"100"}`)},
//{"runlog v20190207 w/o address", "runlog", noAddr, newAddr(), 1, models.RunLogTopic20190207withoutIndexes, cltest.StringToVersionedLogData20190207withoutIndexes(t, "id", cltest.NewAddress(), `{"value":"100"}`)},
//{"runlog v20190207 matching address", "runlog", sharedAddr, sharedAddr, 1, models.RunLogTopic20190207withoutIndexes, cltest.StringToVersionedLogData20190207withoutIndexes(t, "id", cltest.NewAddress(), `{"value":"100"}`)},
//{"runlog w non-matching topic", "runlog", sharedAddr, sharedAddr, 0, common.Hash{}, cltest.StringToVersionedLogData20190207withoutIndexes(t, "id", cltest.NewAddress(), `{"value":"100"}`)},
//}

//for _, test := range tests {
//t.Run(test.name, func(t *testing.T) {
//store, el, cleanup := cltest.NewJobSubscriber(t)
//defer cleanup()

//eth := cltest.MockEthOnStore(t, store)
//eth.Register("eth_getLogs", []models.Log{})
//eth.Register("eth_chainId", store.Config.ChainID())
//logChan := make(chan models.Log, 1)
//eth.RegisterSubscription("logs", logChan)

//job := cltest.NewJob()
//initr := models.Initiator{Type: test.initType}
//initr.Address = test.initrAddr
//job.Initiators = []models.Initiator{initr}
//require.NoError(t, store.CreateJob(&job))
//el.AddJob(job, cltest.Head(1))

//ht := services.NewHeadTracker(store, []strpkg.HeadTrackable{el})
//require.NoError(t, ht.Start())

//logChan <- models.Log{
//Address: test.logAddr,
//Data:    test.data,
//Topics: []common.Hash{
//test.topic0,
//models.IDToTopic(job.ID),
//newAddr().Hash(),
//common.BigToHash(big.NewInt(0)),
//},
//}

//cltest.WaitForRuns(t, job, store, test.wantCount)

//eth.EventuallyAllCalled(t)
//})
//}
//}

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
