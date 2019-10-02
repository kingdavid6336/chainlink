package services_test

import (
	"math/big"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/onsi/gomega"
	"github.com/smartcontractkit/chainlink/core/internal/cltest"
	"github.com/smartcontractkit/chainlink/core/internal/mocks"
	"github.com/smartcontractkit/chainlink/core/services"
	strpkg "github.com/smartcontractkit/chainlink/core/store"
	"github.com/smartcontractkit/chainlink/core/store/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestServices_NewInitiatorSubscription_BackfillLogs(t *testing.T) {
	t.Parallel()

	store, cleanup := cltest.NewStore(t)
	defer cleanup()
	eth := cltest.MockEthOnStore(t, store)

	job := cltest.NewJobWithLogInitiator()
	initr := job.Initiators[0]
	log := cltest.LogFromFixture(t, "testdata/subscription_logs.json")
	eth.Register("eth_getLogs", []models.Log{log})
	eth.RegisterSubscription("logs")

	var count int32
	callback := func(*strpkg.Store, services.JobManager, models.LogRequest) { atomic.AddInt32(&count, 1) }
	fromBlock := cltest.Head(0)
	jm := new(mocks.JobManager)
	sub, err := services.NewInitiatorSubscription(initr, job, store, jm, fromBlock, callback)
	assert.NoError(t, err)
	defer sub.Unsubscribe()

	eth.EventuallyAllCalled(t)

	gomega.NewGomegaWithT(t).Eventually(func() int32 {
		return atomic.LoadInt32(&count)
	}).Should(gomega.Equal(int32(1)))
}

func TestServices_NewInitiatorSubscription_BackfillLogs_WithNoHead(t *testing.T) {
	t.Parallel()

	store, cleanup := cltest.NewStore(t)
	defer cleanup()
	eth := cltest.MockEthOnStore(t, store)

	job := cltest.NewJobWithLogInitiator()
	initr := job.Initiators[0]
	eth.RegisterSubscription("logs")

	var count int32
	callback := func(*strpkg.Store, services.JobManager, models.LogRequest) { atomic.AddInt32(&count, 1) }
	jm := new(mocks.JobManager)
	sub, err := services.NewInitiatorSubscription(initr, job, store, jm, nil, callback)
	assert.NoError(t, err)
	defer sub.Unsubscribe()

	eth.EventuallyAllCalled(t)
	assert.Equal(t, int32(0), atomic.LoadInt32(&count))
}

func TestServices_NewInitiatorSubscription_PreventsDoubleDispatch(t *testing.T) {
	t.Parallel()

	store, cleanup := cltest.NewStore(t)
	defer cleanup()
	eth := cltest.MockEthOnStore(t, store)

	job := cltest.NewJobWithLogInitiator()
	initr := job.Initiators[0]

	log := cltest.LogFromFixture(t, "testdata/subscription_logs.json")
	eth.Register("eth_getLogs", []models.Log{log}) // backfill
	logsChan := make(chan models.Log)
	eth.RegisterSubscription("logs", logsChan)

	var count int32
	callback := func(*strpkg.Store, services.JobManager, models.LogRequest) { atomic.AddInt32(&count, 1) }
	head := cltest.Head(0)
	jm := new(mocks.JobManager)
	sub, err := services.NewInitiatorSubscription(initr, job, store, jm, head, callback)
	assert.NoError(t, err)
	defer sub.Unsubscribe()

	// Add the same original log
	logsChan <- log
	// Add a log after the repeated log to make sure it gets processed
	log2 := cltest.LogFromFixture(t, "testdata/requestLog0original.json")
	logsChan <- log2

	eth.EventuallyAllCalled(t)
	g := gomega.NewGomegaWithT(t)
	g.Eventually(func() int32 { return atomic.LoadInt32(&count) }).Should(gomega.Equal(int32(2)))
}

func TestServices_ReceiveLogRequest_IgnoredLogWithRemovedFlag(t *testing.T) {
	t.Parallel()

	store, cleanup := cltest.NewStore(t)
	defer cleanup()

	jobSpec := cltest.NewJobWithLogInitiator()
	require.NoError(t, store.CreateJob(&jobSpec))

	log := models.InitiatorLogEvent{
		JobSpecID: *jobSpec.ID,
		Log: models.Log{
			Removed: true,
		},
	}

	originalCount := 0
	err := store.ORM.DB.Model(&models.JobRun{}).Count(&originalCount).Error
	require.NoError(t, err)

	jm := new(mocks.JobManager)
	services.ReceiveLogRequest(store, jm, log)
	jm.AssertExpectations(t)
}

func TestServices_StartJobSubscription(t *testing.T) {
	t.Parallel()

	sharedAddr := cltest.NewAddress()
	noAddr := common.Address{}

	tests := []struct {
		name      string
		initType  string
		initrAddr common.Address
		logAddr   common.Address
		topic0    common.Hash
		data      hexutil.Bytes
	}{
		{
			"ethlog matching address",
			"ethlog",
			sharedAddr,
			sharedAddr,
			common.Hash{},
			hexutil.Bytes{},
		},
		{
			"ethlog all address",
			"ethlog",
			noAddr,
			cltest.NewAddress(),
			common.Hash{},
			hexutil.Bytes{},
		},
		{
			"runlog v0 matching address",
			"runlog",
			sharedAddr,
			sharedAddr,
			models.RunLogTopic0original,
			cltest.StringToVersionedLogData0(t,
				"id",
				`{"value":"100"}`,
			),
		},
		{
			"runlog v20190123 w/o address",
			"runlog",
			noAddr,
			cltest.NewAddress(),
			models.RunLogTopic20190123withFullfillmentParams,
			cltest.StringToVersionedLogData20190123withFulfillmentParams(t, "id", `{"value":"100"}`),
		},
		{
			"runlog v20190123 matching address",
			"runlog",
			sharedAddr,
			sharedAddr,
			models.RunLogTopic20190123withFullfillmentParams,
			cltest.StringToVersionedLogData20190123withFulfillmentParams(t, "id", `{"value":"100"}`),
		},
		{
			"runlog v20190207 w/o address",
			"runlog",
			noAddr,
			cltest.NewAddress(),
			models.RunLogTopic20190207withoutIndexes,
			cltest.StringToVersionedLogData20190207withoutIndexes(t, "id", cltest.NewAddress(), `{"value":"100"}`),
		},
		{
			"runlog v20190207 matching address",
			"runlog",
			sharedAddr,
			sharedAddr,
			models.RunLogTopic20190207withoutIndexes,
			cltest.StringToVersionedLogData20190207withoutIndexes(t, "id", cltest.NewAddress(), `{"value":"100"}`),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, cleanup := cltest.NewStore(t)
			defer cleanup()

			eth := cltest.MockEthOnStore(t, store)
			eth.Register("eth_getLogs", []models.Log{})
			logChan := make(chan models.Log, 1)
			eth.RegisterSubscription("logs", logChan)

			job := cltest.NewJob()
			initr := models.Initiator{Type: test.initType}
			initr.Address = test.initrAddr
			job.Initiators = []models.Initiator{initr}
			require.NoError(t, store.CreateJob(&job))

			executeJobChannel := make(chan struct{})

			jobManager := new(mocks.JobManager)
			jobManager.On("ExecuteJobWithRunRequest", job.ID, mock.Anything, mock.Anything, big.NewInt(0), mock.Anything).
				Return(nil, nil).
				Run(func(mock.Arguments) {
					executeJobChannel <- struct{}{}
				})

			subscription, err := services.StartJobSubscription(job, cltest.Head(91), store, jobManager)
			require.NoError(t, err)
			assert.NotNil(t, subscription)

			logChan <- models.Log{
				Address: test.logAddr,
				Data:    test.data,
				Topics: []common.Hash{
					test.topic0,
					models.IDToTopic(job.ID),
					cltest.NewAddress().Hash(),
					common.BigToHash(big.NewInt(0)),
				},
			}

			cltest.CallbackOrTimeout(t, "ExecuteJobWithRunRequest", func() {
				<-executeJobChannel
			})

			jobManager.AssertExpectations(t)
			eth.EventuallyAllCalled(t)
		})
	}
}

func TestServices_StartJobSubscription_RunlogNoTopicMatch(t *testing.T) {
	t.Parallel()

	sharedAddr := cltest.NewAddress()

	tests := []struct {
		name string
		data hexutil.Bytes
	}{
		{
			"runlog w non-matching topic",
			cltest.StringToVersionedLogData20190123withFulfillmentParams(t, "id", `{"value":"100"}`)},
		{
			"runlog w non-matching topic",
			cltest.StringToVersionedLogData20190207withoutIndexes(t, "id", cltest.NewAddress(), `{"value":"100"}`),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, cleanup := cltest.NewStore(t)
			defer cleanup()

			eth := cltest.MockEthOnStore(t, store)
			eth.Register("eth_getLogs", []models.Log{})
			logChan := make(chan models.Log, 1)
			eth.RegisterSubscription("logs", logChan)

			job := cltest.NewJob()
			initr := models.Initiator{Type: "runlog"}
			initr.Address = sharedAddr
			job.Initiators = []models.Initiator{initr}
			require.NoError(t, store.CreateJob(&job))

			jobManager := new(mocks.JobManager)

			subscription, err := services.StartJobSubscription(job, cltest.Head(91), store, jobManager)
			require.NoError(t, err)
			assert.NotNil(t, subscription)

			logChan <- models.Log{
				Address: sharedAddr,
				Data:    test.data,
				Topics: []common.Hash{
					common.Hash{},
					models.IDToTopic(job.ID),
					cltest.NewAddress().Hash(),
					common.BigToHash(big.NewInt(0)),
				},
			}

			jobManager.AssertExpectations(t)
			eth.EventuallyAllCalled(t)
		})
	}
}
