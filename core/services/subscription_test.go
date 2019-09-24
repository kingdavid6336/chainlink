package services_test

import (
	"sync/atomic"
	"testing"

	"github.com/onsi/gomega"
	"github.com/smartcontractkit/chainlink/core/internal/cltest"
	"github.com/smartcontractkit/chainlink/core/internal/mocks"
	"github.com/smartcontractkit/chainlink/core/services"
	strpkg "github.com/smartcontractkit/chainlink/core/store"
	"github.com/smartcontractkit/chainlink/core/store/models"
	"github.com/stretchr/testify/assert"
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

	gomega.NewGomegaWithT(t).Consistently(func() int {
		count := 0
		err := store.ORM.DB.Model(&models.JobRun{}).Count(&count).Error
		require.NoError(t, err)
		return count - originalCount
	}).Should(gomega.Equal(0))
}
