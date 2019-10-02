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
