package services_test

import (
	"bytes"
	"fmt"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/smartcontractkit/chainlink/core/adapters"
	"github.com/smartcontractkit/chainlink/core/internal/cltest"
	clnull "github.com/smartcontractkit/chainlink/core/null"
	"github.com/smartcontractkit/chainlink/core/services"
	"github.com/smartcontractkit/chainlink/core/store/assets"
	"github.com/smartcontractkit/chainlink/core/store/models"
	"github.com/smartcontractkit/chainlink/core/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	null "gopkg.in/guregu/null.v3"
)

func TestNewRun(t *testing.T) {
	store, cleanup := cltest.NewStore(t)
	defer cleanup()

	input := models.JSON{Result: gjson.Parse(`{"address":"0xdfcfc2b9200dbb10952c2b7cce60fc7260e03c6f"}`)}

	_, bt := cltest.NewBridgeType(t, "timecube", "http://http://timecube.2enp.com/")
	bt.MinimumContractPayment = assets.NewLink(10)
	require.NoError(t, store.CreateBridgeType(bt))

	creationHeight := big.NewInt(1000)

	jobSpec := models.NewJob()
	jobSpec.Tasks = []models.TaskSpec{{
		Type: "timecube",
	}}
	jobSpec.Initiators = []models.Initiator{{
		Type: models.InitiatorEthLog,
	}}

	inputResult := models.RunResult{Data: input}
	run, err := services.NewRun(&jobSpec, &jobSpec.Initiators[0], &inputResult, creationHeight, store, nil)
	assert.NoError(t, err)
	assert.Equal(t, string(models.RunStatusInProgress), string(run.Status))
	assert.Len(t, run.TaskRuns, 1)
	assert.Equal(t, input, run.Overrides.Data)
	assert.False(t, run.TaskRuns[0].Confirmations.Valid)
}

func TestNewRun_MeetsMinimumPayment(t *testing.T) {
	tests := []struct {
		name            string
		MinJobPayment   *assets.Link
		RunPayment      *assets.Link
		meetsMinPayment bool
	}{
		{"insufficient payment", assets.NewLink(100), assets.NewLink(10), false},
		{"sufficient payment (strictly greater)", assets.NewLink(1), assets.NewLink(10), true},
		{"sufficient payment (equal)", assets.NewLink(10), assets.NewLink(10), true},
		{"runs that do not accept payments must return true", assets.NewLink(10), nil, true},
		{"return true when minpayment is not specified in jobspec", nil, assets.NewLink(0), true},
	}

	for _, tt := range tests {
		test := tt
		t.Run(test.name, func(t *testing.T) {
			actual := services.MeetsMinimumPayment(test.MinJobPayment, test.RunPayment)
			assert.Equal(t, test.meetsMinPayment, actual)
		})
	}
}

func TestNewRun_requiredPayment(t *testing.T) {
	store, cleanup := cltest.NewStore(t)
	defer cleanup()

	input := models.JSON{Result: gjson.Parse(`{"address":"0xdfcfc2b9200dbb10952c2b7cce60fc7260e03c6f"}`)}

	_, bt := cltest.NewBridgeType(t, "timecube", "http://http://timecube.2enp.com/")
	bt.MinimumContractPayment = assets.NewLink(10)
	require.NoError(t, store.CreateBridgeType(bt))

	tests := []struct {
		name                  string
		payment               *assets.Link
		minimumConfigPayment  *assets.Link
		minimumJobSpecPayment *assets.Link
		expectedStatus        models.RunStatus
	}{
		{"creates runnable job", nil, assets.NewLink(0), assets.NewLink(0), models.RunStatusInProgress},
		{"insufficient payment as specified by config", assets.NewLink(9), assets.NewLink(10), assets.NewLink(0), models.RunStatusErrored},
		{"sufficient payment as specified by config", assets.NewLink(10), assets.NewLink(10), assets.NewLink(0), models.RunStatusInProgress},
		{"insufficient payment as specified by adapter", assets.NewLink(9), assets.NewLink(0), assets.NewLink(0), models.RunStatusErrored},
		{"sufficient payment as specified by adapter", assets.NewLink(10), assets.NewLink(0), assets.NewLink(0), models.RunStatusInProgress},
		{"insufficient payment as specified by jobSpec MinPayment", assets.NewLink(9), assets.NewLink(0), assets.NewLink(10), models.RunStatusErrored},
		{"sufficient payment as specified by jobSpec MinPayment", assets.NewLink(10), assets.NewLink(0), assets.NewLink(10), models.RunStatusInProgress},
	}

	for _, tt := range tests {
		test := tt
		t.Run(test.name, func(t *testing.T) {
			store.Config.Set("MINIMUM_CONTRACT_PAYMENT", test.minimumConfigPayment)

			jobSpec := models.NewJob()
			jobSpec.Tasks = []models.TaskSpec{{
				Type: "timecube",
			}}
			jobSpec.Initiators = []models.Initiator{{
				Type: models.InitiatorEthLog,
			}}
			jobSpec.MinPayment = test.minimumJobSpecPayment

			inputResult := models.RunResult{Data: input}

			run, err := services.NewRun(&jobSpec, &jobSpec.Initiators[0], &inputResult, nil, store, test.payment)
			assert.NoError(t, err)
			assert.Equal(t, string(test.expectedStatus), string(run.Status))
		})
	}
}

func TestNewRun_minimumConfirmations(t *testing.T) {
	store, cleanup := cltest.NewStore(t)
	defer cleanup()

	input := models.JSON{Result: gjson.Parse(`{"address":"0xdfcfc2b9200dbb10952c2b7cce60fc7260e03c6f"}`)}
	inputResult := models.RunResult{Data: input}

	creationHeight := big.NewInt(1000)

	tests := []struct {
		name                string
		configConfirmations uint32
		taskConfirmations   uint32
		expectedStatus      models.RunStatus
	}{
		{"creates runnable job", 0, 0, models.RunStatusInProgress},
		{"requires minimum task confirmations", 2, 0, models.RunStatusPendingConfirmations},
		{"requires minimum config confirmations", 0, 2, models.RunStatusPendingConfirmations},
	}

	for _, tt := range tests {
		test := tt
		t.Run(test.name, func(t *testing.T) {
			store.Config.Set("MIN_INCOMING_CONFIRMATIONS", test.configConfirmations)

			jobSpec := cltest.NewJobWithLogInitiator()
			jobSpec.Tasks[0].Confirmations = clnull.Uint32From(test.taskConfirmations)

			run, err := services.NewRun(
				&jobSpec,
				&jobSpec.Initiators[0],
				&inputResult,
				creationHeight,
				store,
				nil)
			assert.NoError(t, err)
			assert.Equal(t, string(test.expectedStatus), string(run.Status))
			require.Len(t, run.TaskRuns, 1)
			max := utils.MaxUint32(test.taskConfirmations, test.configConfirmations)
			assert.Equal(t, max, run.TaskRuns[0].MinimumConfirmations.Uint32)
		})
	}
}

func TestNewRun_startAtAndEndAt(t *testing.T) {
	pastTime := cltest.ParseNullableTime(t, "2000-01-01T00:00:00.000Z")
	futureTime := cltest.ParseNullableTime(t, "3000-01-01T00:00:00.000Z")
	nullTime := null.Time{Valid: false}

	tests := []struct {
		name    string
		startAt null.Time
		endAt   null.Time
		errored bool
	}{
		{"job not started", futureTime, nullTime, true},
		{"job started", pastTime, futureTime, false},
		{"job with no time range", nullTime, nullTime, false},
		{"job ended", nullTime, pastTime, true},
	}

	store, cleanup := cltest.NewStore(t)
	defer cleanup()
	clock := cltest.UseSettableClock(store)
	clock.SetTime(time.Now())

	for _, tt := range tests {
		test := tt
		t.Run(test.name, func(t *testing.T) {
			job := cltest.NewJobWithWebInitiator()
			job.StartAt = test.startAt
			job.EndAt = test.endAt
			assert.Nil(t, store.CreateJob(&job))

			_, err := services.NewRun(&job, &job.Initiators[0], &models.RunResult{}, nil, store, nil)
			if test.errored {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNewRun_noTasksErrorsInsteadOfPanic(t *testing.T) {
	store, cleanup := cltest.NewStore(t)
	defer cleanup()

	job := cltest.NewJobWithWebInitiator()
	job.Tasks = []models.TaskSpec{}
	require.NoError(t, store.CreateJob(&job))

	jr, err := services.NewRun(&job, &job.Initiators[0], &models.RunResult{}, nil, store, nil)
	assert.NoError(t, err)
	assert.True(t, jr.Status.Errored())
	assert.True(t, jr.Result.HasError())
}

func sleepAdapterParams(t testing.TB, n int) models.JSON {
	d := time.Duration(n)
	json := []byte(fmt.Sprintf(`{"until":%v}`, time.Now().Add(d*time.Second).Unix()))
	return cltest.ParseJSON(t, bytes.NewBuffer(json))
}

func TestQueueSleepingTask(t *testing.T) {
	store, cleanup := cltest.NewStore(t)
	defer cleanup()
	store.Clock = cltest.NeverClock{}

	// reject a run with an invalid state
	jobID := models.NewID()
	runID := models.NewID()
	run := &models.JobRun{
		ID:        runID,
		JobSpecID: jobID,
	}
	err := services.QueueSleepingTask(run, store)
	assert.Error(t, err)

	// reject a run with no tasks
	run = &models.JobRun{
		ID:        runID,
		JobSpecID: jobID,
		Status:    models.RunStatusPendingSleep,
	}
	err = services.QueueSleepingTask(run, store)
	assert.Error(t, err)

	jobSpec := models.JobSpec{ID: models.NewID()}
	require.NoError(t, store.ORM.CreateJob(&jobSpec))

	// reject a run that is sleeping but its task is not
	run = &models.JobRun{
		ID:        models.NewID(),
		JobSpecID: jobSpec.ID,
		Status:    models.RunStatusPendingSleep,
		TaskRuns: []models.TaskRun{models.TaskRun{
			ID:       models.NewID(),
			TaskSpec: models.TaskSpec{Type: adapters.TaskTypeSleep, JobSpecID: jobSpec.ID},
		}},
	}
	require.NoError(t, store.CreateJobRun(run))
	err = services.QueueSleepingTask(run, store)
	assert.Error(t, err)

	// error decoding params into adapter
	inputFromTheFuture := cltest.ParseJSON(t, bytes.NewBuffer([]byte(`{"until": -1}`)))
	run = &models.JobRun{
		ID:        models.NewID(),
		JobSpecID: jobSpec.ID,
		Status:    models.RunStatusPendingSleep,
		TaskRuns: []models.TaskRun{
			models.TaskRun{
				ID:     models.NewID(),
				Status: models.RunStatusPendingSleep,
				TaskSpec: models.TaskSpec{
					JobSpecID: jobSpec.ID,
					Type:      adapters.TaskTypeSleep,
					Params:    inputFromTheFuture,
				},
			},
		},
	}
	require.NoError(t, store.CreateJobRun(run))
	err = services.QueueSleepingTask(run, store)
	assert.NoError(t, err)
	assert.Equal(t, string(models.RunStatusErrored), string(run.TaskRuns[0].Status))
	assert.Equal(t, string(models.RunStatusErrored), string(run.Status))

	// mark run as pending, task as completed if duration has already elapsed
	run = &models.JobRun{
		ID:        models.NewID(),
		JobSpecID: jobSpec.ID,
		Status:    models.RunStatusPendingSleep,
		TaskRuns: []models.TaskRun{models.TaskRun{
			ID:       models.NewID(),
			Status:   models.RunStatusPendingSleep,
			TaskSpec: models.TaskSpec{Type: adapters.TaskTypeSleep, JobSpecID: jobSpec.ID},
		}},
	}
	require.NoError(t, store.CreateJobRun(run))
	err = services.QueueSleepingTask(run, store)
	assert.NoError(t, err)
	assert.Equal(t, string(models.RunStatusCompleted), string(run.TaskRuns[0].Status))
	assert.Equal(t, string(models.RunStatusInProgress), string(run.Status))

	runRequest, open := <-store.RunChannel.Receive()
	assert.True(t, open)
	assert.Equal(t, run.ID, runRequest.ID)

}

func TestQueueSleepingTaskA_CompletesSleepingTaskAfterDurationElapsed_Happy(t *testing.T) {
	store, cleanup := cltest.NewStore(t)
	defer cleanup()
	store.Clock = cltest.NeverClock{}

	jobSpec := models.JobSpec{ID: models.NewID()}
	require.NoError(t, store.ORM.CreateJob(&jobSpec))

	// queue up next run if duration has not elapsed yet
	clock := cltest.UseSettableClock(store)
	store.Clock = clock
	clock.SetTime(time.Time{})

	inputFromTheFuture := sleepAdapterParams(t, 60)
	run := &models.JobRun{
		ID:        models.NewID(),
		JobSpecID: jobSpec.ID,
		Status:    models.RunStatusPendingSleep,
		TaskRuns: []models.TaskRun{
			models.TaskRun{
				ID:     models.NewID(),
				Status: models.RunStatusPendingSleep,
				TaskSpec: models.TaskSpec{
					JobSpecID: jobSpec.ID,
					Type:      adapters.TaskTypeSleep,
					Params:    inputFromTheFuture,
				},
			},
		},
	}
	require.NoError(t, store.CreateJobRun(run))
	err := services.QueueSleepingTask(run, store)
	assert.NoError(t, err)
	assert.Equal(t, string(models.RunStatusPendingSleep), string(run.TaskRuns[0].Status))
	assert.Equal(t, string(models.RunStatusPendingSleep), string(run.Status))

	// force the duration elapse
	clock.SetTime((time.Time{}).Add(math.MaxInt64))
	runRequest, open := <-store.RunChannel.Receive()
	assert.True(t, open)
	assert.Equal(t, run.ID, runRequest.ID)

	*run, err = store.ORM.FindJobRun(run.ID)
	assert.NoError(t, err)
	assert.Equal(t, string(models.RunStatusCompleted), string(run.TaskRuns[0].Status))
	assert.Equal(t, string(models.RunStatusInProgress), string(run.Status))
}

func TestQueueSleepingTaskA_CompletesSleepingTaskAfterDurationElapsed_Archived(t *testing.T) {
	store, cleanup := cltest.NewStore(t)
	defer cleanup()
	store.Clock = cltest.NeverClock{}

	jobSpec := models.JobSpec{ID: models.NewID()}
	require.NoError(t, store.ORM.CreateJob(&jobSpec))

	// queue up next run if duration has not elapsed yet
	clock := cltest.UseSettableClock(store)
	store.Clock = clock
	clock.SetTime(time.Time{})

	inputFromTheFuture := sleepAdapterParams(t, 60)
	run := &models.JobRun{
		ID:        models.NewID(),
		JobSpecID: jobSpec.ID,
		Status:    models.RunStatusPendingSleep,
		TaskRuns: []models.TaskRun{
			models.TaskRun{
				ID:     models.NewID(),
				Status: models.RunStatusPendingSleep,
				TaskSpec: models.TaskSpec{
					JobSpecID: jobSpec.ID,
					Type:      adapters.TaskTypeSleep,
					Params:    inputFromTheFuture,
				},
			},
		},
	}
	require.NoError(t, store.CreateJobRun(run))
	require.NoError(t, store.ArchiveJob(jobSpec.ID))

	unscoped := store.Unscoped()
	err := services.QueueSleepingTask(run, unscoped)
	assert.NoError(t, err)
	assert.Equal(t, string(models.RunStatusPendingSleep), string(run.TaskRuns[0].Status))
	assert.Equal(t, string(models.RunStatusPendingSleep), string(run.Status))

	// force the duration elapse
	clock.SetTime((time.Time{}).Add(math.MaxInt64))
	runRequest, open := <-store.RunChannel.Receive()
	assert.True(t, open)
	assert.Equal(t, run.ID, runRequest.ID)

	require.Error(t, utils.JustError(store.FindJobRun(run.ID)), "archived runs should not be visible to normal store")

	*run, err = unscoped.FindJobRun(run.ID)
	assert.NoError(t, err)
	assert.Equal(t, string(models.RunStatusCompleted), string(run.TaskRuns[0].Status))
	assert.Equal(t, string(models.RunStatusInProgress), string(run.Status))
}
