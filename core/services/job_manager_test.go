package services_test

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/smartcontractkit/chainlink/core/adapters"
	"github.com/smartcontractkit/chainlink/core/internal/cltest"
	clnull "github.com/smartcontractkit/chainlink/core/null"
	"github.com/smartcontractkit/chainlink/core/services"
	"github.com/smartcontractkit/chainlink/core/store/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestJobManager_ResumePendingTask(t *testing.T) {
	// reject a run with an invalid state
	jobID := models.NewID()
	runID := models.NewID()
	run := &models.JobRun{
		ID:        runID,
		JobSpecID: jobID,
	}
	err := services.ResumePendingTask(run, models.RunResult{})
	assert.Error(t, err)

	// reject a run with no tasks
	run = &models.JobRun{Status: models.RunStatusPendingBridge}
	err = services.ResumePendingTask(run, models.RunResult{})
	assert.Error(t, err)

	// input with error errors run
	run = &models.JobRun{
		ID:        runID,
		JobSpecID: jobID,
		Status:    models.RunStatusPendingBridge,
		TaskRuns:  []models.TaskRun{models.TaskRun{ID: models.NewID(), JobRunID: runID}},
	}
	err = services.ResumePendingTask(run, models.RunResult{CachedJobRunID: runID, Status: models.RunStatusErrored})
	assert.NoError(t, err)
	assert.True(t, run.FinishedAt.Valid)
	assert.Len(t, run.TaskRuns, 1)
	assert.Equal(t, string(models.RunStatusErrored), string(run.TaskRuns[0].Result.Status))

	// completed input with remaining tasks should put task into pending
	run = &models.JobRun{
		ID:        runID,
		JobSpecID: jobID,
		Status:    models.RunStatusPendingBridge,
		TaskRuns:  []models.TaskRun{models.TaskRun{ID: models.NewID(), JobRunID: runID}, models.TaskRun{ID: models.NewID(), JobRunID: runID}},
	}
	input := models.JSON{Result: gjson.Parse(`{"address":"0xdfcfc2b9200dbb10952c2b7cce60fc7260e03c6f"}`)}
	err = services.ResumePendingTask(run, models.RunResult{CachedJobRunID: runID, Data: input, Status: models.RunStatusCompleted})
	assert.NoError(t, err)
	assert.Equal(t, string(models.RunStatusInProgress), string(run.Status))
	assert.Len(t, run.TaskRuns, 2)
	assert.Equal(t, run.ID, run.TaskRuns[0].Result.CachedJobRunID)
	assert.Equal(t, string(models.RunStatusCompleted), string(run.TaskRuns[0].Result.Status))

	// completed input with no remaining tasks should get marked as complete
	run = &models.JobRun{
		ID:        runID,
		JobSpecID: jobID,
		Status:    models.RunStatusPendingBridge,
		TaskRuns:  []models.TaskRun{models.TaskRun{ID: models.NewID(), JobRunID: runID}},
	}
	err = services.ResumePendingTask(run, models.RunResult{CachedJobRunID: runID, Data: input, Status: models.RunStatusCompleted})
	assert.NoError(t, err)
	assert.Equal(t, string(models.RunStatusCompleted), string(run.Status))
	assert.True(t, run.FinishedAt.Valid)
	assert.Len(t, run.TaskRuns, 1)
	assert.Equal(t, run.ID, run.TaskRuns[0].Result.CachedJobRunID)
	assert.Equal(t, string(models.RunStatusCompleted), string(run.TaskRuns[0].Result.Status))
}

func TestJobManager_ResumeConfirmingTask(t *testing.T) {
	store, cleanup := cltest.NewStore(t)
	defer cleanup()

	// reject a run with no tasks
	run := &models.JobRun{Status: models.RunStatusPendingConfirmations}
	err := services.ResumeConfirmingTask(run, nil, store.TxManager)
	assert.Error(t, err)

	// leave in pending if not enough confirmations have been met yet
	creationHeight := models.NewBig(big.NewInt(0))
	run = &models.JobRun{
		ID:             models.NewID(),
		CreationHeight: creationHeight,
		Status:         models.RunStatusPendingConfirmations,
		TaskRuns: []models.TaskRun{models.TaskRun{
			ID:                   models.NewID(),
			MinimumConfirmations: clnull.Uint32From(2),
			TaskSpec: models.TaskSpec{
				Type: adapters.TaskTypeNoOp,
			},
		}},
	}
	err = services.ResumeConfirmingTask(run, creationHeight.ToInt(), store.TxManager)
	assert.NoError(t, err)
	assert.Equal(t, string(models.RunStatusPendingConfirmations), string(run.Status))
	assert.Equal(t, uint32(1), run.TaskRuns[0].Confirmations.Uint32)

	// input, should go from pending -> in progress and save the input
	run = &models.JobRun{
		ID:             models.NewID(),
		CreationHeight: creationHeight,
		Status:         models.RunStatusPendingConfirmations,
		TaskRuns: []models.TaskRun{models.TaskRun{
			ID:                   models.NewID(),
			MinimumConfirmations: clnull.Uint32From(1),
			TaskSpec: models.TaskSpec{
				Type: adapters.TaskTypeNoOp,
			},
		}},
	}
	observedHeight := big.NewInt(1)
	err = services.ResumeConfirmingTask(run, observedHeight, store.TxManager)
	assert.NoError(t, err)
	assert.Equal(t, string(models.RunStatusInProgress), string(run.Status))
}

func TestJobManager_ResumeConnectingTask(t *testing.T) {
	// reject a run with no tasks
	run := &models.JobRun{
		ID:        models.NewID(),
		JobSpecID: models.NewID(),
		Status:    models.RunStatusPendingConnection,
	}
	err := services.ResumeConnectingTask(run)
	assert.Error(t, err)

	// input, should go from pending -> in progress
	run = &models.JobRun{
		ID:        models.NewID(),
		JobSpecID: models.NewID(),
		Status:    models.RunStatusPendingConnection,
		TaskRuns: []models.TaskRun{models.TaskRun{
			ID: models.NewID(),
		}},
	}
	err = services.ResumeConnectingTask(run)
	assert.NoError(t, err)
	assert.Equal(t, string(models.RunStatusInProgress), string(run.Status))
}

func TestJobManager_ExecuteJob_DoesNotSaveToTaskSpec(t *testing.T) {
	t.Parallel()
	app, cleanup := cltest.NewApplication(t)
	defer cleanup()

	store := app.Store
	eth := cltest.MockEthOnStore(t, store)
	eth.Register("eth_chainId", store.Config.ChainID())

	app.Start()

	job := cltest.NewJobWithWebInitiator()
	job.Tasks = []models.TaskSpec{cltest.NewTask(t, "NoOp")} // empty params
	require.NoError(t, store.CreateJob(&job))

	initr := job.Initiators[0]
	input := cltest.RunResultWithData(`{"random": "input"}`)
	jr, err := app.JobManager.ExecuteJob(job.ID, &initr, &input, nil)
	require.NoError(t, err)
	cltest.WaitForJobRunToComplete(t, store, *jr)

	retrievedJob, err := store.FindJob(job.ID)
	require.NoError(t, err)
	require.Len(t, job.Tasks, 1)
	require.Len(t, retrievedJob.Tasks, 1)
	assert.Equal(t, job.Tasks[0].Params, retrievedJob.Tasks[0].Params)
}

func TestJobManager_ExecuteJobWithRunRequest(t *testing.T) {
	t.Parallel()
	app, cleanup := cltest.NewApplication(t)
	defer cleanup()

	store := app.Store
	eth := cltest.MockEthOnStore(t, store)
	eth.Register("eth_chainId", store.Config.ChainID())

	app.Start()

	job := cltest.NewJobWithRunLogInitiator()
	job.Tasks = []models.TaskSpec{cltest.NewTask(t, "NoOp")} // empty params
	require.NoError(t, store.CreateJob(&job))

	requestID := "RequestID"
	initr := job.Initiators[0]
	rr := models.NewRunRequest()
	rr.RequestID = &requestID
	input := cltest.RunResultWithData(`{"random": "input"}`)
	jr, err := app.JobManager.ExecuteJobWithRunRequest(
		job.ID,
		&initr,
		&input,
		nil,
		rr,
	)
	require.NoError(t, err)
	updatedJR := cltest.WaitForJobRunToComplete(t, store, *jr)
	assert.Equal(t, rr.RequestID, updatedJR.RunRequest.RequestID)
}

func TestJobManager_ExecuteJobWithRunRequest_fromRunLog_Happy(t *testing.T) {
	t.Parallel()

	initiatingTxHash := cltest.NewHash()
	triggeringBlockHash := cltest.NewHash()
	otherBlockHash := cltest.NewHash()

	tests := []struct {
		name             string
		logBlockHash     common.Hash
		receiptBlockHash common.Hash
		wantStatus       models.RunStatus
	}{
		{
			name:             "main chain",
			logBlockHash:     triggeringBlockHash,
			receiptBlockHash: triggeringBlockHash,
			wantStatus:       models.RunStatusCompleted,
		},
		{
			name:             "ommered chain",
			logBlockHash:     triggeringBlockHash,
			receiptBlockHash: otherBlockHash,
			wantStatus:       models.RunStatusErrored,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config, cfgCleanup := cltest.NewConfig(t)
			defer cfgCleanup()
			minimumConfirmations := uint32(2)
			config.Set("MIN_INCOMING_CONFIRMATIONS", minimumConfirmations)
			app, cleanup := cltest.NewApplicationWithConfig(t, config)
			defer cleanup()

			eth := app.MockEthCallerSubscriber()
			app.Start()

			store := app.GetStore()
			job := cltest.NewJobWithRunLogInitiator()
			job.Tasks = []models.TaskSpec{cltest.NewTask(t, "NoOp")}
			require.NoError(t, store.CreateJob(&job))

			creationHeight := big.NewInt(1)
			requestID := "RequestID"
			initr := job.Initiators[0]
			rr := models.NewRunRequest()
			rr.RequestID = &requestID
			rr.TxHash = &initiatingTxHash
			rr.BlockHash = &test.logBlockHash
			input := cltest.RunResultWithData(`{"random": "input"}`)
			jr, err := app.JobManager.ExecuteJobWithRunRequest(
				job.ID,
				&initr,
				&input,
				creationHeight,
				rr,
			)
			require.NoError(t, err)
			cltest.WaitForJobRunToPendConfirmations(t, app.Store, *jr)

			confirmedReceipt := models.TxReceipt{
				Hash:        initiatingTxHash,
				BlockHash:   &test.receiptBlockHash,
				BlockNumber: cltest.Int(3),
			}
			eth.Context("validateOnMainChain", func(ethMock *cltest.EthMock) {
				eth.Register("eth_getTransactionReceipt", confirmedReceipt)
			})

			err = app.JobManager.ResumeConfirmingTasks(big.NewInt(2))
			require.NoError(t, err)
			updatedJR := cltest.WaitForJobRunStatus(t, store, *jr, test.wantStatus)
			assert.Equal(t, rr.RequestID, updatedJR.RunRequest.RequestID)
			assert.Equal(t, minimumConfirmations, updatedJR.TaskRuns[0].MinimumConfirmations.Uint32)
			assert.True(t, updatedJR.TaskRuns[0].MinimumConfirmations.Valid)
			assert.Equal(t, minimumConfirmations, updatedJR.TaskRuns[0].Confirmations.Uint32, "task run should track its current confirmations")
			assert.True(t, updatedJR.TaskRuns[0].Confirmations.Valid)
			assert.True(t, eth.AllCalled(), eth.Remaining())
		})
	}
}

func TestJobManager_ExecuteJobWithRunRequest_fromRunLog_ConnectToLaggingEthNode(t *testing.T) {
	t.Parallel()

	initiatingTxHash := cltest.NewHash()
	triggeringBlockHash := cltest.NewHash()

	config, cfgCleanup := cltest.NewConfig(t)
	defer cfgCleanup()
	minimumConfirmations := uint32(2)
	config.Set("MIN_INCOMING_CONFIRMATIONS", minimumConfirmations)
	app, cleanup := cltest.NewApplicationWithConfig(t, config)
	defer cleanup()

	eth := app.MockEthCallerSubscriber()
	app.MockStartAndConnect()

	store := app.GetStore()
	job := cltest.NewJobWithRunLogInitiator()
	job.Tasks = []models.TaskSpec{cltest.NewTask(t, "NoOp")}
	require.NoError(t, store.CreateJob(&job))

	requestID := "RequestID"
	initr := job.Initiators[0]
	rr := models.NewRunRequest()
	rr.RequestID = &requestID
	rr.TxHash = &initiatingTxHash
	rr.BlockHash = &triggeringBlockHash

	futureCreationHeight := big.NewInt(9)
	pastCurrentHeight := big.NewInt(1)

	input := cltest.RunResultWithData(`{"random": "input"}`)
	jr, err := app.JobManager.ExecuteJobWithRunRequest(
		job.ID,
		&initr,
		&input,
		futureCreationHeight,
		rr,
	)
	require.NoError(t, err)
	cltest.WaitForJobRunToPendConfirmations(t, app.Store, *jr)

	err = app.JobManager.ResumeConfirmingTasks(pastCurrentHeight)
	require.NoError(t, err)
	updatedJR := cltest.WaitForJobRunToPendConfirmations(t, app.Store, *jr)
	assert.True(t, updatedJR.TaskRuns[0].Confirmations.Valid)
	assert.Equal(t, uint32(0), updatedJR.TaskRuns[0].Confirmations.Uint32)
	assert.True(t, eth.AllCalled(), eth.Remaining())
}
