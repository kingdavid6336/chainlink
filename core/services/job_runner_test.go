package services_test

import (
	"testing"

	"github.com/smartcontractkit/chainlink/core/internal/cltest"
	"github.com/smartcontractkit/chainlink/core/internal/mocks"
	"github.com/smartcontractkit/chainlink/core/services"
	"github.com/smartcontractkit/chainlink/core/store/models"
	"github.com/stretchr/testify/mock"
)

func TestJobRunner(t *testing.T) {
	t.Parallel()

	je := new(mocks.JobExecutor)
	jr := services.NewJobRunner(je)

	executeJobChannel := make(chan struct{})

	jr.Start()
	defer jr.Stop()

	je.On("Execute", mock.Anything).
		Return(nil, nil).
		Run(func(mock.Arguments) {
			executeJobChannel <- struct{}{}
		})

	jr.Run(&models.JobRun{ID: models.NewID()})

	cltest.CallbackOrTimeout(t, "Execute", func() {
		<-executeJobChannel
	})

	je.AssertExpectations(t)

	//assert.Equal(t, 0, jr.workerCount())
}
