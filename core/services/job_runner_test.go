package services_test

import (
	"testing"

	"github.com/onsi/gomega"
	"github.com/smartcontractkit/chainlink/core/internal/cltest"
	"github.com/smartcontractkit/chainlink/core/internal/mocks"
	"github.com/smartcontractkit/chainlink/core/services"
	"github.com/smartcontractkit/chainlink/core/store/models"
	"github.com/stretchr/testify/mock"
)

func TestJobRunner(t *testing.T) {
	t.Parallel()
	g := gomega.NewGomegaWithT(t)

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

	g.Eventually(func() int {
		return jr.WorkerCount()
	}).Should(gomega.Equal(1))

	cltest.CallbackOrTimeout(t, "Execute", func() {
		<-executeJobChannel
	})

	je.AssertExpectations(t)

	g.Eventually(func() int {
		return jr.WorkerCount()
	}).Should(gomega.Equal(0))
}

func TestJobRunner_OneWorkerPerRun(t *testing.T) {
	t.Parallel()
	g := gomega.NewGomegaWithT(t)

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
	jr.Run(&models.JobRun{ID: models.NewID()})

	g.Eventually(func() int {
		return jr.WorkerCount()
	}).Should(gomega.Equal(2))

	cltest.CallbackOrTimeout(t, "Execute", func() {
		<-executeJobChannel
		<-executeJobChannel
	})

	je.AssertExpectations(t)

	g.Eventually(func() int {
		return jr.WorkerCount()
	}).Should(gomega.Equal(0))
}

func TestJobRunner_OneWorkerForSameRunTriggeredMultipleTimes(t *testing.T) {
	t.Parallel()
	g := gomega.NewGomegaWithT(t)

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

	id := models.NewID()
	jr.Run(&models.JobRun{ID: id})
	jr.Run(&models.JobRun{ID: id})

	g.Eventually(func() int {
		return jr.WorkerCount()
	}).Should(gomega.Equal(1))

	cltest.CallbackOrTimeout(t, "Execute", func() {
		<-executeJobChannel
		<-executeJobChannel
	})

	je.AssertExpectations(t)

	g.Eventually(func() int {
		return jr.WorkerCount()
	}).Should(gomega.Equal(0))
}
