package services

//go:generate mockery -name JobUpdater -output ../internal/mocks/ -case=underscore

// JobUpdater is solely responsible for commiting changes to JobRuns to storage
type JobUpdater interface {
	Start() error
	Stop()
}

//type jobUpdater struct {
//updates chan *runUpdate
//}

//type runUpdate struct {
//run    *models.JobRun
//result chan error
//}

//// NewJobUpdater returns a new job updater
//func NewJobUpdater() JobManager {
//ju := &jobUpdater{
//updates: make(chan *runUpdate),
//}
//go ju.run()
//return ju
//}

//func (ju *jobUpdater) run() {
//for {
//select {
//case update := <-jm.updates:
//if err := jm.orm.SaveJobRun(update.run); err != nil {
//update.result <- err
//continue
//}
//update.result <- jm.jobRunner.Run(update.run.ID)
//}
//}
//}
