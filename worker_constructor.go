package workerpool

// workerConstructor is a aggregated parameters
// which are related to worker construction.
type workerConstructor[T any] struct {
	IdGenerator   func() string
	Exec          WorkExecuter[T]
	OnReceive     func(p T)
	OnDone        func(p T, err error)
	TaskCh        chan T
	recordReceive func(p T)
	recordDone    func(p T, err error)
}

func (p *workerConstructor[T]) Build() *worker[T] {
	return &worker[T]{
		id:     p.IdGenerator(),
		Worker: p.build(),
	}
}

func (p *workerConstructor[T]) build() Worker[T] {
	combinedOnTaskReceived := func(param T) {
		if p.OnReceive != nil {
			p.OnReceive(param)
		}
		if p.recordReceive != nil {
			p.recordReceive(param)
		}
	}
	combinedOnTaskDone := func(param T, err error) {
		if p.recordDone != nil {
			p.recordDone(param, err)
		}
		if p.OnDone != nil {
			p.OnDone(param, err)
		}
	}
	return NewWorker(p.Exec, p.TaskCh, combinedOnTaskReceived, combinedOnTaskDone)
}
