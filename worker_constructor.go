package workerpool

// workerConstructor is a aggregated parameters
// which are related to worker construction.
type workerConstructor[K comparable, T any] struct {
	IdPool        IdPool[K]
	Exec          WorkExecuter[K, T]
	OnReceive     func(p T)
	OnDone        func(p T, err error)
	TaskCh        chan T
	recordReceive func(p T)
	recordDone    func(p T, err error)
}

func (p *workerConstructor[K, T]) Build() *worker[K, T] {
	id, ok := p.IdPool.Get()
	if !ok {
		return nil
	}
	return &worker[K, T]{
		id:     id,
		Worker: p.build(),
	}
}

func (p *workerConstructor[K, T]) build() *Worker[K, T] {
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
