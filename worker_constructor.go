package workerpool

import "context"

// workerConstructor is a aggregated parameters
// which are related to worker construction.
type workerConstructor[K comparable, T any] struct {
	IdPool        IdPool[K]
	Exec          WorkExecuter[K, T]
	OnStart       func(ctx context.Context, k K)
	OnReceive     func(k K, p T)
	OnDone        func(k K, p T, err error)
	TaskCh        chan T
	recordReceive func(k K, p T)
	recordDone    func(k K, p T, err error)
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
	combinedOnTaskReceived := func(id K, param T) {
		if p.OnReceive != nil {
			p.OnReceive(id, param)
		}
		if p.recordReceive != nil {
			p.recordReceive(id, param)
		}
	}
	combinedOnTaskDone := func(id K, param T, err error) {
		if p.recordDone != nil {
			p.recordDone(id, param, err)
		}
		if p.OnDone != nil {
			p.OnDone(id, param, err)
		}
	}

	opts := []workerOption[K, T]{
		SetOnTaskReceived(combinedOnTaskReceived),
		SetOnTaskDone(combinedOnTaskDone),
	}
	if p.OnStart != nil {
		opts = append(opts, SetOnStart[K, T](p.OnStart))
	}
	return NewWorker(p.Exec, p.TaskCh, opts...)
}
