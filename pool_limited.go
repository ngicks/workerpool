package workerpool

import "sync"

var _ WorkerPool[string, string] = (*LimitedWorkerPool[string, string])(nil)

type LimitedWorkerPool[K comparable, T any] struct {
	mu     sync.Mutex
	idPool IdPool[K]
	exec   WorkExecuter[K, T]
}

func NewLimitedWorkerPool[K comparable, T any](
	idPool IdPool[K],
	exec WorkExecuter[K, T],
) *LimitedWorkerPool[K, T] {
	return &LimitedWorkerPool[K, T]{
		idPool: idPool,
		exec:   exec,
	}
}

func (p *LimitedWorkerPool[K, T]) Get() (w Worker[K, T], ok bool) {
	if id, ok := p.idPool.Get(); ok {
		w = NewExecutorWorker(id, p.exec)
		if init, ok := p.exec.(WorkExecutorInitializer[K]); ok {
			init.Start(id)
		}
		return w, true
	}
	return nil, false
}

func (p *LimitedWorkerPool[K, T]) Put(w Worker[K, T]) {
	if init, ok := p.exec.(WorkExecutorInitializer[K]); ok {
		init.Stop(w.Id())
	}
	p.idPool.Put(w.Id())
}
