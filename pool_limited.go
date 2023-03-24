package workerpool

import "sync"

var _ WorkerPool[string, string] = (*LimitedWorkerPool[string, string])(nil)

type LimitedWorkerPool[K comparable, T any] struct {
	mu sync.Mutex

	hook Hook[K, T]

	exec WorkExecuter[K, T]
}

func (p *LimitedWorkerPool[K, T]) SetHook(h Hook[K, T])
func (p *LimitedWorkerPool[K, T]) Get() (w Worker[K, T], ok bool) {
	return NewExecutorWorker[K, T]()
}
func (p *LimitedWorkerPool[K, T]) Put(w Worker[K, T])
