package workerpool

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
)

type wrappedWorker[K comparable, T any] struct {
	Worker[K, T]
	cancel context.CancelFunc
}

var _ onlyExposable[int] = (*Container[int, int])(nil)

type Container[K comparable, T any] struct {
	pool WorkerPool[K, T]

	activeWorkers   map[K]wrappedWorker[K, T]
	sleepingWorkers map[K]wrappedWorker[K, T]

	taskCh chan T

	wg sync.WaitGroup

	workerCond *sync.Cond

	shouldRecover    bool
	onAbnormalReturn func(error)
}

func NewContainer[K comparable, T any](
	pool WorkerPool[K, T],
	options ...Option[K, T],
) *Container[K, T] {
	c := &Container[K, T]{
		pool:             pool,
		taskCh:           make(chan T),
		activeWorkers:    make(map[K]wrappedWorker[K, T]),
		sleepingWorkers:  make(map[K]wrappedWorker[K, T]),
		onAbnormalReturn: func(err error) {},
		workerCond:       sync.NewCond(&sync.Mutex{}),
	}

	for _, opt := range options {
		opt(c)
	}

	return c
}

func (c *Container[K, T]) Sender() chan<- T {
	return c.taskCh
}
func (c *Container[K, T]) Add(delta int) (added int) {
	if delta <= 0 {
		return 0
	}

	c.workerCond.L.Lock()
	defer c.workerCond.L.Unlock()
	defer func() {
		if added > 0 {
			c.workerCond.Broadcast()
		}
	}()

	for i := 0; i < delta; i++ {
		worker, ok := c.pool.Get()
		if !ok {
			break
		}

		added++

		runCtx, cancel := context.WithCancel(context.Background())
		wrapped := wrappedWorker[K, T]{Worker: worker, cancel: cancel}
		c.activeWorkers[wrapped.Id()] = wrapped
		c.wg.Add(1)
		go func() {
			defer func() {
				// These must be done in defer func
				// since it could abnormally return by calling runtime.Goexit.
				cancel()

				c.workerCond.L.Lock()
				delete(c.activeWorkers, worker.Id())
				delete(c.sleepingWorkers, worker.Id())
				c.pool.Put(wrapped.Worker)
				c.workerCond.Broadcast()
				c.workerCond.L.Unlock()

				c.wg.Done()
			}()
			c.runWorker(runCtx, wrapped, c.shouldRecover, c.onAbnormalReturn)
		}()
	}

	return added
}

var (
	errGoexit = errors.New("runtime.Goexit was called")
)

type panicErr struct {
	err   any
	stack []byte
}

// Error implements error interface.
func (p *panicErr) Error() string {
	return fmt.Sprintf("%v\n\n%s", p.err, p.stack)
}

func (c *Container[K, T]) runWorker(
	ctx context.Context,
	worker wrappedWorker[K, T],
	shouldRecover bool,
	abnormalReturnCb func(error),
) (workerErr error) {
	var normalReturn, recovered bool
	var abnormalReturnErr error

	// see https://cs.opensource.google/go/x/sync/+/0de741cf:singleflight/singleflight.go;l=138-200;drc=0de741cfad7ff3874b219dfbc1b9195b58c7c490
	defer func() {
		if !normalReturn && !recovered {
			abnormalReturnErr = errGoexit
		}
		if !normalReturn {
			abnormalReturnCb(abnormalReturnErr)
		}

		if recovered && !shouldRecover {
			panic(abnormalReturnErr)
		}
	}()

	func() {
		defer func() {
			if err := recover(); err != nil {
				abnormalReturnErr = &panicErr{
					err:   err,
					stack: debug.Stack(),
				}
			}
		}()

		_, workerErr = worker.Run(ctx, c.taskCh)
		normalReturn = true
	}()
	if !normalReturn {
		recovered = true
	}
	return
}

func (c *Container[K, T]) Remove(delta int) (removed int) {
	if delta <= 0 {
		return
	}

	c.workerCond.L.Lock()
	defer func() {
		if removed > 0 {
			c.workerCond.Broadcast()
		}
		c.workerCond.L.Unlock()
	}()

	oldDelta := delta
	cancelWorker := func(predicate func(w wrappedWorker[K, T]) bool) {
		for k, w := range c.activeWorkers {
			if delta == 0 {
				break
			}
			if predicate(w) {
				delta--
				w.cancel()
				delete(c.activeWorkers, k)
				c.sleepingWorkers[k] = w
			}
		}
	}

	cancelWorker(func(w wrappedWorker[K, T]) bool { return w.State().IsActive() })
	cancelWorker(func(w wrappedWorker[K, T]) bool { return true })

	removed = oldDelta - delta
	return removed
}

func (c *Container[K, T]) Len() (worker int, sleeping int) {
	c.workerCond.L.Lock()
	defer c.workerCond.L.Unlock()
	return len(c.activeWorkers), len(c.sleepingWorkers)
}

func (c *Container[K, T]) Kill() {
	c.workerCond.L.Lock()
	defer c.workerCond.L.Unlock()

	for _, w := range c.activeWorkers {
		w.Kill()
	}
	for _, w := range c.sleepingWorkers {
		w.Kill()
	}
}

func (c *Container[K, T]) Pause(ctx context.Context, fn func(ctx context.Context)) (err error) {
	// 1. wait until all workers pause.
	//    If ctx is cancelled release all waiter.
	// 2. call passed fn.
	c.workerCond.L.Lock()
	defer c.workerCond.L.Unlock()

	workerPauseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workerPause := make(chan struct{}, len(c.activeWorkers))
	workerPauseFn := func(ctx context.Context) {
		workerPause <- struct{}{}
		<-ctx.Done()
	}

	var waitGoroutine sync.WaitGroup
	for _, w := range c.activeWorkers {
		waitGoroutine.Add(1)
		go func(w wrappedWorker[K, T]) {
			defer waitGoroutine.Done()
			w.Pause(workerPauseCtx, workerPauseFn)
		}(w)
	}

	var pausedWorker int
	for {
		if pausedWorker == len(c.activeWorkers) {
			break
		}
		select {
		case <-workerPause:
			pausedWorker++
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	fn(ctx)
	cancel()
	waitGoroutine.Wait()
	return nil
}
func (c *Container[K, T]) Wait() {
	c.wg.Wait()
}
func (c *Container[K, T]) WaitUntil(condition func(alive int, sleeping int) bool, action ...func()) {
	c.workerCond.L.Lock()
	defer c.workerCond.L.Unlock()

	for !condition(len(c.activeWorkers), len(c.sleepingWorkers)) {
		c.workerCond.Wait()
	}
	for _, act := range action {
		act()
	}
}
