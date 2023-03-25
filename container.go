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
	cancel   context.CancelFunc
	Sleeping bool
}

type Container[K comparable, T any] struct {
	pool WorkerPool[K, T]

	workers  map[K]*wrappedWorker[K, T]
	sleeping map[K]struct{}

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
		workers:          make(map[K]*wrappedWorker[K, T]),
		sleeping:         make(map[K]struct{}),
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
		wrapped := &wrappedWorker[K, T]{Worker: worker, cancel: cancel}
		c.workers[wrapped.Id()] = wrapped
		c.wg.Add(1)
		go func() {
			defer func() {
				// These must be done in defer func
				// since it could abnormally return by calling runtime.Goexit.
				cancel()

				c.workerCond.L.Lock()
				delete(c.workers, worker.Id())
				delete(c.sleeping, worker.Id())
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
	worker *wrappedWorker[K, T],
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

		workerErr = worker.Run(ctx, c.taskCh)
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
	cancelWorker := func(predicate func(w *wrappedWorker[K, T]) bool) {
		for k, w := range c.workers {
			if delta == 0 {
				break
			}
			if predicate(w) {
				delta--
				c.sleeping[k] = struct{}{}
				w.Sleeping = true
				w.cancel()
			}
		}
	}

	cancelWorker(func(w *wrappedWorker[K, T]) bool { return w.State() == Active })
	cancelWorker(func(*wrappedWorker[K, T]) bool { return true })

	removed = oldDelta - delta
	return removed
}

// Len returns length of workers.
// It also additionally reports sleeping and active workers.
// active is a non-negative value only if fetchActive is true, otherwise negative.
//
// sleeping means a worker is being deleted via Remove and awaited for completion of a current task,
// thus not receiving new tasks.
// Sleeping is always workers >= sleeping.
//
// active indicates a worker is actively working on a task.
// It may report an incorrect active worker count, since Container[K, T] does not lock the state of workers.
// However there is an invariant where it is always workers >= active.
func (c *Container[K, T]) Len(fetchActive bool) (workers, sleeping, active int) {
	c.workerCond.L.Lock()
	defer c.workerCond.L.Unlock()

	active = -1

	if fetchActive {
		active = 0
		for _, w := range c.workers {
			if w.State() == Active {
				active++
			}
		}
	}

	return len(c.workers), len(c.sleeping), active
}

func (c *Container[K, T]) Kill() {
	c.workerCond.L.Lock()
	defer c.workerCond.L.Unlock()

	for _, w := range c.workers {
		w.Kill()
	}
}

func (c *Container[K, T]) Pause(ctx context.Context, fn func(ctx context.Context)) (err error) {
	// 1. wait until all workers pause.
	//    If ctx is cancelled release all waiter.
	// 2. call passed fn.
	c.workerCond.L.Lock()
	defer c.workerCond.L.Unlock()

	workerNum := len(c.workers) - len(c.sleeping)

	// fast path
	if workerNum == 0 {
		return nil
	}

	workerPauseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workerPause := make(chan struct{}, workerNum)
	workerPauseFn := func(ctx context.Context) {
		workerPause <- struct{}{}
		<-ctx.Done()
	}

	var waitGoroutine sync.WaitGroup
	for _, w := range c.workers {
		// sleeping indicates the context passed to Worker's Run is already cancelled.
		// It cannot be stepped into Paused state.
		if w.Sleeping {
			continue
		}

		waitGoroutine.Add(1)
		go func(w *wrappedWorker[K, T]) {
			defer waitGoroutine.Done()
			w.Pause(workerPauseCtx, workerPauseFn)
		}(w)
	}

	var pausedWorker int
	for {
		if pausedWorker == workerNum {
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
func (c *Container[K, T]) WaitUntil(condition func(workers, sleeping int) bool, action ...func()) {
	c.workerCond.L.Lock()
	defer c.workerCond.L.Unlock()

	for !condition(len(c.workers), len(c.sleeping)) {
		c.workerCond.Wait()
	}
	for _, act := range action {
		act()
	}
}

func (c *Container[K, T]) Load(key K) (value *wrappedWorker[K, T], ok bool) {
	c.workerCond.L.Lock()
	defer c.workerCond.L.Unlock()

	value, ok = c.workers[key]

	return value, ok
}

func (c *Container[K, T]) Range(f func(key K, value *wrappedWorker[K, T]) bool) {
	c.workerCond.L.Lock()
	defer c.workerCond.L.Unlock()

	for k, v := range c.workers {
		if !f(k, v) {
			break
		}
	}
}
