package workerpool

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	orderedmap "github.com/wk8/go-ordered-map/v2"
)

var (
	ErrAlreadyRunning  = errors.New("already running")
	ErrAlreadyEnded    = errors.New("already ended")
	ErrKilled          = errors.New("killed")
	ErrNotRunning      = errors.New("not running")
	ErrInputChanClosed = errors.New("input chan is closed")
)

var (
	ErrAbnormalReturn = errors.New("abnormal return")
)

type worker[K comparable, T any] struct {
	*Worker[K, T]
	id       K
	cancelFn context.CancelFunc
	sync.Mutex
}

func (w *worker[K, T]) SetCancelFn(fn context.CancelFunc) {
	w.cancelFn = fn
}

func (w *worker[K, T]) Cancel() {
	if w.cancelFn != nil {
		w.cancelFn()
	}
}

// Pool is a collection of workers, which
// holds any number of Worker[K, T]'s, and runs them in goroutines.
type Pool[K comparable, T any] struct {
	wg sync.WaitGroup

	activeWorkerNum int
	taskCh          chan T

	workerCond      *sync.Cond
	workers         *orderedmap.OrderedMap[K, *worker[K, T]]
	sleepingWorkers map[K]*worker[K, T]

	constructor      workerConstructor[K, T]
	onAbnormalReturn func(error)
}

// New creates WorkerPool with 0 worker.
func New[K comparable, T any](
	exec WorkExecuter[K, T],
	idPool IdPool[K],
	options ...Option[K, T],
) *Pool[K, T] {
	w := &Pool[K, T]{
		taskCh:           make(chan T),
		workers:          orderedmap.New[K, *worker[K, T]](),
		sleepingWorkers:  make(map[K]*worker[K, T]),
		onAbnormalReturn: func(err error) {},
		workerCond:       sync.NewCond(&sync.Mutex{}),
	}

	add := func(i int) {
		w.workerCond.L.Lock()
		w.activeWorkerNum += i
		w.workerCond.Broadcast()
		w.workerCond.L.Unlock()
	}
	w.constructor = workerConstructor[K, T]{
		IdPool: idPool,
		Exec:   exec,
		TaskCh: w.taskCh,
		recordReceive: func(T) {
			add(1)
		},
		recordDone: func(T, error) {
			add(-1)
		},
	}

	for _, opt := range options {
		opt(w)
	}

	return w
}

// Sender is getter of a sender side of the task channel,
// where you can send tasks to workers.
func (p *Pool[K, T]) Sender() chan<- T {
	return p.taskCh
}

func (p *Pool[K, T]) WaitUntil(condition func(alive, sleeping, active int) bool, action ...func()) {
	p.workerCond.L.Lock()
	defer p.workerCond.L.Unlock()

	for !condition(p.len()) {
		p.workerCond.Wait()
	}
	for _, act := range action {
		act()
	}
}

// Add adds delta number of workers to p.
// This will create new delta number of goroutines.
// delta is limited to be positive and non zero number, otherwise is no-op.
func (p *Pool[K, T]) Add(delta int) (ok bool) {
	if delta <= 0 {
		return false
	}

	p.workerCond.L.Lock()
	defer p.workerCond.L.Unlock()

	for i := 0; i < delta; i++ {
		worker := p.constructor.Build()
		if worker == nil {
			return false
		}

		runCtx, cancel := context.WithCancel(context.Background())
		worker.SetCancelFn(cancel)
		p.workers.Set(worker.id, worker)

		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			defer cancel()
			p.runWorker(runCtx, worker, true, p.onAbnormalReturn)
		}()

	}
	p.workerCond.Broadcast()
	return true
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

func (p *Pool[K, T]) runWorker(
	ctx context.Context,
	worker *worker[K, T],
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
		// This deletion must be observed *after* abnormalReturnCb is called.
		// The users might use WaitUntil to detect abnormal returns, then check errors by side effect of abnormalReturnCb.
		p.workerCond.L.Lock()
		p.workers.Delete(worker.id)
		delete(p.sleepingWorkers, worker.id)
		p.constructor.IdPool.Put(worker.id)
		p.workerCond.Broadcast()
		p.workerCond.L.Unlock()

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

		_, workerErr = worker.Run(ctx, worker.id)
		normalReturn = true
	}()
	if !normalReturn {
		recovered = true
	}
	return
}

// Remove removes delta number of workers from p.
// Removed workers would be held as sleeping if they were still working on a task.
// delta is limited to be positive and non zero number, otherwise is no-op.
func (p *Pool[K, T]) Remove(delta int) {
	if delta <= 0 {
		return
	}

	p.workerCond.L.Lock()
	defer p.workerCond.L.Unlock()

	oldDelta := delta
	cancelWorker := func(predicate func(w *worker[K, T]) bool) {
		old := p.workers.Oldest()
		next := old
		for next != nil {
			if delta == 0 {
				break
			}
			old = next
			next = old.Next()
			if predicate(old.Value) {
				old.Value.Cancel()
				p.workers.Delete(old.Key)
				p.sleepingWorkers[old.Key] = old.Value
				delta--
			}
		}
	}

	cancelWorker(func(w *worker[K, T]) bool { return !w.State().IsActive() })
	cancelWorker(func(w *worker[K, T]) bool { return true })

	if delta != oldDelta {
		p.workerCond.Broadcast()
	}
}

// Len returns number of workers.
// alive is running workers. sleeping is workers removed by Remove but still working on its task.
func (p *Pool[K, T]) Len() (alive, sleeping, active int) {
	p.workerCond.L.Lock()
	defer p.workerCond.L.Unlock()
	return p.len()
}

func (p *Pool[K, T]) len() (alive, sleeping, active int) {
	return p.workers.Len(), len(p.sleepingWorkers), p.activeWorkerNum
}

// Kill kills all workers.
func (p *Pool[K, T]) Kill() {
	p.workerCond.L.Lock()
	defer p.workerCond.L.Unlock()

	for pair := p.workers.Oldest(); pair != nil; pair = pair.Next() {
		pair.Value.Kill()
	}
	for _, w := range p.sleepingWorkers {
		w.Kill()
	}
}

func (p *Pool[K, T]) Pause(
	ctx context.Context,
	timeout time.Duration,
) (continueWorkers func() (cancelled bool), err error) {
	p.workerCond.L.Lock()

	var wg sync.WaitGroup
	var mu sync.Mutex
	continueFns := make([]func() (cancelled bool), 0, p.workers.Len())
	for pair := p.workers.Oldest(); pair != nil; pair = pair.Next() {
		wg.Add(1)
		go func(worker *worker[K, T]) {
			defer wg.Done()
			cont, err := worker.Pause(ctx, timeout)
			if err == nil {
				mu.Lock()
				continueFns = append(continueFns, cont)
				mu.Unlock()
			}
		}(pair.Value)
	}

	p.workerCond.L.Unlock()

	wg.Wait()

	continueFn := func() (cancelled bool) {
		alreadyCancelled := false
		for _, fn := range continueFns {
			if !fn() {
				alreadyCancelled = true
			}
		}
		return !alreadyCancelled
	}

	select {
	case <-ctx.Done():
		continueFn()
		return nil, ctx.Err()
	default:
	}

	return continueFn, nil
}

// Wait waits for all workers to stop.
// Calling this without Kill and/or Remove all workers may block forever.
func (p *Pool[K, T]) Wait() {
	p.wg.Wait()
}
