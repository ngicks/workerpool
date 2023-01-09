package workerpool

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
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

type worker[T any] struct {
	Worker[T]
	id       string
	cancelFn context.CancelFunc
	sync.Mutex
}

func (w *worker[T]) SetCancelFn(fn context.CancelFunc) {
	w.Lock()
	w.cancelFn = fn
	w.Unlock()
}

func (w *worker[T]) Cancel() {
	w.Lock()
	defer w.Unlock()

	if w.cancelFn != nil {
		w.cancelFn()
	}
}

// Pool is a collection of workers, which
// holds any number of Worker[T]'s, and runs them in goroutines.
type Pool[T any] struct {
	wg sync.WaitGroup

	activeWorkerNum atomic.Int64
	taskCh          chan T

	workerMu        sync.Mutex
	workers         *orderedmap.OrderedMap[string, *worker[T]]
	sleepingWorkers map[string]*worker[T]
	workerEndCh     chan string

	constructor      workerConstructor[T]
	onAbnormalReturn func(error)
}

// New creates WorkerPool with 0 worker.
func New[T any](
	exec WorkExecuter[T],
	options ...Option[T],
) *Pool[T] {
	w := &Pool[T]{
		taskCh:           make(chan T),
		workers:          orderedmap.New[string, *worker[T]](),
		sleepingWorkers:  make(map[string]*worker[T]),
		onAbnormalReturn: func(err error) {},
	}

	w.constructor = workerConstructor[T]{
		IdGenerator:   uuid.NewString,
		Exec:          exec,
		TaskCh:        w.taskCh,
		recordReceive: func(T) { w.activeWorkerNum.Add(1) },
		recordDone:    func(T, error) { w.activeWorkerNum.Add(-1) },
	}

	for _, opt := range options {
		opt(w)
	}

	return w
}

// Sender is getter of a sender side of the task channel,
// where you can send tasks to workers.
func (p *Pool[T]) Sender() chan<- T {
	return p.taskCh
}

// Add adds delta number of workers to p.
// This will create new delta number of goroutines.
// delta is limited to be positive and non zero number, otherwise is no-op.
func (p *Pool[T]) Add(delta int) {
	if delta <= 0 {
		return
	}

	p.workerMu.Lock()
	defer p.workerMu.Unlock()

	for i := 0; i < delta; i++ {
		worker := p.constructor.Build()
		p.wg.Add(1)
		p.workers.Set(worker.id, worker)

		go func() {
			defer p.wg.Done()
			p.runWorker(worker, true, p.onAbnormalReturn)
		}()
	}
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

func (p *Pool[T]) runWorker(
	worker *worker[T],
	shouldRecover bool,
	abnormalReturnCb func(error),
) (workerErr error) {
	var normalReturn, recovered bool
	var abnormalReturnErr error

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// see https://cs.opensource.google/go/x/sync/+/0de741cf:singleflight/singleflight.go;l=138-200;drc=0de741cfad7ff3874b219dfbc1b9195b58c7c490
	defer func() {
		p.workerMu.Lock()
		p.workers.Delete(worker.id)
		delete(p.sleepingWorkers, worker.id)
		p.workerMu.Unlock()

		if p.workerEndCh != nil {
			p.workerEndCh <- worker.id
		}

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

		worker.SetCancelFn(cancel)
		_, workerErr = worker.Run(ctx)
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
func (p *Pool[T]) Remove(delta int) {
	if delta <= 0 {
		return
	}

	p.workerMu.Lock()
	defer p.workerMu.Unlock()

	old := p.workers.Oldest()
	next := old
	for next != nil {
		if delta == 0 {
			break
		}
		old = next
		next = old.Next()
		old.Value.Cancel()
		p.workers.Delete(old.Key)
		p.sleepingWorkers[old.Key] = old.Value
		delta--
	}
}

// Len returns number of workers.
// alive is running workers. sleeping is workers removed by Remove but still working on its task.
func (p *Pool[T]) Len() (alive, sleeping int) {
	p.workerMu.Lock()
	defer p.workerMu.Unlock()
	return p.workers.Len(), len(p.sleepingWorkers)
}

// ActiveWorkerNum returns number of actively working worker.
func (p *Pool[T]) ActiveWorkerNum() int64 {
	return p.activeWorkerNum.Load()
}

// Kill kills all workers.
func (p *Pool[T]) Kill() {
	p.workerMu.Lock()
	defer p.workerMu.Unlock()

	for pair := p.workers.Oldest(); pair != nil; pair = pair.Next() {
		pair.Value.Kill()
	}
	for _, w := range p.sleepingWorkers {
		w.Kill()
	}
}

func (p *Pool[T]) Pause(
	ctx context.Context,
	timeout time.Duration,
) (continueWorkers func() (cancelled bool), err error) {
	p.workerMu.Lock()
	defer p.workerMu.Unlock()

	var wg sync.WaitGroup
	var mu sync.Mutex
	continueFns := make([]func() (cancelled bool), 0, p.workers.Len())
	for pair := p.workers.Oldest(); pair != nil; pair = pair.Next() {
		wg.Add(1)
		go func(worker *worker[T]) {
			defer wg.Done()
			cont, err := worker.Pause(ctx, timeout)
			if err == nil {
				mu.Lock()
				continueFns = append(continueFns, cont)
				mu.Unlock()
			}
		}(pair.Value)
	}

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
func (p *Pool[T]) Wait() {
	p.wg.Wait()
}
