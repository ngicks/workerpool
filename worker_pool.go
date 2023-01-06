package workerpool

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

// goroutines counts the number of goroutines; for testing.
// The author saw context std lib did this, thinking compiler will drop these value outside test files.
var goroutines atomic.Int64

var (
	ErrAlreadyStarted = errors.New("already started")
	ErrAlreadyEnded   = errors.New("already ended")
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

// WorkerPool is container for workers.
type WorkerPool[T any] struct {
	wg sync.WaitGroup

	activeWorkerNum int64
	paramCh         chan T

	workerMu        sync.Mutex
	workers         map[string]*worker[T]
	sleepingWorkers map[string]*worker[T]

	fn                WorkExecuter[T]
	workerConstructor WorkerConstructor[T]
	abnormalReturnCb  func(error)
}

func New[T any](
	fn WorkExecuter[T],
	options ...Option[T],
) *WorkerPool[T] {
	w := &WorkerPool[T]{
		fn:               fn,
		workers:          make(map[string]*worker[T]),
		sleepingWorkers:  make(map[string]*worker[T]),
		abnormalReturnCb: func(err error) {},
	}

	for _, opt := range options {
		opt(w)
	}

	if w.paramCh == nil {
		paramCh := make(chan T)
		w.paramCh = paramCh
		w.workerConstructor = DefaultWorkerConstructor(paramCh, nil, nil)
	}

	return w
}

// SenderChan is getter of sender side of pramCh.
func (p *WorkerPool[T]) SenderChan() chan<- T {
	return p.paramCh
}

// Add adds delta number of workers to p.
// This will create new delta number of goroutines.
func (p *WorkerPool[T]) Add(delta uint32) {
	p.workerMu.Lock()
	defer p.workerMu.Unlock()

	for i := uint32(0); i < delta; i++ {
		worker := p.buildWorker()
		p.wg.Add(1)
		goroutines.Add(1)
		go func() {
			defer goroutines.Add(-1)
			defer p.wg.Done()
			p.runWorker(worker, true, p.abnormalReturnCb)
		}()

		p.workers[worker.id] = worker
	}
}

func (p *WorkerPool[T]) buildWorker() *worker[T] {
	return &worker[T]{
		id: uuid.NewString(),
		Worker: p.workerConstructor(
			p.fn,
			func(T) { atomic.AddInt64(&p.activeWorkerNum, 1) },
			func(T, error) { atomic.AddInt64(&p.activeWorkerNum, -1) },
		),
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

func (p *WorkerPool[T]) runWorker(
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
		delete(p.workers, worker.id)
		delete(p.sleepingWorkers, worker.id)
		p.workerMu.Unlock()

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

// Remove removes delta number of randomly selected workers from p.
// Removed workers could be held as sleeping if they are still working on workFn.
func (p *WorkerPool[T]) Remove(delta uint32) {
	p.workerMu.Lock()
	defer p.workerMu.Unlock()

	var count uint32
	for _, worker := range p.workers {
		if count < delta {
			worker.Cancel()
			delete(p.workers, worker.id)
			p.sleepingWorkers[worker.id] = worker
			count++
		} else {
			break
		}
	}
}

// Len returns number of workers.
// alive is running workers. sleeping is workers removed by Remove and still working on its job.
func (p *WorkerPool[T]) Len() (alive int, sleeping int) {
	p.workerMu.Lock()
	defer p.workerMu.Unlock()
	return len(p.workers), len(p.sleepingWorkers)
}

// ActiveWorkerNum returns number of actively working worker.
func (p *WorkerPool[T]) ActiveWorkerNum() int64 {
	return atomic.LoadInt64(&p.activeWorkerNum)
}

// Kill kills all workers.
func (p *WorkerPool[T]) Kill() {
	p.workerMu.Lock()
	defer p.workerMu.Unlock()

	for _, w := range p.workers {
		w.Kill()
	}
	for _, w := range p.sleepingWorkers {
		w.Kill()
	}
}

// Wait waits for all workers to stop.
// Calling this without Kill and/or Remove all workers may block forever.
func (p *WorkerPool[T]) Wait() {
	p.wg.Wait()
}
