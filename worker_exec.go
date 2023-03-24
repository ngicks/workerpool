package workerpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var (
	ErrWorkerFatal = errors.New("worker fatal")
)

// WorkExecuter is an executor of tasks.
type WorkExecuter[K comparable, T any] interface {
	// Exec executes a task.
	// ctx is cancelled if and only if the executor is needed to stop immediately.
	// A long running task must respect the ctx.
	Exec(ctx context.Context, id K, param T) error
}

// WorkFn wraps a function so that it can be used as WorkExecutor.
type WorkFn[K comparable, T any] func(ctx context.Context, id K, param T) error

func (w WorkFn[K, T]) Exec(ctx context.Context, id K, param T) error {
	return w(ctx, id, param)
}

var _ WorkExecuter[string, int] = WorkFn[string, int](nil)

var _ Worker[string, string] = (*ExecutorWorker[string, string])(nil)

type ExecutorWorker[K comparable, T any] struct {
	id       K
	executor WorkExecuter[K, T]

	onStateChange func(s WorkingState)

	stateCond *sync.Cond
	isRunning bool
	state     WorkingState

	killCh   chan struct{}
	pauseCh  chan func()
	cancelFn atomic.Pointer[context.CancelFunc]
}

func NewExecutorWorker[K comparable, T any](id K, executor WorkExecuter[K, T]) *ExecutorWorker[K, T] {
	return &ExecutorWorker[K, T]{
		id:       id,
		executor: executor,

		onStateChange: func(WorkingState) {},

		stateCond: sync.NewCond(&sync.Mutex{}),

		killCh:  make(chan struct{}, 1),
		pauseCh: make(chan func()),
	}
}

func (w *ExecutorWorker[K, T]) Id() K {
	return w.id
}

func (w *ExecutorWorker[K, T]) Run(ctx context.Context, taskCh <-chan T) (killed bool, err error) {
	w.stateCond.L.Lock()
	if w.state != Stopped {
		w.stateCond.L.Unlock()
		return false, ErrAlreadyRunning
	}
	w.state = Idle
	select {
	case <-w.killCh:
	default:
	}
	w.stateCond.Broadcast()
	w.stateCond.L.Unlock()

	defer w.withinStateLock(func() { w.state = Stopped })

	var (
		zero, task T
		ok         bool
		pauseFn    func()
	)
	for {
		w.stateCond.L.Lock()
		state := w.state
		w.stateCond.L.Unlock()

		switch state {
		default:
			// prevent any caller from
			go panic("")
		case Idle:
			// Reset
			task = zero
			pauseFn = nil

			select {
			// prevent it from accidentally step forward consecutive
			case <-ctx.Done():
				return false, ctx.Err()
			case <-w.killCh:
				return true, nil
			default:
				select {
				case <-ctx.Done():
					return false, ctx.Err()
				case <-w.killCh:
					return true, nil
				case pauseFn = <-w.pauseCh:
					w.withinStateLock(func() {
						w.state = Paused
					})
				case task, ok = <-taskCh:
					if !ok {
						return true, ErrInputChanClosed
					}
					w.withinStateLock(func() {
						w.state = Active
					})
				}
			}
		case Active:
			func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				w.cancelFn.Store(&cancel)
				defer w.cancelFn.Store(nil)

				select {
				// Prevent race condition.
				case <-w.killCh:
					cancel()
				default:
				}

				err = w.executor.Exec(ctx, w.id, task)
			}()

			if errors.Is(err, ErrWorkerFatal) {
				return true, err
			}

			w.withinStateLock(func() {
				w.state = Idle
			})
		case Paused:
			pauseFn()
			w.withinStateLock(func() {
				w.state = Idle
			})
		}
	}
}

func (w *ExecutorWorker[K, T]) Pause(ctx context.Context, fn func(ctx context.Context)) (err error) {
	w.stateCond.L.Lock()
	if w.state == Stopped {
		w.stateCond.L.Unlock()
		return ErrNotRunning
	}
	w.stateCond.L.Unlock()

	var (
		wg     sync.WaitGroup
		called chan struct{} = make(chan struct{})
	)

	wg.Add(1)
	pauseFn := func() {
		close(called)
		defer wg.Done()
		fn(ctx)
	}
	select {
	case w.pauseCh <- pauseFn:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Observe it is called.
	// Prevent some code optimizations.
	<-called
	wg.Wait()
	return nil
}

func (w *ExecutorWorker[K, T]) Kill() {
	w.stateCond.L.Lock()
	select {
	case w.killCh <- struct{}{}:
	default:
	}
	w.stateCond.L.Unlock()

	if cancel := w.cancelFn.Load(); cancel != nil {
		(*cancel)()
	}
}

func (w *ExecutorWorker[K, T]) State() WorkingState {
	w.stateCond.L.Lock()
	defer w.stateCond.L.Unlock()
	return w.state
}

func (w *ExecutorWorker[K, T]) WaitUntil(condition func(state WorkingState) bool, actions ...func()) {
	w.stateCond.L.Lock()
	defer w.stateCond.L.Unlock()

	for !condition(w.state) {
		w.stateCond.Wait()
	}

	for _, act := range actions {
		act()
	}
}

// withinStateLock executes fn within state lock.
// If the state changes it sends Broadcast() to notify all waiters of cond.
func (w *ExecutorWorker[K, T]) withinStateLock(fn func()) {
	w.stateCond.L.Lock()
	defer w.stateCond.L.Unlock()

	old := w.state
	fn()
	if w.state != old {
		w.onStateChange(w.state)
		w.stateCond.Broadcast()
	}
}
