package workerpool

import (
	"context"
	"sync/atomic"
)

type WorkExecuter[T any] interface {
	Exec(ctx context.Context, param T) error
}

type WorkFn[T any] func(ctx context.Context, param T) error

func (w WorkFn[T]) Exec(ctx context.Context, param T) error {
	return w(ctx, param)
}

var _ WorkExecuter[int] = WorkFn[int](nil)

// Worker represents a single task executor.
// It works on a single task at a time.
// It may be in stopped-state where loop is stopped,
// working-state where working in loop,
// or ended-state where no way is given to step back into working-state again.
type Worker[T any] struct {
	isRunning atomic.Int32
	isEnded   atomic.Int32

	killCh  chan struct{}
	pauseCh chan func()

	fn             WorkExecuter[T]
	paramCh        <-chan T
	onTaskReceived func(param T)
	onTaskDone     func(param T, err error)
	cancelFn       atomic.Pointer[context.CancelFunc]
}

func NewWorker[T any](
	fn WorkExecuter[T],
	paramCh <-chan T,
	onTaskReceived func(param T),
	onTaskDone func(param T, err error),
) Worker[T] {
	if onTaskReceived == nil {
		onTaskReceived = func(T) {}
	}
	if onTaskDone == nil {
		onTaskDone = func(T, error) {}
	}

	return Worker[T]{
		killCh:         make(chan struct{}),
		pauseCh:        make(chan func()),
		fn:             fn,
		paramCh:        paramCh,
		onTaskReceived: onTaskReceived,
		onTaskDone:     onTaskDone,
		cancelFn:       atomic.Pointer[context.CancelFunc]{},
	}
}

// Run starts w's worker loop. It blocks until ctx is cancelled and/or Kill is called and work returns if it is ongoing.
// Or it could be return early if conditions below are met.
// w will be ended if paramCh is closed, Kill is called or workFn returns abnormally.
//
//   - Run returns `ErrAlreadyEnded` if worker is already ended.
//   - Run returns `ErrAlreadyStarted` if worker is already started.
func (w *Worker[T]) Run(ctx context.Context) (killed bool, err error) {
	if w.IsEnded() {
		return false, ErrAlreadyEnded
	}
	if !w.setRunning(true) {
		return false, ErrAlreadyStarted
	}
	defer w.setRunning(false)

	var normalReturn bool
	defer func() {
		if !normalReturn {
			killed = true
			w.setEnded()
			return
		}
		if w.IsEnded() {
			killed = true
			return
		}
	}()

loop:
	for {
		select {
		case <-w.killCh:
			break loop
		case <-ctx.Done():
			break loop
		default:
			select {
			case <-w.killCh:
				break loop
			case <-ctx.Done():
				break loop
			case pauseFn := <-w.pauseCh:
				pauseFn()
			case param, ok := <-w.paramCh:
				if !ok {
					w.setEnded()
					break loop
				}
				func() {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()

					w.cancelFn.Store(&cancel)
					// Prevent it from being held after it is unneeded.
					defer w.cancelFn.Store(nil)

					select {
					// in case of racy kill
					case <-w.killCh:
						return
					default:
					}

					var err error
					w.onTaskReceived(param)
					defer func() { w.onTaskDone(param, err) }()
					err = w.fn.Exec(ctx, param)
				}()
			}
		}
	}
	// If task exits abnormally, called runtime.Goexit or panicking, it would not reach this line.
	normalReturn = true
	// killed will be mutated again in defer func.
	return killed, nil
}

func (w *Worker[T]) Pause(ctx context.Context) (continueWorker func(), err error) {
	called := make(chan struct{})
	continueCh := make(chan struct{})

	wait := make(chan struct{})
	defer func() {
		<-wait
	}()

	goroutines.Add(1)
	go func() {
		pauseFn := func() {
			close(called)
			<-continueCh
		}

		select {
		case w.pauseCh <- pauseFn:
		case <-w.killCh:
		case <-ctx.Done():
		}
		goroutines.Add(-1)
		close(wait)
	}()

	select {
	case <-called:
	case <-w.killCh:
		close(continueCh)
		return nil, ErrKilled
	case <-ctx.Done():
		close(continueCh)
		return nil, ctx.Err()
	}

	return func() {
		close(continueCh)
	}, nil

}

// Kill kills this worker.
// If a task is being worked at the time of invocation,
// a context passed to the workFn will be cancelled immediately.
// Kill makes this worker to step into ended state, making it impossible to Start-ed again.
func (w *Worker[T]) Kill() {
	if w.setEnded() {
		close(w.killCh)
	} else {
		return
	}

	if cancel := w.cancelFn.Load(); cancel != nil {
		(*cancel)()
	}
}

func (w *Worker[T]) IsEnded() bool {
	return w.isEnded.Load() == 1
}

func (w *Worker[T]) setEnded() bool {
	return w.isEnded.CompareAndSwap(0, 1)
}

func (w *Worker[T]) IsRunning() bool {
	return w.isRunning.Load() == 1
}

func (w *Worker[T]) setRunning(to bool) bool {
	if to {
		return w.isRunning.CompareAndSwap(0, 1)
	} else {
		return w.isRunning.CompareAndSwap(1, 0)
	}
}
