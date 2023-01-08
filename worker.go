package workerpool

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/ngicks/gommon/pkg/common"
)

// swap out this if tests need to.
var timerFactory = func() common.ITimer {
	return common.NewTimerImpl()
}

// WorkExecuter is an executor of tasks.
type WorkExecuter[T any] interface {
	// Exec executes a task.
	// ctx is cancelled if and only if the executor is needed to stop immediately.
	// A long running task must respect the ctx.
	Exec(ctx context.Context, param T) error
}

// WorkFn wraps a function so that it can be used as WorkExecutor.
type WorkFn[T any] func(ctx context.Context, param T) error

func (w WorkFn[T]) Exec(ctx context.Context, param T) error {
	return w(ctx, param)
}

var _ WorkExecuter[int] = WorkFn[int](nil)

// Worker represents a single task executor,
// It works on a single task, which is sent though task channel, at a time.
// It may be in stopped-state where loop is stopped,
// running-state where it is working in loop,
// or ended-state where no way is given to step back into working-state again.
type Worker[T any] struct {
	isRunning atomic.Int32
	isEnded   atomic.Int32

	killCh  chan struct{}
	pauseCh chan func()

	fn             WorkExecuter[T]
	taskCh         <-chan T
	onTaskReceived func(param T)
	onTaskDone     func(param T, err error)
	cancelFn       atomic.Pointer[context.CancelFunc]
}

// NewWorker returns initialized Worker.
// Both or either of onTaskReceived and onTaskDone can be nil.
func NewWorker[T any](
	fn WorkExecuter[T],
	taskCh <-chan T,
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
		taskCh:         taskCh,
		onTaskReceived: onTaskReceived,
		onTaskDone:     onTaskDone,
	}
}

// Run starts w's worker loop. It blocks until ctx is cancelled and/or Kill is called
// and WorkExecutor returns if it has an ongoing task.
// Or it could be return early if conditions below are met.
//
//   - if worker is already ended. (with ErrAlreadyEnded set)
//   - if Run is called simultaneously. (with ErrAlreadyRunning set)
//
// The ended state can be reached if (1) Kill is called,
// (2) WorkExecutor returned abnormally (panic or runtime.Goexit),
// or (3) taskCh is closed.
func (w *Worker[T]) Run(ctx context.Context) (killed bool, err error) {
	if w.IsEnded() {
		return false, ErrAlreadyEnded
	}
	if !w.setRunning(true) {
		return false, ErrAlreadyRunning
	}
	defer w.setRunning(false)

	var normalReturn bool
	defer func() {
		if !normalReturn {
			// Now, Pause could be listening on killCh. So w must be Kill'ed in an abnormal situation.
			w.Kill()
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
		case pauseFn := <-w.pauseCh:
			pauseFn()
		default:
			select {
			case <-w.killCh:
				break loop
			case <-ctx.Done():
				break loop
			case pauseFn := <-w.pauseCh:
				pauseFn()
			case param, ok := <-w.taskCh:
				if !ok {
					w.Kill()
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

// Pause pauses w until returned continueWorker is called or timeout duration is passed.
// Pause may take long time to return since it would wait for WorkExecutor if it had a task.
// Cancel ctx if you need to time-out w to pause.
//
// Internally timer is created and set with timeout when w is paused.
// Thus timeout (nearly) equals to the maximum duration of the pause.
func (w *Worker[T]) Pause(
	ctx context.Context,
	timeout time.Duration,
) (continueWorker func() (cancelled bool), err error) {
	pauseFnIsCalled := make(chan struct{})
	continueCh := make(chan struct{})

	wait := make(chan struct{})
	defer func() {
		<-wait
	}()

	go func() {
		pauseFn := func() {
			close(pauseFnIsCalled)
			<-continueCh
		}

		select {
		case w.pauseCh <- pauseFn:
		case <-w.killCh:
		case <-ctx.Done():
		}
		close(wait)
	}()

	closeContinueCh := func() bool {
		select {
		case <-continueCh:
			return false
		default:
			close(continueCh)
			return true
		}
	}

	select {
	case <-pauseFnIsCalled:
	case <-w.killCh:
		closeContinueCh()
		return nil, ErrKilled
	case <-ctx.Done():
		closeContinueCh()
		return nil, ctx.Err()
	}

	// time.After is not eligible for this use-case,
	// since the timer returned from After will not be GC'ed until it goes off.
	timer := timerFactory()
	timer.Reset(timeout)

	go func() {
		select {
		case <-timer.Channel():
		case <-continueCh:
		case <-w.killCh:
		}
		timer.Stop()
		closeContinueCh()
	}()

	return closeContinueCh, nil
}

// Kill kills this worker.
// If a task is being worked at the time of invocation,
// the context passed to the WorkExecutor will be cancelled immediately.
// Kill makes this worker to step into ended state, making it impossible to started again.
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
