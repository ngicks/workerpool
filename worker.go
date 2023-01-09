package workerpool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngicks/gommon/pkg/common"
)

type RunningState int32

const (
	Stopped RunningState = iota
	Running
	Paused
)

const (
	Ended = 100 + iota
	KilledStillRunning
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
	stateMu   sync.Mutex
	isRunning RunningState // 0 = stopped, 1 = running, 2 = paused
	isEnded   int32

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
	if !w.start() {
		return false, ErrAlreadyRunning
	}
	defer w.stop()

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
					err = ErrInputChanClosed
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

					normalReturnInner := false
					var err error
					w.onTaskReceived(param)
					defer func() {
						if !normalReturnInner {
							err = ErrAbnormalReturn
						}
						w.onTaskDone(param, err)
					}()
					err = w.fn.Exec(ctx, param)
					normalReturnInner = true
				}()
			}
		}
	}
	// If task exits abnormally, called runtime.Goexit or panicking, it would not reach this line.
	normalReturn = true
	// killed will be mutated again in defer func.
	return killed, err
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
	if !w.IsRunning() {
		return nil, ErrNotRunning
	}
	pauseFnIsCalled := make(chan struct{})
	continueCh := make(chan struct{})
	ensurePauseFnReturn := make(chan struct{})

	wait := make(chan struct{})
	defer func() {
		<-wait
	}()

	go func() {
		pauseFn := func() {
			w.pause()
			close(pauseFnIsCalled)
			<-continueCh
			w.unpause()
			close(ensurePauseFnReturn)
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

	return func() (cancelled bool) {
		cancelled = closeContinueCh()
		<-ensurePauseFnReturn
		return cancelled
	}, nil
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
	w.stateMu.Lock()
	defer w.stateMu.Unlock()
	return w.isEnded == 1
}

func (w *Worker[T]) setEnded() bool {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()

	if w.isEnded == 0 {
		w.isEnded = 1
		return true
	}
	return false
}

func (w *Worker[T]) IsRunning() bool {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()

	return w.isRunning == Running
}

func (w *Worker[T]) IsPaused() bool {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()

	return w.isRunning == Paused
}

func (w *Worker[T]) State() RunningState {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()

	isRunning := w.isRunning
	isEnded := w.isEnded

	if isEnded == 0 {
		return RunningState(isRunning)
	} else if isRunning != Stopped {
		return KilledStillRunning
	}
	return Ended
}

func (w *Worker[T]) start() bool {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()

	if w.isRunning == Stopped {
		w.isRunning = Running
		return true
	}
	return false
}

func (w *Worker[T]) stop() bool {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()

	if w.isRunning == Running {
		w.isRunning = Stopped
		return true
	}
	return false
}

func (w *Worker[T]) pause() {
	w.stateMu.Lock()
	w.isRunning = Paused
	w.stateMu.Unlock()
}

func (w *Worker[T]) unpause() {
	w.stateMu.Lock()
	w.isRunning = Running
	w.stateMu.Unlock()
}
