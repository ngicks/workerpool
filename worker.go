package workerpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngicks/gommon/pkg/common"
)

var (
	// ErrWorkerFatal means worker is in fatal state.
	// WorkExecuter may returns the error to stop the worker,
	// as it has been in fatal state.
	ErrWorkerFatal = errors.New("worker fatal")
)

type WorkerState int32

const (
	Stopped WorkerState = iota
	Running
	Paused
)

const (
	EndedMask WorkerState = 1 << (16 + iota)
	ActiveMask
)

const (
	formerStatesMask = 0b1111111111111111
	latterStateMask  = 0b111111111111111 << 16
)

func (s WorkerState) name() string {
	state, isEnded := s.State()
	isActive := s.IsActive()
	var suffix string
	if isEnded {
		suffix = "Ended"
	} else if isActive {
		suffix = "Active"
	}

	var name string
	switch state {
	case Stopped:
		name = "stopped"
	case Running:
		name = "running"
	case Paused:
		name = "paused"
	}
	return name + suffix
}

func (s WorkerState) set(state WorkerState) WorkerState {
	return s&latterStateMask | state&^latterStateMask
}

func (s WorkerState) setEnded() WorkerState {
	return s | EndedMask
}

func (s WorkerState) setActive() WorkerState {
	return s | ActiveMask
}
func (s WorkerState) unsetActive() WorkerState {
	return s &^ ActiveMask
}

func (s WorkerState) State() (state WorkerState, isEnded bool) {
	return s &^ latterStateMask, s.IsEnded()
}

func (s WorkerState) IsEnded() bool {
	return s&EndedMask > 0
}
func (s WorkerState) IsRunning() bool {
	return s&Running > 0
}
func (s WorkerState) IsPaused() bool {
	return s&Paused > 0
}
func (s WorkerState) IsActive() bool {
	return s&ActiveMask > 0
}
func (s WorkerState) IsStarted() bool {
	return s&^latterStateMask > 0
}

// swap out this if tests need to.
var timerFactory = func() common.Timer {
	return common.NewTimerReal()
}

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

type workerOption[K comparable, T any] func(w *Worker[K, T])

func SetOnTaskReceived[K comparable, T any](onTaskReceived func(K, T)) workerOption[K, T] {
	return func(w *Worker[K, T]) {
		if onTaskReceived != nil {
			w.onTaskReceived = onTaskReceived
		} else {
			w.onTaskReceived = func(id K, param T) {}
		}
	}
}

func SetOnTaskDone[K comparable, T any](onTaskDone func(K, T, error)) workerOption[K, T] {
	return func(w *Worker[K, T]) {
		if onTaskDone != nil {
			w.onTaskDone = onTaskDone
		} else {
			w.onTaskDone = func(id K, param T, err error) {}
		}
	}
}

func SetOnStart[K comparable, T any](onStart func(context.Context, K)) workerOption[K, T] {
	return func(w *Worker[K, T]) {
		if onStart != nil {
			w.onStart = onStart
		} else {
			w.onStart = nil
		}
	}
}

// Worker is a series of processes, where it pulls tasks from the channel, executes them through WorkExecutor,
// and also manages states.
// Worker may associate itself to the resource, which is pointed by the id, via onStart, onTaskReceived and onTaskDone hooks.
type Worker[K comparable, T any] struct {
	stateCond *sync.Cond
	state     WorkerState

	killCh  chan struct{}
	pauseCh chan func()

	fn             WorkExecuter[K, T]
	taskCh         <-chan T
	onStart        func(ctx context.Context, id K)
	onTaskReceived func(id K, param T)
	onTaskDone     func(id K, param T, err error)
	cancelFn       atomic.Pointer[context.CancelFunc]

	timerFactory func() common.Timer
}

// NewWorker returns a stopped Worker.
func NewWorker[K comparable, T any](
	fn WorkExecuter[K, T],
	taskCh <-chan T,
	options ...workerOption[K, T],
) *Worker[K, T] {
	w := &Worker[K, T]{
		stateCond:      sync.NewCond(&sync.Mutex{}),
		killCh:         make(chan struct{}),
		pauseCh:        make(chan func()),
		fn:             fn,
		taskCh:         taskCh,
		onTaskReceived: func(id K, param T) {},
		onTaskDone:     func(id K, param T, err error) {},
		timerFactory:   timerFactory,
	}

	for _, opt := range options {
		opt(w)
	}

	return w
}

// Run starts w's worker loop. It blocks until ctx is cancelled and/or Kill is called
// and WorkExecutor returns if it has an ongoing task.
// Or it could be return early if conditions below are met.
//
//   - If worker is already ended. (with ErrAlreadyEnded set)
//   - If Run is called simultaneously. (with ErrAlreadyRunning set)
//
// The ended state can be reached if (1) Kill is called,
// (2) WorkExecutor returned abnormally (panic or runtime.Goexit),
// (3) taskCh is closed, or (4) WorkExecutor returns ErrWorkerFatal.
func (w *Worker[K, T]) Run(ctx context.Context, id K) (killed bool, err error) {
	if w.State().IsEnded() {
		return false, ErrAlreadyEnded
	}
	if !w.start() {
		return false, ErrAlreadyRunning
	}
	defer w.stop()

	if w.onStart != nil {
		startCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		w.onStart(startCtx, id)
	}

	var normalReturn bool
	defer func() {
		if !normalReturn {
			// Now, Pause could be listening on killCh. So w must be Kill'ed in an abnormal situation.
			w.Kill()
			return
		}
		if w.state.IsEnded() {
			killed = true
			return
		}
	}()

loop:
	for {
		// zero-ing
		err = nil

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
					w.setActive(true)
					defer w.setActive(false)

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
					w.onTaskReceived(id, param)
					defer func() {
						if !normalReturnInner {
							err = ErrAbnormalReturn
						}
						w.onTaskDone(id, param, err)
					}()
					err = w.fn.Exec(ctx, id, param)
					normalReturnInner = true
				}()
				if errors.Is(err, ErrWorkerFatal) {
					break loop
				}
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
func (w *Worker[K, T]) Pause(
	ctx context.Context,
	timeout time.Duration,
) (continueWorker func() (cancelled bool), err error) {
	if !w.State().IsRunning() {
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

	var called atomic.Bool
	closeContinueCh := func() bool {
		if called.CompareAndSwap(false, true) {
			close(continueCh)
			return true
		} else {
			return false
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
	timer := w.timerFactory()
	timer.Reset(timeout)

	go func() {
		select {
		case <-timer.C():
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
func (w *Worker[K, T]) Kill() {
	if w.setEnded() {
		close(w.killCh)
	} else {
		return
	}

	if cancel := w.cancelFn.Load(); cancel != nil {
		(*cancel)()
	}
}

func (w *Worker[K, T]) setEnded() bool {
	w.stateCond.L.Lock()
	defer w.stateCond.L.Unlock()

	if !w.state.IsEnded() {
		w.state = w.state.setEnded()
		w.stateCond.Broadcast()
		return true
	}
	return false
}

// WaitUntil waits until condition returns true. actions will be called within lock.
// If w reaches ended state and the condition is not waiting for w to end, WaitUntil returns false without calling actions,
// returns true otherwise.
func (w *Worker[K, T]) WaitUntil(condition func(state WorkerState) bool, actions ...func()) (ok bool) {
	w.stateCond.L.Lock()
	defer w.stateCond.L.Unlock()

	for !condition(w.state) {
		if w.state.IsEnded() {
			return false
		}
		w.stateCond.Wait()
	}

	for _, act := range actions {
		act()
	}

	return true
}

func (w *Worker[K, T]) State() WorkerState {
	w.stateCond.L.Lock()
	defer w.stateCond.L.Unlock()

	return w.state
}

func (w *Worker[K, T]) start() bool {
	w.stateCond.L.Lock()
	defer w.stateCond.L.Unlock()

	if !w.state.IsStarted() {
		w.state = w.state.set(Running)
		w.stateCond.Broadcast()
		return true
	}
	return false
}

func (w *Worker[K, T]) stop() bool {
	w.stateCond.L.Lock()
	defer w.stateCond.L.Unlock()

	if w.state.IsStarted() {
		w.state = w.state.set(Stopped)
		w.stateCond.Broadcast()
		return true
	}
	return false
}

func (w *Worker[K, T]) setActive(active bool) {
	w.stateCond.L.Lock()
	if active {
		w.state = w.state.setActive()
	} else {
		w.state = w.state.unsetActive()
	}
	w.stateCond.Broadcast()
	w.stateCond.L.Unlock()
}

func (w *Worker[K, T]) pause() {
	w.stateCond.L.Lock()
	w.state = w.state.set(Paused)
	w.stateCond.Broadcast()
	w.stateCond.L.Unlock()
}

func (w *Worker[K, T]) unpause() {
	w.stateCond.L.Lock()
	w.state = w.state.set(Running)
	w.stateCond.Broadcast()
	w.stateCond.L.Unlock()
}
