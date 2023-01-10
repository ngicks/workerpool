package workerpool

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngicks/gommon/pkg/timing"
	"github.com/ngicks/type-param-common/slice"
	"github.com/stretchr/testify/assert"
)

func createAssertWorkerNum(t *testing.T, pool interface{ Len() (int, int) }) func(alive, sleeping int) bool {
	return func(alive, sleeping int) bool {
		t.Helper()
		alive_, sleeping_ := pool.Len()
		return assert.Equal(
			t,
			alive, alive_,
			"Not equal: number of alive worker is %d, want = %d",
			alive_, alive,
		) &&
			assert.Equal(
				t,
				sleeping, sleeping_,
				"Not equal: number of sleeping worker is %d, want = %d",
				sleeping_, sleeping,
			)
	}
}

func createAssertActiveWorker(t *testing.T, pool interface{ ActiveWorkerNum() int64 }) func(active int) bool {
	return func(active int) bool {
		t.Helper()
		active_ := int(pool.ActiveWorkerNum())
		return assert.Equal(
			t,
			active, active_,
			"Not equal: number of active worker is %d, want = %d",
			active_, active,
		)
	}
}

func TestPool(t *testing.T) {
	idParamFactory := createIdParamFactory()

	w := &workFn{}
	w.init()

	// releasing all blocking stepper.
	// For correct error printing
	defer func() {
		for i := 0; i < 100; i++ {
			select {
			case w.stepper <- struct{}{}:
			default:
				return
			}
		}
	}()

	recorderHook := &recorderHook{}
	recorderHook.init()

	pool := New[idParam](w, SetHook(recorderHook.onTaskReceived, recorderHook.onTaskDone))

	assertWorkerNum := createAssertWorkerNum(t, pool)
	assertActiveWorker := createAssertActiveWorker(t, pool)

	assertWorkerNum(0, 0)
	assertActiveWorker(0)

	select {
	case pool.Sender() <- idParam{}:
		t.Fatalf("task sent: worker is zero.")
	case <-time.After(time.Millisecond):
	}

	// noop
	pool.Add(0)
	pool.Add(-5)

	assertWorkerNum(0, 0)

	pool.Add(5)

	assertWorkerNum(5, 0)

	for i := 0; i < 3; i++ {
		waiter := timing.CreateWaiterFn(func() { <-w.called })
		pool.Sender() <- idParamFactory()
		waiter()
	}

	assertWorkerNum(5, 0)
	assertActiveWorker(3)

	// give some time to workExec to step
	// from sending w.called channel to blocking on w.stepper channel
	time.Sleep(20 * time.Millisecond)

	for i := 0; i < 3; i++ {
		waiter := timing.CreateWaiterFn(func() { <-recorderHook.onDone })
		w.step()
		waiter()
	}

	assertWorkerNum(5, 0)
	assertActiveWorker(0)

	for i := 0; i < 5; i++ {
		waiter := timing.CreateWaiterFn(func() { <-w.called })
		pool.Sender() <- idParamFactory()
		waiter()
	}

	assertWorkerNum(5, 0)
	assertActiveWorker(5)

	select {
	case pool.Sender() <- idParam{}:
		t.Errorf("task sent: sender channel is un-buffered and all workers are busy")
	case <-time.After(time.Millisecond):
	}

	waiter := timing.CreateWaiterFn(func() { <-w.called })

	// You must do tricks like this to ensure runtime switched context to the newly created goroutine.
	switchCh := make(chan struct{})
	go func() {
		<-switchCh
		pool.Sender() <- idParamFactory()
	}()
	switchCh <- struct{}{}

	pool.Add(5)
	waiter()

	assertWorkerNum(10, 0)
	if !assertActiveWorker(6) {
		t.Fatalf("adding worker while sending task is blocking must unblock sender immediately after.")
	}

	for i := 0; i < 3; i++ {
		waiter := timing.CreateWaiterFn(func() { <-recorderHook.onDone })
		w.step()
		waiter()
	}

	assertWorkerNum(10, 0)
	assertActiveWorker(3)

	// noop
	pool.Add(-123109)
	pool.Remove(0)
	pool.Remove(-123)

	assertWorkerNum(10, 0)
	assertActiveWorker(3)

	pool.Remove(10)

	timing.PollUntil(func(context.Context) bool {
		alive, sleeping := pool.Len()
		return alive == 0 && sleeping == 3
	}, 50*time.Millisecond, 3*time.Second)

	if !assertWorkerNum(0, 3) {
		t.Errorf("workers must be held as sleeping state," +
			" where a worker is not pulling new task but is still working on its task")
	}
	assertActiveWorker(3)

	for i := 0; i < 3; i++ {
		waiter := timing.CreateWaiterFn(func() { <-recorderHook.onDone })
		w.step()
		waiter()
	}
	assertWorkerNum(0, 0)
	assertActiveWorker(0)

	pool.Wait()
}

func TestPool_Exec_abnormal_return(t *testing.T) {
	w := &workFn{}
	w.init()

	defer func() {
		for i := 0; i < 100; i++ {
			select {
			case w.stepper <- struct{}{}:
			default:
				return
			}
		}
	}()

	var errorStack slice.Deque[error]
	var errorStackMu sync.Mutex
	pool := New[idParam](
		w,
		SetAbnormalReturnCb[idParam](func(err error) {
			errorStackMu.Lock()
			errorStack.PushBack(err)
			errorStackMu.Unlock()
		}),
	)

	pool.Add(10)

	label := "njgnmopjp0iadjkpwac08jjmw;da;"
	w.MustPanicWith(label)

	switchCh := make(chan struct{})
	go func() {
		<-switchCh
		pool.Sender() <- idParam{}
		select {
		case w.stepper <- struct{}{}:
			t.Errorf("WorkExecutor not panicking")
		case <-time.After(time.Millisecond):
		}
	}()
	switchCh <- struct{}{}

	timing.PollUntil(func(context.Context) bool {
		alive, sleeping := pool.Len()
		return alive == 9 && sleeping == 0
	}, 50*time.Millisecond, 2*time.Second)

	errorStackMu.Lock()
	lastErr, _ := errorStack.PopBack()
	errorStackMu.Unlock()
	if errStr := lastErr.Error(); !strings.Contains(errStr, label) {
		t.Fatalf("err message not containing %s, but actually is %s", label, errStr)
	}

	w.MustPanicWith(nil)
	var called atomic.Bool
	w.onCalledHook = func() {
		called.Store(true)
		runtime.Goexit()
	}

	switchCh = make(chan struct{})
	go func() {
		<-switchCh
		pool.Sender() <- idParam{}
		select {
		case w.stepper <- struct{}{}:
			t.Errorf("WorkExecutor not exiting")
		case <-time.After(time.Millisecond):
		}
	}()
	switchCh <- struct{}{}

	timing.PollUntil(func(context.Context) bool {
		alive, sleeping := pool.Len()
		return alive == 8 && sleeping == 0
	}, 50*time.Millisecond, 2*time.Second)

	if !called.Load() {
		t.Fatalf("incorrect test implementation: onCalledHook is not called")
	}

	errorStackMu.Lock()
	lastErr, _ = errorStack.PopBack()
	errorStackMu.Unlock()
	label = "runtime.Goexit was called"
	if errStr := lastErr.Error(); !strings.Contains(errStr, label) {
		t.Fatalf("err message not containing %s, but actually is %s", label, errStr)
	}

	pool.Kill()
	pool.Wait()
}

type stackWorkExec struct {
	sync.Mutex

	stepper chan struct{} // stepper is received when Exec is called, calling step() or send on stepper will step an Exec call to return.

	stack slice.Stack[func() error]
}

func newStackWorkExec() *stackWorkExec {
	return &stackWorkExec{
		stepper: make(chan struct{}),
	}
}

func (e *stackWorkExec) step() {
	e.stepper <- struct{}{}
}

func (e *stackWorkExec) Exec(ctx context.Context, param idParam) error {
	<-e.stepper

	e.Lock()
	fn, _ := e.stack.Pop()
	e.Unlock()

	if fn != nil {
		return fn()
	}
	return nil
}

func TestPool_Pause(t *testing.T) {
	assert := assert.New(t)

	workExec := newStackWorkExec()

	workExec.stack.Push(func() error { panic("foo") })
	workExec.stack.Push(func() error { runtime.Goexit(); return nil })
	workExec.stack.Push(func() error { return nil })

	pool := New[idParam](workExec)

	pool.Add(10)

	assertWorkerNum := createAssertWorkerNum(t, pool)
	assertActiveWorker := createAssertActiveWorker(t, pool)

	defer pool.Wait()
	defer pool.Kill()

	defer func() {
		for i := 0; i < 100; i++ {
			select {
			case workExec.stepper <- struct{}{}:
			default:
				return
			}
		}
	}()

	for i := 0; i < 10; i++ {
		pool.Sender() <- idParam{}
	}

	var continueWorkers func() bool
	var err error
	pauseReturn := make(chan struct{})
	go func() {
		<-pauseReturn
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		continueWorkers, err = pool.Pause(ctx, time.Hour)
		close(pauseReturn)
	}()
	pauseReturn <- struct{}{}

	select {
	case <-pauseReturn:
		t.Fatalf("Pause must not return at this point. all workers are blocking")
	case <-time.After(time.Millisecond):
	}

	assertWorkerNum(10, 0)
	assertActiveWorker(10)

	for i := 0; i < 10; i++ {
		workExec.step()
	}

	select {
	case <-pauseReturn:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("Pause must return at this point. all workers are unblocked")
	}

	pool.workerMu.Lock()
	for pair := pool.workers.Oldest(); pair != nil; pair = pair.Next() {
		assert.True(pair.Value.IsPaused(), "worker: %+v", pair.Value)
	}
	pool.workerMu.Unlock()

	assert.True(continueWorkers())
	assert.NoError(err)
	assert.False(continueWorkers())

	pool.workerMu.Lock()
	for pair := pool.workers.Oldest(); pair != nil; pair = pair.Next() {
		assert.False(pair.Value.IsPaused())
	}
	pool.workerMu.Unlock()

	assertWorkerNum(8, 0)
	assertActiveWorker(0)

	pool.Remove(8)
	pool.Wait()
}

func TestPool_Pause_timeout(t *testing.T) {
	assert := assert.New(t)

	workExec := newStackWorkExec()
	recorderHook := &recorderHook{}
	recorderHook.init()

	pool := New[idParam](
		workExec,
		SetHook(recorderHook.onTaskReceived, recorderHook.onTaskDone),
	)

	pool.Add(10)

	defer pool.Wait()
	defer pool.Kill()

	defer func() {
		for i := 0; i < 100; i++ {
			select {
			case workExec.stepper <- struct{}{}:
			default:
				return
			}
		}
	}()

	for i := 0; i < 10; i++ {
		pool.Sender() <- idParam{}
	}

	var continueWorkers func() bool
	var err error
	pauseReturn := make(chan struct{})
	go func() {
		<-pauseReturn
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		continueWorkers, err = pool.Pause(ctx, time.Millisecond)
		close(pauseReturn)
	}()
	pauseReturn <- struct{}{}

	select {
	case <-pauseReturn:
		t.Fatalf("Pause must not return at this point. all workers are blocking")
	case <-time.After(3 * time.Millisecond):
	}

	for i := 0; i < 10; i++ {
		waiter := timing.CreateWaiterFn(func() { <-recorderHook.onDone })
		workExec.step()
		waiter()
	}

	select {
	case <-pauseReturn:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("Pause must return at this point. all workers are blocking")
	}

	<-time.After(time.Millisecond)

	assert.False(continueWorkers(), "must timed out")
	assert.NoError(err)
}
func TestPool_Pause_cancelling_context(t *testing.T) {
	assert := assert.New(t)

	workExec := newStackWorkExec()

	pool := New[idParam](workExec)

	pool.Add(10)

	defer pool.Wait()
	defer pool.Kill()

	defer func() {
		for i := 0; i < 100; i++ {
			select {
			case workExec.stepper <- struct{}{}:
			default:
				return
			}
		}
	}()

	for i := 0; i < 10; i++ {
		pool.Sender() <- idParam{}
	}

	var continueWorkers func() bool
	var err error
	pauseReturn := make(chan struct{})
	go func() {
		<-pauseReturn
		ctx, cancel := context.WithCancel(context.Background())
		sw := make(chan struct{})
		go func() {
			<-sw
			<-time.After(time.Millisecond)
			cancel()
		}()
		sw <- struct{}{}
		continueWorkers, err = pool.Pause(ctx, time.Millisecond)
		close(pauseReturn)
	}()
	pauseReturn <- struct{}{}

	select {
	case <-pauseReturn:
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("Pause must return at this point. the context passed to Pause is cancelled")
	}

	assert.ErrorIs(err, context.Canceled)
	assert.Nil(continueWorkers)

	for i := 0; i < 10; i++ {
		workExec.step()
	}

	pool.workerMu.Lock()
	for pair := pool.workers.Oldest(); pair != nil; pair = pair.Next() {
		assert.False(pair.Value.IsPaused())
	}
	pool.workerMu.Unlock()
}
