package workerpool

import (
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngicks/type-param-common/slice"
	"github.com/stretchr/testify/assert"
)

// return a waiter, where caller can be blocked until passed fn's return.
func createWaiter(fn ...func()) (waiter func()) {
	var wg sync.WaitGroup

	sw := make(chan struct{})
	for _, f := range fn {
		wg.Add(1)
		go func(fn func()) {
			<-sw
			defer wg.Done()
			fn()
		}(f)
	}

	for i := 0; i < len(fn); i++ {
		sw <- struct{}{}
	}

	return wg.Wait
}

func createRepeatedWaiter(fn func(), repeat int) (waiter func()) {
	repeated := make([]func(), repeat)
	for i := 0; i < repeat; i++ {
		repeated[i] = fn
	}

	return createWaiter(repeated...)
}

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
	pool.workerEndCh = make(chan string, 100)

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
		waiter := createWaiter(func() { <-w.called })
		pool.Sender() <- idParamFactory()
		waiter()
	}

	assertWorkerNum(5, 0)
	assertActiveWorker(3)

	// give some time to workExec to step
	// from sending w.called channel to blocking on w.stepper channel
	time.Sleep(20 * time.Millisecond)

	for i := 0; i < 3; i++ {
		waiter := createWaiter(func() { <-recorderHook.onDone })
		w.step()
		waiter()
	}

	assertWorkerNum(5, 0)
	assertActiveWorker(0)

	for i := 0; i < 5; i++ {
		waiter := createWaiter(func() { <-w.called })
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

	waiter := createWaiter(func() { <-w.called })

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
		waiter := createWaiter(func() { <-recorderHook.onDone })
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

	waiter = createRepeatedWaiter(
		func() {
			select {
			case <-pool.workerEndCh:
			case <-time.After(500 * time.Millisecond):
				t.Logf("Remove: timed out reporting on pool.workerEndCh")
			}
		},
		7,
	)
	pool.Remove(10)
	waiter()

	if !assertWorkerNum(0, 3) {
		t.Errorf("workers must be held as sleeping state," +
			" where a worker is not pulling new task but is still working on its task")
	}
	assertActiveWorker(3)

	for i := 0; i < 3; i++ {
		waiter := createWaiter(func() { <-recorderHook.onDone })
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
	pool.workerEndCh = make(chan string)

	pool.Add(10)

	label := "njgnmopjp0iadjkpwac08jjmw;da;"
	w.MustPanicWith(label)

	waiter := createWaiter(func() { <-pool.workerEndCh })

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

	waiter()

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

	waiter = createWaiter(func() { <-pool.workerEndCh })

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

	waiter()

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
}
