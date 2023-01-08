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

func pollUntil(predicate func() bool, interval time.Duration, timeout time.Duration) (ok bool) {
	doneCh := make(chan struct{})

	done := func() {
		select {
		case <-doneCh:
			return
		default:
			close(doneCh)
		}
	}

	go func() {
		for {
			select {
			case <-doneCh:
				return
			default:
			}
			if predicate() {
				break
			}
			time.Sleep(interval)
		}
		done()
	}()

	t := timerFactory()
	t.Reset(timeout)
	defer t.Stop()
	select {
	case <-doneCh:
		return true
	case <-t.Channel():
		done()
		return false
	}
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

func TestWorkerPool(t *testing.T) {
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
		waiter := createWaiter(func() { <-w.called })
		pool.Sender() <- idParamFactory()
		waiter()
	}

	assertWorkerNum(5, 0)
	assertActiveWorker(3)

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

	// You must do tricks like this to ensure runtime switched context to the a newly created goroutine.
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

	pool.Remove(10)
	pollOk := pollUntil(
		func() bool {
			alive, sleeping := pool.Len()
			return alive == 0 && sleeping == 3
		},
		time.Millisecond, 3*time.Second,
	)
	if !pollOk {
		t.Errorf("timed-out: polling that calling Remove removes all idle workers and" +
			" remains still-actively-working workers as sleeping")
	}

	if !assertWorkerNum(0, 3) {
		t.Fatalf("workers must be held as sleeping state," +
			" where workers are not pulling new task but is still working on an each task")
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

func TestWorkerPool_Exec_abnormal_return(t *testing.T) {
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

	pollOk := pollUntil(func() bool {
		alive, sleeping := pool.Len()
		return alive == 9 && sleeping == 0
	}, time.Millisecond, 3*time.Second)
	if !pollOk {
		t.Fatalf("timed-out: polling that Worker removed after WorkExecutor panics")
	}

	errorStackMu.Lock()
	lastErr := errorStack[errorStack.Len()-1]
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

	pollOk = pollUntil(func() bool {
		alive, sleeping := pool.Len()
		return alive == 8 && sleeping == 0
	}, time.Millisecond, 3*time.Second)
	if !pollOk {
		t.Errorf("timed-out: polling that Worker removed after WorkExecutor exits")
	}

	if !called.Load() {
		t.Fatalf("incorrect test implementation: onCalledHook is not called")
	}

	errorStackMu.Lock()
	lastErr = errorStack[errorStack.Len()-1]
	errorStackMu.Unlock()
	if errStr := lastErr.Error(); !strings.Contains(errStr, "runtime.Goexit was called") {
		t.Fatalf("err message not containing %s, but actually is %s", label, errStr)
	}
}
