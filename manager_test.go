package workerpool

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ngicks/gommon/pkg/common"
	"github.com/ngicks/gommon/pkg/timing"
	"github.com/ngicks/type-param-common/slice"
	"github.com/stretchr/testify/assert"
)

var _ common.ITimer = newFakeTimer()

type fakeTimer struct {
	sync.Mutex
	blockOn chan struct{}
	channel chan time.Time
	resetTo slice.Deque[any] // time.Duration | time.Time | nil
	reset   chan any         // time.Duration | time.Time
	started bool
}

func newFakeTimer() *fakeTimer {
	return &fakeTimer{
		blockOn: make(chan struct{}),
		channel: make(chan time.Time),
		reset:   make(chan any, 1),
	}
}

func (t *fakeTimer) Channel() <-chan time.Time {
	return t.channel
}
func (t *fakeTimer) Reset(d time.Duration) {
	t.Lock()
	defer t.Unlock()
	t.started = true
	t.resetTo.PushBack(d)
	select {
	case t.reset <- d:
	default:
	}
}
func (t *fakeTimer) ResetTo(to time.Time) {
	t.Lock()
	defer t.Unlock()
	t.started = true
	t.resetTo.PushBack(to)
	select {
	case t.reset <- to:
	default:
	}
}
func (t *fakeTimer) Stop() {
	t.Lock()
	defer t.Unlock()
	t.started = false
	t.resetTo.Push(nil)
}
func (t *fakeTimer) Send(tt time.Time) {
	t.channel <- tt
}

func (t *fakeTimer) ExhaustResetCh() {
	for {
		select {
		case <-t.reset:
		default:
			return
		}
	}
}

func TestManager(t *testing.T) {
	assert := assert.New(t)

	// setting up the test target.
	workExec := newWorkFn()
	recorderHook := newRecorderHook()
	pool := New[idParam](
		workExec,
		SetHook(recorderHook.onTaskReceived, recorderHook.onTaskDone),
	)
	manager := NewManager(
		pool, 31,
		SetMaxWaiting[idParam](5),
		SetRemovalBatchSize[idParam](7),
		SetRemovalInterval[idParam](500*time.Millisecond),
	)

	// mocking out internal timer.
	fakeTimer := newFakeTimer()
	manager.timerFactory = func() common.ITimer {
		return fakeTimer
	}

	// building assertions
	assertAliveWorkerNum := func(alive int) bool {
		alive_, _, _ := manager.Len()
		return assert.Equal(alive, alive_)
	}
	assertTotalWorkerNum := func(total int) bool {
		alive, sleeping, _ := manager.Len()
		return assert.Equal(total, alive+sleeping)
	}
	assertWorkerNum := createAssertWorkerNum(t, pool)
	assertActiveWorker := createAssertActiveWorker(t, pool)

	// building synchronization helper
	waitExecReturn := func() (waiter func()) {
		return timing.CreateWaiterFn(func() { <-recorderHook.onDone })
	}
	waitTimerReset := func() (waiter func()) {
		return timing.CreateWaiterFn(func() { <-fakeTimer.reset })
	}
	waitWorkerNum := func(alive, sleeping int) {
		manager.WaitWorker(func(alive_, sleeping_, active_ int) bool {
			return alive == alive_ && sleeping == sleeping_
		})
	}

	// building helper
	idParamFactory := createIdParamFactory()

	// avoid goroutine leak
	defer manager.Wait()
	defer manager.Kill()

	// just naming intent.
	unblockAllWorkExec := func() {
		for i := 0; i < 100; i++ {
			select {
			case workExec.stepper <- struct{}{}:
			default:
				return
			}
		}
	}
	// avoid timeout
	defer unblockAllWorkExec()

	// run manager
	ctx, cancel := context.WithCancel(context.Background())
	var runRetTask idParam
	var runRetHadPending bool
	var runRetErr error
	defer cancel()
	sw := make(chan struct{})
	go func() {
		<-sw
		runRetTask, runRetHadPending, runRetErr = manager.Run(ctx)
		close(sw)
	}()
	sw <- struct{}{}

	assertWorkerNum(0, 0)

	waiter := waitTimerReset()
	manager.Sender() <- idParamFactory()
	waiter()

	if !assertWorkerNum(6, 0) {
		t.Errorf("sent 1 task, worker num is not as expected, want = 1 (active worker) + 5 (max waiting)")
	}

	for i := 0; i < 30; i++ {
		waiter := waitTimerReset()
		manager.Sender() <- idParamFactory()
		waiter()
	}

	if !assertWorkerNum(31, 0) {
		t.Errorf("sent additional 30 tasks, worker num is not as expected, want = 31 (maxWorker)")
	}

	select {
	case manager.Sender() <- idParamFactory():
	case <-time.After(10 * time.Millisecond):
		t.Errorf("manager takes 1 additional task")
	}

	cancel()
	<-sw

	assert.Equal(31, runRetTask.Id)
	assert.True(runRetHadPending)
	assert.NoError(runRetErr)

	for i := 0; i < 31; i++ {
		waiter := waitExecReturn()
		workExec.step()
		waiter()
	}

	// Rerun
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	sw = make(chan struct{})
	go func() {
		<-sw
		runRetTask, runRetHadPending, runRetErr = manager.Run(ctx)
		close(sw)
	}()
	sw <- struct{}{}

	assertActiveWorker(0)

	for i := 0; i < 3; i++ {
		fakeTimer.ExhaustResetCh()
		waiter := waitTimerReset()
		fakeTimer.Send(time.Now())
		waiter()

		waitWorkerNum(31-7*(i+1), 0)
		assertAliveWorkerNum(31 - 7*(i+1))
	}

	// at this point, alive worker should be 10
	assertWorkerNum(10, 0)

	fakeTimer.ExhaustResetCh()
	workExec.ExhaustCalledCh()

	for i := 0; i < 5; i++ {
		waiter := timing.CreateWaiterFn(func() { <-fakeTimer.reset }, func() { <-workExec.called })
		manager.Sender() <- idParamFactory()
		waiter()
	}

	waitWorkerNum(10, 0)
	if !assertAliveWorkerNum(10) {
		t.Errorf("sent additional 5 tasks, worker num is not as expected, want = 10 (active 5 + max Waiting 5)")
	}

	fakeTimer.ExhaustResetCh()
	waiter = waitTimerReset()
	fakeTimer.Send(time.Now())
	waiter()

	waitWorkerNum(10, 0)
	if !assertTotalWorkerNum(10) {
		alive, sleeping, active := manager.Len()
		t.Errorf(
			"max waiting is 5, must not remove worker: active = %d, (alive, sleeping) = (%d, %d)",
			active, alive, sleeping,
		)
	}
	assertActiveWorker(5)

	for i := 0; i < 2; i++ {
		waiter := waitExecReturn()
		workExec.step()
		waiter()
	}

	assertActiveWorker(3)

	waiter = waitTimerReset()
	fakeTimer.Send(time.Now())
	waiter()

	assertAliveWorkerNum(8)

	for i := 0; i < 3; i++ {
		waiter := waitExecReturn()
		workExec.step()
		waiter()
	}

	waiter = waitTimerReset()
	fakeTimer.Send(time.Now())
	waiter()

	waitWorkerNum(5, 0)
	assertWorkerNum(5, 0)

	// finally close sender
	close(manager.Sender())

	<-sw
	assert.Equal(0, runRetTask.Id)
	assert.False(runRetHadPending)
	assert.ErrorIs(runRetErr, ErrInputChanClosed)
}
