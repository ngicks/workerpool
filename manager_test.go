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

	workExec := &workFn{}
	workExec.init()

	recorderHook := &recorderHook{}
	recorderHook.init()

	waitExecReturn := func() (waiter func()) {
		return timing.CreateWaiterFn(func() { <-recorderHook.onDone })
	}

	pool := New[idParam](
		workExec,
		SetHook(recorderHook.onTaskReceived, recorderHook.onTaskDone),
	)

	// pollWorkerNum := func(alive, sleeping int) bool {
	// 	return timing.PollUntil(func(context.Context) bool {
	// 		alive_, sleeping_ := pool.Len()
	// 		return alive_ == alive && sleeping_ == sleeping
	// 	}, 5*time.Millisecond, time.Second)
	// }

	manager := NewManager(
		pool, 31,
		SetMaxWaiting[idParam](5),
		SetRemovalBatchSize[idParam](7),
		SetRemovalInterval[idParam](500*time.Millisecond),
	)

	assertAliveWorkerNum := func(alive int) bool {
		alive_, _ := manager.Len()
		return assert.Equal(alive, alive_)
	}
	assertTotalWorkerNum := func(total int) bool {
		alive, sleeping := manager.Len()
		return assert.Equal(total, alive+sleeping)
	}
	assertWorkerNum := createAssertWorkerNum(t, pool)
	assertActiveWorker := createAssertActiveWorker(t, pool)

	defer manager.Wait()
	defer manager.Kill()

	unblockAllWorkExec := func() {
		for i := 0; i < 100; i++ {
			select {
			case workExec.stepper <- struct{}{}:
			default:
				return
			}
		}
	}

	defer unblockAllWorkExec()

	fakeTimer := newFakeTimer()
	manager.timerFactory = func() common.ITimer {
		return fakeTimer
	}

	waitTimerReset := func() (waiter func()) {
		return timing.CreateWaiterFn(func() { <-fakeTimer.reset })
	}

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

	idParamFactory := createIdParamFactory()

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
		assertAliveWorkerNum(31 - 7*(i+1))
	}

	// at this point, alive worker should be 10

	fakeTimer.ExhaustResetCh()

exhaustLoop:
	for {
		select {
		case <-workExec.called:
		default:
			break exhaustLoop
		}
	}

	for i := 0; i < 5; i++ {
		waiter := waitTimerReset()
		waitReceive := timing.CreateWaiterFn(func() { <-workExec.called })
		manager.Sender() <- idParamFactory()
		waitReceive()
		waiter()
	}

	if !assertAliveWorkerNum(10) {
		t.Errorf("sent additional 5 tasks, worker num is not as expected, want = 10 (active 5 + max Waiting 5)")
	}

	fakeTimer.ExhaustResetCh()
	waiter = waitTimerReset()
	fakeTimer.Send(time.Now())
	waiter()

	if !assertTotalWorkerNum(10) {
		active := manager.ActiveWorkerNum()
		alive, sleeping := manager.Len()
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

	pool.WaitWorker(func(alive, sleeping int) bool {
		return alive == 5 && sleeping == 0
	})
	assertWorkerNum(5, 0)

	// finally close sender
	close(manager.Sender())

	<-sw
	assert.Equal(0, runRetTask.Id)
	assert.False(runRetHadPending)
	assert.ErrorIs(runRetErr, ErrInputChanClosed)

	unblockAllWorkExec()
}
