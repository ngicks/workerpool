package workerpool

import (
	"context"
	"testing"
	"time"

	"github.com/ngicks/gommon/pkg/common"
	"github.com/ngicks/gommon/pkg/timing"
	"github.com/stretchr/testify/assert"
)

func TestManager(t *testing.T) {
	assert := assert.New(t)

	// setting up the test target.
	workExec := newWorkFn()
	recorderHook := newRecorderHook()
	pool := New[string, idParam](
		workExec,
		NewUuidPool(),
		SetHook(nil, recorderHook.onTaskReceived, recorderHook.onTaskDone),
	)
	manager := NewManager(
		pool, 31,
		SetMaxWaiting[string, idParam](5),
		SetRemovalBatchSize[string, idParam](7),
		SetRemovalInterval[string, idParam](500*time.Millisecond),
	)

	// mocking out internal timer.
	fakeTimer := common.NewTimerFake()
	manager.timerFactory = func() common.Timer {
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
		return timing.CreateWaiterFn(func() { <-fakeTimer.ResetCh })
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
		waiter := timing.CreateWaiterFn(func() { <-fakeTimer.ResetCh }, func() { <-workExec.called })
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
