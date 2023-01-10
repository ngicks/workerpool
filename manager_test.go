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
	started bool
}

func newFakeTimer() *fakeTimer {
	return &fakeTimer{
		blockOn: make(chan struct{}),
		channel: make(chan time.Time),
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
}
func (t *fakeTimer) ResetTo(to time.Time) {
	t.Lock()
	defer t.Unlock()
	t.started = true
	t.resetTo.PushBack(to)
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

func TestManager(t *testing.T) {
	assert := assert.New(t)

	workExec := &workFn{}
	workExec.init()

	pool := New[idParam](workExec)

	pollWorkerNum := func(alive, sleeping int) bool {
		return timing.PollUntil(func(context.Context) bool {
			alive_, sleeping_ := pool.Len()
			return alive_ == alive && sleeping_ == sleeping_
		}, 50*time.Millisecond, 3*time.Second)
	}

	assertWorkerNum := createAssertWorkerNum(t, pool)
	// assertActiveWorker := createAssertActiveWorker(t, pool)

	manager := NewManager(
		pool, 31,
		SetMaxWaiting[idParam](5),
		SetRemovalBatchSize[idParam](7),
		SetRemovalInterval[idParam](500*time.Millisecond),
	)

	defer manager.Wait()
	defer manager.Kill()

	defer func() {
		for i := 0; i < 100; i++ {
			select {
			case workExec.stepper <- struct{}{}:
			default:
				return
			}
		}
	}()

	fakeTimer := newFakeTimer()
	manager.timerFactory = func() common.ITimer {
		return fakeTimer
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

	manager.Sender() <- idParamFactory()

	pollWorkerNum(6, 0)
	if !assertWorkerNum(6, 0) {
		t.Errorf("sent 1 task, worker num is not as expected, want = 1 (active worker) + 5 (max waiting)")
	}

	for i := 0; i < 30; i++ {
		manager.Sender() <- idParamFactory()
	}

	pollWorkerNum(31, 0)
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

	for i := 0; i < 100; i++ {
		select {
		case workExec.stepper <- struct{}{}:
		default:
			return
		}
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

	fakeTimer.Lock()
	oldResetLen := fakeTimer.resetTo.Len()
	fakeTimer.Unlock()

	pollReset := func() bool {
		return timing.PollUntil(func(context.Context) bool {
			fakeTimer.Lock()
			defer fakeTimer.Unlock()
			cond := fakeTimer.resetTo.Len() > oldResetLen
			if cond {
				oldResetLen = fakeTimer.resetTo.Len()
			}
			return cond
		}, 10*time.Millisecond, time.Second)
	}

	for i := 0; i < 3; i++ {
		fakeTimer.Send(time.Now())
		pollReset()
		assertWorkerNum(31-7*(i+1), 0)
	}

	for i := 0; i < 5; i++ {
		manager.Sender() <- idParamFactory()
	}

	<-time.After(time.Millisecond)

	pollWorkerNum(10, 0)
	if !assertWorkerNum(10, 0) {
		t.Errorf("sent additional 5 tasks, worker num is not as expected, want = 10 (active 5 + max Waiting 5)")
	}

	fakeTimer.Send(time.Now())
	pollReset()

	if !assertWorkerNum(10, 0) {
		t.Errorf("max waiting is 5, must not remove worker")
	}

	workExec.step()
	workExec.step()

	fakeTimer.Send(time.Now())
	pollReset()

	pollWorkerNum(8, 0)
	assertWorkerNum(8, 0)

	workExec.step()
	workExec.step()
	workExec.step()

	fakeTimer.Send(time.Now())
	pollReset()

	pollWorkerNum(5, 0)
	assertWorkerNum(5, 0)

	close(manager.Sender())

	<-sw
	assert.Equal(0, runRetTask.Id)
	assert.False(runRetHadPending)
	assert.ErrorIs(runRetErr, ErrInputChanClosed)
}
