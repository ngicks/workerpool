package workerpool

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/ngicks/gommon/pkg/timing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TODO: use hacked timer to test time outs.

func setupWorker() (
	worker *Worker[string, idParam],
	taskCh chan idParam,
	workExec *workFn,
	recorder *recorderHook,
	runWorker func(worker *Worker[string, idParam], workerId string) (
		runCtx context.Context,
		cancelRun context.CancelFunc,
		runRetValue func() (killed bool, recovered any, err error),
		closedOnRunReturn chan struct{},
	),
) {
	workExec = newWorkFn()
	recorder = newRecorderHook()

	taskCh = make(chan idParam)

	worker = NewWorker[string, idParam](
		workExec,
		taskCh,
		SetOnStart[string, idParam](recorder.onStart),
		SetOnTaskReceived(recorder.onTaskReceived),
		SetOnTaskDone(recorder.onTaskDone),
	)

	runWorker = func(worker *Worker[string, idParam], workerId string) (
		runCtx context.Context,
		cancelRun context.CancelFunc,
		runRetValue func() (killed bool, recovered any, err error),
		closedOnRunReturn chan struct{},
	) {
		ctx, cancel := context.WithCancel(context.Background())
		sw := make(chan struct{})
		var killed bool
		var err error
		// race detector complains about this. We are forced to use atomic.Pointer here.
		var recovered atomic.Pointer[any]
		go func() {
			defer func() {
				recv := recover()
				recovered.Store(&recv)
			}()
			defer func() {
				close(sw)
			}()
			defer cancel()
			<-sw
			killed, err = worker.Run(ctx, workerId)
		}()
		sw <- struct{}{}

		worker.WaitUntil(func(state WorkerState) bool { return state.IsRunning() })

		return ctx,
			cancel,
			func() (bool, any, error) {
				<-sw
				recovered := recovered.Load()
				var retRecovered any
				if recovered != nil {
					retRecovered = *recovered
				}
				return killed, retRecovered, err
			},
			sw
	}

	return worker, taskCh, workExec, recorder, runWorker
}

func waitUntilRunning[K comparable, T any](worker *Worker[K, T]) {
	worker.WaitUntil(func(state WorkerState) bool { return state.IsRunning() })
}

func TestWorker(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	worker,
		taskCh,
		workExec,
		recorder,
		runWorker := setupWorker()

	require.False(worker.State().IsRunning(), "IsRunning is true. want == false. it's just created.")
	require.False(worker.State().IsEnded(), "IsEnded is true. want == false, it's just created.")

	_,
		cancelRun,
		_,
		closedOnRunReturn := runWorker(worker, "foo")

	cont, _ := worker.Pause(context.Background(), time.Hour)

	recorder.Lock()
	assert.Equal("foo", recorder.onStartArg.Id)
	assert.Nil(recorder.onStartArg.Ctx.Err())
	onStartCtx := recorder.onStartArg.Ctx
	assertOnStartCancelled := func(cancelled bool) {
		if cancelled {
			assert.Error(onStartCtx.Err())
		} else {
			assert.NoError(onStartCtx.Err())
		}
	}
	recorder.Unlock()

	cont()

	var err error
	_, err = worker.Run(context.TODO(), "foo")
	assert.ErrorIs(err, ErrAlreadyRunning)

	assert.True(worker.State().IsRunning())
	assert.False(worker.State().IsEnded())

	waiter := timing.CreateWaiterFn(func() { <-workExec.called })
	taskCh <- idParam{0}
	waiter()

	assertCallCount := func(workFn, onTaskReceive, onTaskDone int) {
		t.Helper()
		assert.Equal(
			len(workExec.args),
			workFn,
			"onTaskReceived is called %d times, want %d",
			len(workExec.args), workFn,
		)
		assert.Equal(
			len(recorder.receivedArgs),
			onTaskReceive,
			"onTaskReceived is called %d times, want %d",
			len(recorder.receivedArgs), onTaskReceive,
		)
		assert.Equal(
			len(recorder.doneArgs),
			onTaskDone,
			"onTaskDone is called %d times, want %d",
			len(recorder.doneArgs), onTaskDone,
		)
	}

	assertCallCount(0, 1, 0)

	workExec.step()

	taskCh <- idParam{1}
	<-workExec.called

	assertCallCount(1, 2, 1)

	workExec.step()

	// observing run returning
	assertOnStartCancelled(false)
	cancelRun()
	<-closedOnRunReturn
	assertOnStartCancelled(true)

	assert.False(worker.State().IsRunning())
	assert.False(worker.State().IsEnded())

	assertCallCount(2, 2, 2)

	// re-run
	_, cancelRun, _, closedOnRunReturn = runWorker(worker, "foo")

	taskCh <- idParam{2}
	assert.True(worker.State().IsRunning())
	assert.False(worker.State().IsEnded())

	<-workExec.called
	workExec.step()
	cancelRun()
	<-closedOnRunReturn

	assert.False(worker.State().IsRunning())
	assert.False(worker.State().IsEnded())

	assertCallCount(3, 3, 3)

	if diff := cmp.Diff(workExec.args, []workFnArg{
		{Param: idParam{0}},
		{Param: idParam{1}},
		{Param: idParam{2}},
	}); diff != "" {
		t.Fatalf("workFn must be called with param sent though paramCh. diff = %s", diff)
	}
	if diff := cmp.Diff(recorder.receivedArgs, []hookArg{
		{"foo", idParam{0}, nil}, {"foo", idParam{1}, nil}, {"foo", idParam{2}, nil},
	}); diff != "" {
		t.Fatalf("onTaskReceived must be called with param sent though paramCh. diff = %s", diff)
	}
	if diff := cmp.Diff(recorder.doneArgs, []hookArg{
		{Id: "foo", Param: idParam{0}, Err: ErrInt(0)},
		{Id: "foo", Param: idParam{1}, Err: ErrInt(1)},
		{Id: "foo", Param: idParam{2}, Err: ErrInt(2)},
	}); diff != "" {
		t.Fatalf("onTaskDone must be called with param sent though paramCh, and error returned from workFn. diff = %s", diff)
	}
}

func TestWorker_context_passed_to_work_fn_is_cancelled_after_Kill_is_called(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	worker,
		taskCh,
		workExec,
		_,
		runWorker := setupWorker()

	_,
		cancelRun,
		runRetValue,
		closedOnRunReturn := runWorker(worker, "foo")
	defer func() {
		<-closedOnRunReturn
	}()
	defer cancelRun()

	taskCh <- idParam{}
	<-workExec.called
	worker.Kill()
	assert.True(worker.State().IsRunning())
	assert.True(worker.State().IsEnded())
	workExec.step()
	<-closedOnRunReturn

	assert.False(worker.State().IsRunning())
	assert.True(worker.State().IsEnded())

	// get return value of worker.Run
	killed, _, err := runRetValue()

	require.True(killed, "killed must be true")
	require.Nil(err)
	require.True(workExec.args[0].ContextCancelled)

	_, err = worker.Run(context.TODO(), "foo")
	require.ErrorIs(err, ErrAlreadyEnded)

	// ensure this does not panic.
	worker.Kill()
	worker.Kill()
	worker.Kill()
}

func TestWorker_killed_when_taskCh_is_closed(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	worker,
		taskCh,
		workExec,
		_,
		runWorker := setupWorker()

	_,
		_,
		runRetValue,
		closedOnRunReturn := runWorker(worker, "foo")

	defer func() {
		<-closedOnRunReturn
	}()
	defer worker.Kill()

	taskCh <- idParam{}
	<-workExec.called
	assert.True(worker.State().IsRunning())
	workExec.step()
	close(taskCh)

	<-closedOnRunReturn

	killed, _, err := runRetValue()
	require.True(killed)
	require.ErrorIs(err, ErrInputChanClosed)
}

func TestWorker_killed_when_work_fn_panics(t *testing.T) {
	assert := assert.New(t)

	worker,
		taskCh,
		workExec,
		recorderHook,
		runWorker := setupWorker()

	_,
		_,
		runRetValue,
		closedOnRunReturn := runWorker(worker, "foo")

	workExec.MustPanicWith("foo")

	taskCh <- idParam{}
	<-workExec.called
	<-closedOnRunReturn

	killed, recovered, err := runRetValue()

	assert.False(killed)
	assert.True(worker.State().IsEnded())
	assert.False(worker.State().IsRunning())
	assert.NoError(err)
	assert.NotNil(recovered)
	assert.Equal(recovered.(string), "foo")
	assert.ErrorIs(recorderHook.doneArgs[0].Err, ErrAbnormalReturn)
}

func TestWorker_killed_when_work_fn_calls_Goexit(t *testing.T) {
	assert := assert.New(t)

	worker,
		taskCh,
		workExec,
		recorderHook,
		runWorker := setupWorker()

	_,
		_,
		_,
		closedOnRunReturn := runWorker(worker, "foo")

	workExec.onCalledHook = func() {
		runtime.Goexit()
	}

	taskCh <- idParam{}

	<-closedOnRunReturn

	assert.True(worker.State().IsEnded())
	assert.False(worker.State().IsRunning())
	assert.ErrorIs(recorderHook.doneArgs[0].Err, ErrAbnormalReturn)
}

func TestWorker_pause(t *testing.T) {
	assert := assert.New(t)

	worker,
		taskCh,
		workExec,
		_,
		runWorker := setupWorker()

	_,
		cancelRun,
		_,
		closedOnRunReturn := runWorker(worker, "foo")
	defer func() {
		<-closedOnRunReturn
	}()
	defer cancelRun()

	assert.True(worker.State().IsRunning(), "IsRunning: just started running")
	assert.False(worker.State().IsPaused(), "IsPaused: just started running")
	assert.False(worker.State().IsEnded(), ".IsEnded: just started running")

	cont, err := worker.Pause(context.Background(), time.Hour)
	assert.NoError(err)
	assert.False(worker.State().IsRunning(), "IsRunning: right after Pause returns")
	assert.True(worker.State().IsPaused(), "IsPaused: right after Pause returns")
	assert.False(worker.State().IsEnded(), "IsEnded: right after Pause returns")

	dur := time.Millisecond
	select {
	case taskCh <- idParam{0}:
		t.Errorf("paramCh must not receive until continueWorker is called")
	case <-time.After(dur):
	}

	cont()

	assert.True(worker.State().IsRunning(), "IsRunning: right after continueWorker returns")
	assert.False(worker.State().IsPaused(), "IsPaused: right after Pause returns")
	assert.False(worker.State().IsEnded(), "IsEnded: right after Pause returns")

	timeout := time.Second
	select {
	case taskCh <- idParam{0}:
		workExec.step()
	case <-time.After(timeout):
		t.Errorf(
			"paramCh must receive after continueWorker is called, but still not receive after %s",
			timeout,
		)
	}

	taskCh <- idParam{1}

	pauseCtx, cancelPause := context.WithCancel(context.Background())
	go func() { cancelPause() }()
	cont, err = worker.Pause(pauseCtx, time.Hour)
	assert.ErrorIs(err, context.Canceled)
	assert.Nil(cont)

	workExec.step()

	// After Pause returns, cancelling context is no-op.
	pauseCtx, cancelPause = context.WithCancel(context.Background())
	cont, err = worker.Pause(pauseCtx, time.Hour)
	cancelPause()
	assert.NoError(err)
	assert.NotNil(cont)
	assert.True(cont(), "continueWorker must return true if it is called first time.")

	// It's safe to call continueWorker multiple. (not concurrently)
	for i := 0; i < 10; i++ {
		assert.False(cont(), "continueWorker must return false if it is second or more call.")
	}

	// after cancelling pause, worker works normally.
	taskCh <- idParam{3}
	<-workExec.called
	workExec.step()

	// pause is released after timeout duration.
	cont, err = worker.Pause(context.Background(), time.Microsecond)
	assert.NoError(err)
	assert.NotNil(cont)

	race := make(chan struct{})
	go func() {
		<-race
		taskCh <- idParam{4}
		<-workExec.called
		workExec.step()
		close(race)
	}()
	race <- struct{}{}

	select {
	case <-race:
	case <-time.After(time.Millisecond):
		t.Errorf("Pause must be released after timeout duration, but did not")
	}

	assert.False(cont(), "After Pause timed-out, continueWorker must return false")

	taskCh <- idParam{5}
	defer workExec.step()

	go func() {
		<-time.After(time.Millisecond)
		worker.Kill()
	}()
	cont, err = worker.Pause(context.Background(), time.Hour)

	assert.ErrorIs(err, ErrKilled)
	assert.Nil(cont)

	cont, err = worker.Pause(context.Background(), time.Hour)
	assert.ErrorIs(err, ErrKilled)
	assert.Nil(cont)
}

func TestWorker_pause_is_released_immediately_after_Kill(t *testing.T) {
	assert := assert.New(t)

	worker,
		_,
		_,
		_,
		runWorker := setupWorker()

	_,
		cancelRun,
		_,
		closedOnRunReturn := runWorker(worker, "foo")
	defer func() {
		<-closedOnRunReturn
	}()
	defer cancelRun()

	cont, err := worker.Pause(context.Background(), time.Hour)
	assert.NotNil(cont)
	assert.NoError(err)

	worker.Kill()

	select {
	case <-time.After(10 * time.Millisecond):
		t.Errorf("Pause is not released, after an invocation of Kill")
	case <-closedOnRunReturn:
	}

	assert.False(cont(), "continueWorker must return false after an invocation of Kill")
}

func TestWorker_cancelling_ctx_after_Pause_returned_is_noop(t *testing.T) {
	assert := assert.New(t)

	worker,
		taskCh,
		workExec,
		_,
		runWorker := setupWorker()

	_,
		cancelRun,
		_,
		closedOnRunReturn := runWorker(worker, "foo")
	defer func() {
		<-closedOnRunReturn
	}()
	defer cancelRun()

	waitUntilRunning(worker)

	pauseCtx, pauseCancel := context.WithCancel(context.Background())
	cont, err := worker.Pause(pauseCtx, 5*time.Millisecond)
	assert.NotNil(cont)
	assert.NoError(err)

	pauseCancel()

	dur := time.Millisecond
	select {
	case taskCh <- idParam{}:
		workExec.step()
		t.Errorf("cancelling ctx after Pause returned must be no-op." +
			" However taskCh is received")
	case <-time.After(dur):
	}

	dur = 10 * time.Millisecond
	select {
	case taskCh <- idParam{}:
		workExec.step()
	case <-time.After(dur):
		t.Errorf("cancelling ctx after Pause returned must be no-op."+
			" But timeout is not working. %s passed", dur)
	}

	assert.False(cont())
}

func TestWorker_ok_without_option(t *testing.T) {
	assert := assert.New(t)

	workExec := newWorkFn()
	taskCh := make(chan idParam)

	worker := NewWorker[string, idParam](
		workExec,
		taskCh,
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		<-done
		worker.Run(ctx, "foo")
		close(done)
	}()
	done <- struct{}{}

	idParamFactory := createIdParamFactory()
	for i := 0; i < 5; i++ {
		taskCh <- idParamFactory()
		workExec.step()
	}

	workExec.ExhaustCalledCh()
	waiter := timing.CreateWaiterFn(func() { <-workExec.called })
	taskCh <- idParamFactory()
	waiter()

	workExec.Lock()
	assert.Len(workExec.args, 5)
	workExec.Unlock()

	workExec.step()

	cancel()
	<-done
}
