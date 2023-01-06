package workerpool_test

import (
	"context"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/ngicks/workerpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TODO: improve readability of overall tests.

type idParam struct {
	Id int
}

type ErrInt int

func (e ErrInt) Error() string {
	return strconv.FormatInt(int64(e), 10)
}

type workFnArg struct {
	Param            idParam
	ContextCancelled bool
}

type workFn struct {
	args    []workFnArg
	blocked chan struct{}
	stepper chan struct{}

	panicLabel string
}

func (w *workFn) init() {
	w.blocked = make(chan struct{}, 1)
	w.stepper = make(chan struct{})
}

func (w *workFn) step() {
	w.stepper <- struct{}{}
}

func (w *workFn) fn(ctx context.Context, param idParam) error {
	select {
	case w.blocked <- struct{}{}:
	default:
	}

	if w.panicLabel != "" {
		panic(w.panicLabel)
	}
	<-w.stepper

	w.args = append(w.args, workFnArg{
		Param:            param,
		ContextCancelled: ctx.Err() != nil,
	})

	return ErrInt(param.Id)
}

type doneArg struct {
	Param idParam
	Err   error
}

type recorderHook struct {
	sync.Mutex
	receivedArgs []idParam
	doneArgs     []doneArg
}

func (r *recorderHook) onTaskReceived(param idParam) {
	r.Lock()
	defer r.Unlock()

	r.receivedArgs = append(r.receivedArgs, param)
}

func (r *recorderHook) onTaskDone(param idParam, err error) {
	r.Lock()
	defer r.Unlock()

	r.doneArgs = append(r.doneArgs,
		doneArg{
			Param: param,
			Err:   err,
		},
	)
}

func TestWorker(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	w := &workFn{}
	w.init()
	recorder := &recorderHook{}
	paramCh := make(chan idParam)

	worker := workerpool.NewWorker[idParam](
		workerpool.WorkFn[idParam](w.fn),
		paramCh,
		recorder.onTaskReceived,
		recorder.onTaskDone,
	)

	require.False(worker.IsRunning(), "IsRunning is true. want == false. it's just created.")
	require.False(worker.IsEnded(), "IsEnded is true. want == false, it's just created.")

	ctx, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	sw1 := make(chan struct{})
	go func() {
		<-sw1
		worker.Run(ctx)
		close(sw1)
	}()
	sw1 <- struct{}{}
	defer func() {
		<-sw1
	}()

	var err error
	_, err = worker.Run(context.TODO())
	assert.ErrorIs(err, workerpool.ErrAlreadyStarted)

	assert.True(worker.IsRunning())
	assert.False(worker.IsEnded())

	paramCh <- idParam{0}
	<-w.blocked

	assertCallCount := func(workFn, onTaskReceive, onTaskDone int) {
		t.Helper()
		assert.Equal(
			len(w.args),
			workFn,
			"onTaskReceived is called %d times, want %d",
			len(w.args), workFn,
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

	w.step()

	paramCh <- idParam{1}
	<-w.blocked

	assertCallCount(1, 2, 1)

	w.step()

	cancel1()
	<-sw1

	assert.False(worker.IsRunning())
	assert.False(worker.IsEnded())

	assertCallCount(2, 2, 2)

	ctx, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	sw2 := make(chan struct{})
	go func() {
		<-sw2
		worker.Run(ctx)
		close(sw2)
	}()
	sw2 <- struct{}{}
	defer func() {
		<-sw2
	}()

	paramCh <- idParam{2}
	assert.True(worker.IsRunning())
	assert.False(worker.IsEnded())

	<-w.blocked
	w.step()
	cancel2()
	<-sw2

	assert.False(worker.IsRunning())
	assert.False(worker.IsEnded())

	assertCallCount(3, 3, 3)

	if diff := cmp.Diff(w.args, []workFnArg{
		{Param: idParam{0}},
		{Param: idParam{1}},
		{Param: idParam{2}},
	}); diff != "" {
		t.Fatalf("workFn must be called with param sent though paramCh. diff = %s", diff)
	}
	if diff := cmp.Diff(recorder.receivedArgs, []idParam{{0}, {1}, {2}}); diff != "" {
		t.Fatalf("onTaskReceived must be called with param sent though paramCh. diff = %s", diff)
	}
	if diff := cmp.Diff(recorder.doneArgs, []doneArg{
		{Param: idParam{0}, Err: ErrInt(0)},
		{Param: idParam{1}, Err: ErrInt(1)},
		{Param: idParam{2}, Err: ErrInt(2)},
	}); diff != "" {
		t.Fatalf("onTaskDone must be called with param sent though paramCh, and error returned from workFn. diff = %s", diff)
	}
}

func TestWorker_context_passed_to_work_fn_is_cancelled_after_Kill_is_called(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	w := &workFn{}
	w.init()
	recorder := &recorderHook{}
	paramCh := make(chan idParam)

	worker := workerpool.NewWorker[idParam](
		workerpool.WorkFn[idParam](w.fn),
		paramCh,
		recorder.onTaskReceived,
		recorder.onTaskDone,
	)

	var killed bool
	var err error
	sw := make(chan struct{})
	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		<-sw
		killed, err = worker.Run(ctx)
		close(sw)
	}()
	sw <- struct{}{}
	defer func() {
		<-sw
	}()

	paramCh <- idParam{}
	<-w.blocked
	worker.Kill()
	assert.True(worker.IsRunning())
	assert.True(worker.IsEnded())
	w.step()
	<-sw

	assert.False(worker.IsRunning())
	assert.True(worker.IsEnded())

	require.True(killed, "killed must be true")
	require.Nil(err)
	require.True(w.args[0].ContextCancelled)

	_, err = worker.Run(context.TODO())
	require.ErrorIs(err, workerpool.ErrAlreadyEnded)

	// ensure this does not panic.
	worker.Kill()
	worker.Kill()
	worker.Kill()
}

func TestWorker_killed_when_paramCh_is_closed(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	w := &workFn{}
	w.init()
	recorder := &recorderHook{}
	paramCh := make(chan idParam)

	worker := workerpool.NewWorker[idParam](
		workerpool.WorkFn[idParam](w.fn),
		paramCh,
		recorder.onTaskReceived,
		recorder.onTaskDone,
	)

	var killed bool
	var err error
	sw := make(chan struct{})
	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		<-sw
		killed, err = worker.Run(ctx)
		close(sw)
	}()
	sw <- struct{}{}
	defer func() {
		<-sw
	}()
	defer worker.Kill()

	assert.True(worker.IsRunning())
	paramCh <- idParam{}
	w.step()
	close(paramCh)

	<-sw

	require.True(killed)
	require.NoError(err)
}

func TestWorker_killed_when_work_fn_panicking(t *testing.T) {
	assert := assert.New(t)

	w := &workFn{}
	w.init()
	w.panicLabel = "foo"

	recorder := &recorderHook{}
	paramCh := make(chan idParam)

	worker := workerpool.NewWorker[idParam](
		workerpool.WorkFn[idParam](w.fn),
		paramCh,
		recorder.onTaskReceived,
		recorder.onTaskDone,
	)

	var killed bool
	var err error
	var recovered atomic.Pointer[any]
	sw := make(chan struct{})
	go func() {
		defer func() {
			recv := recover()
			recovered.Store(&recv)
		}()
		defer func() {
			close(sw)
		}()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		<-sw
		killed, err = worker.Run(ctx)
	}()
	sw <- struct{}{}
	defer func() {
		<-sw
	}()

	paramCh <- idParam{}

	<-w.blocked

	<-sw
	assert.False(killed)
	assert.True(worker.IsEnded())
	assert.False(worker.IsRunning())
	assert.NoError(err)
	assert.NotNil(*recovered.Load())
	assert.Equal((*recovered.Load()).(string), "foo")
}

func TestWorker_killed_when_work_fn_call_Goexit(t *testing.T) {
	assert := assert.New(t)

	recorder := &recorderHook{}
	paramCh := make(chan idParam)

	worker := workerpool.NewWorker[idParam](
		workerpool.WorkFn[idParam](
			func(context.Context, idParam) error { runtime.Goexit(); return nil },
		),
		paramCh,
		recorder.onTaskReceived,
		recorder.onTaskDone,
	)

	sw := make(chan struct{})
	go func() {
		defer func() {
			close(sw)
		}()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		<-sw
		worker.Run(ctx)
	}()
	sw <- struct{}{}
	defer func() {
		<-sw
	}()

	paramCh <- idParam{}

	<-sw
	assert.True(worker.IsEnded())
	assert.False(worker.IsRunning())
}

func TestWorker_pause(t *testing.T) {
	assert := assert.New(t)

	w := &workFn{}
	w.init()
	recorder := &recorderHook{}
	paramCh := make(chan idParam)

	worker := workerpool.NewWorker[idParam](
		workerpool.WorkFn[idParam](w.fn),
		paramCh,
		recorder.onTaskReceived,
		recorder.onTaskDone,
	)

	ctx, cancel := context.WithCancel(context.Background())
	sw := make(chan struct{})
	go func() {
		<-sw
		worker.Run(ctx)
		close(sw)
	}()
	sw <- struct{}{}
	defer func() {
		<-sw
	}()
	defer cancel()

	cont, err := worker.Pause(context.Background())
	assert.NoError(err)

	dur := time.Millisecond
	select {
	case paramCh <- idParam{0}:
		t.Errorf("paramCh must not receive until continueWorker is called")
	case <-time.After(dur):
	}

	cont()

	timeout := time.Second
	select {
	case paramCh <- idParam{0}:
		w.step()
	case <-time.After(timeout):
		t.Errorf(
			"paramCh must receive after continueWorker is called, but still not receive after %s",
			timeout,
		)
	}

	paramCh <- idParam{1}

	pauseCtx, cancelPause := context.WithCancel(context.Background())
	go func() { cancelPause() }()
	cont, err = worker.Pause(pauseCtx)
	assert.ErrorIs(err, context.Canceled)
	assert.Nil(cont)

	w.step()

	// After Pause returns, cancelling context is no-op.
	pauseCtx, cancelPause = context.WithCancel(context.Background())
	cont, err = worker.Pause(pauseCtx)
	cancelPause()
	assert.NoError(err)
	assert.NotNil(cont)
	cont()

	// after cancelling pause, worker works normally.
	paramCh <- idParam{3}
	<-w.blocked
	w.step()

	paramCh <- idParam{4}
	defer w.step()
	go func() {
		<-time.After(time.Millisecond)
		worker.Kill()
	}()
	cont, err = worker.Pause(context.Background())
	assert.ErrorIs(err, workerpool.ErrKilled)
	assert.Nil(cont)
}
