package workerpool

import (
	"context"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/ngicks/gommon/pkg/timing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TODO: use hacked timer to test time outs.

type idParam struct {
	Id int
}

func createIdParamFactory() func() idParam {
	count := 0
	return func() idParam {
		p := idParam{count}
		count++
		return p
	}
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
	sync.Mutex

	args    []workFnArg
	called  chan struct{} // called is received when Exec is called but sender does not block on it.
	stepper chan struct{} // stepper is received when Exec is called, calling step() or send on stepper will step an Exec call to return.

	panicLabel   any
	onCalledHook func()
}

func (w *workFn) MustPanicWith(panicLabel any) {
	if panicLabel == nil {
		w.panicLabel = nil
	} else {
		w.panicLabel = panicLabel
	}
}

func (w *workFn) init() {
	w.called = make(chan struct{}, 1)
	w.stepper = make(chan struct{}) // stepper must block
}

// step unblocks Exec.
func (w *workFn) step() {
	w.stepper <- struct{}{}
}

func (w *workFn) Exec(ctx context.Context, param idParam) error {
	select {
	case w.called <- struct{}{}:
	default:
	}

	if w.panicLabel != nil {
		panic(w.panicLabel)
	}
	if w.onCalledHook != nil {
		w.onCalledHook()
	}
	// log.Println("blocking on stepper")
	<-w.stepper
	// log.Println("received from stepper")

	w.Lock()
	w.args = append(w.args, workFnArg{
		Param:            param,
		ContextCancelled: ctx.Err() != nil,
	})
	w.Unlock()

	return ErrInt(param.Id)
}

type doneArg struct {
	Param idParam
	Err   error
}

type recorderHook struct {
	sync.Mutex
	onReceive    chan struct{}
	onDone       chan struct{}
	receivedArgs []idParam
	doneArgs     []doneArg
}

func (r *recorderHook) init() {
	r.onReceive = make(chan struct{}, 1) // buffering these channel ease race condition
	r.onDone = make(chan struct{}, 1)
}

func (r *recorderHook) onTaskReceived(param idParam) {
	r.Lock()
	r.receivedArgs = append(r.receivedArgs, param)
	r.Unlock()

	select {
	case r.onReceive <- struct{}{}:
	default:
	}
}

func (r *recorderHook) onTaskDone(param idParam, err error) {
	r.Lock()
	r.doneArgs = append(r.doneArgs,
		doneArg{
			Param: param,
			Err:   err,
		},
	)
	defer r.Unlock()

	select {
	case r.onDone <- struct{}{}:
	default:
	}
}

func initWorker() (
	worker *Worker[idParam],
	taskCh chan idParam,
	workExec *workFn,
	recorder *recorderHook,
	runWorker func(run func(ctx context.Context) (killed bool, err error)) (
		runCtx context.Context,
		cancelRun context.CancelFunc,
		runRetValue func() (killed bool, recovered any, err error),
		closedOnRunReturn chan struct{},
	),
) {
	workExec = &workFn{}
	workExec.init()
	recorder = &recorderHook{}
	recorder.init()

	taskCh = make(chan idParam)

	workerConcrete := NewWorker[idParam](
		workExec,
		taskCh,
		recorder.onTaskReceived,
		recorder.onTaskDone,
	)
	worker = &workerConcrete

	runWorker = func(run func(ctx context.Context) (killed bool, err error)) (
		runCtx context.Context,
		cancelRun context.CancelFunc,
		runRetValue func() (killed bool, recovered any, err error),
		closedOnRunReturn chan struct{},
	) {
		ctx, cancel := context.WithCancel(context.Background())
		sw := make(chan struct{})
		var killed bool
		var err error
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
			killed, err = run(ctx)
		}()
		sw <- struct{}{}

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

func waitUntilRunning(worker interface{ IsRunning() bool }) (ok bool) {
	return timing.PollUntil(
		func(ctx context.Context) bool { return worker.IsRunning() },
		time.Millisecond, 50*time.Millisecond,
	)
}

func TestWorker(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	worker,
		taskCh,
		workExec,
		recorder,
		runWorker := initWorker()

	require.False(worker.IsRunning(), "IsRunning is true. want == false. it's just created.")
	require.False(worker.IsEnded(), "IsEnded is true. want == false, it's just created.")

	_,
		cancelRun,
		_,
		closedOnRunReturn := runWorker(worker.Run)

	// reducing race condition
	waitUntilRunning(worker)

	var err error
	_, err = worker.Run(context.TODO())
	assert.ErrorIs(err, ErrAlreadyRunning)

	assert.True(worker.IsRunning())
	assert.False(worker.IsEnded())

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
	cancelRun()
	<-closedOnRunReturn

	assert.False(worker.IsRunning())
	assert.False(worker.IsEnded())

	assertCallCount(2, 2, 2)

	// re-run
	_, cancelRun, _, closedOnRunReturn = runWorker(worker.Run)

	taskCh <- idParam{2}
	assert.True(worker.IsRunning())
	assert.False(worker.IsEnded())

	<-workExec.called
	workExec.step()
	cancelRun()
	<-closedOnRunReturn

	assert.False(worker.IsRunning())
	assert.False(worker.IsEnded())

	assertCallCount(3, 3, 3)

	if diff := cmp.Diff(workExec.args, []workFnArg{
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

	worker,
		taskCh,
		workExec,
		_,
		runWorker := initWorker()

	_,
		cancelRun,
		runRetValue,
		closedOnRunReturn := runWorker(worker.Run)
	defer func() {
		<-closedOnRunReturn
	}()
	defer cancelRun()

	taskCh <- idParam{}
	<-workExec.called
	worker.Kill()
	assert.True(worker.IsRunning())
	assert.True(worker.IsEnded())
	workExec.step()
	<-closedOnRunReturn

	assert.False(worker.IsRunning())
	assert.True(worker.IsEnded())

	// get return value of worker.Run
	killed, _, err := runRetValue()

	require.True(killed, "killed must be true")
	require.Nil(err)
	require.True(workExec.args[0].ContextCancelled)

	_, err = worker.Run(context.TODO())
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
		runWorker := initWorker()

	_,
		_,
		runRetValue,
		closedOnRunReturn := runWorker(worker.Run)

	defer func() {
		<-closedOnRunReturn
	}()
	defer worker.Kill()

	taskCh <- idParam{}
	<-workExec.called
	assert.True(worker.IsRunning())
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
		runWorker := initWorker()

	_,
		_,
		runRetValue,
		closedOnRunReturn := runWorker(worker.Run)

	workExec.MustPanicWith("foo")

	taskCh <- idParam{}
	<-workExec.called
	<-closedOnRunReturn

	killed, recovered, err := runRetValue()

	assert.False(killed)
	assert.True(worker.IsEnded())
	assert.False(worker.IsRunning())
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
		runWorker := initWorker()

	_,
		_,
		_,
		closedOnRunReturn := runWorker(worker.Run)

	workExec.onCalledHook = func() {
		runtime.Goexit()
	}

	taskCh <- idParam{}

	<-closedOnRunReturn

	assert.True(worker.IsEnded())
	assert.False(worker.IsRunning())
	assert.ErrorIs(recorderHook.doneArgs[0].Err, ErrAbnormalReturn)
}

func TestWorker_pause(t *testing.T) {
	assert := assert.New(t)

	worker,
		taskCh,
		workExec,
		_,
		runWorker := initWorker()

	_,
		cancelRun,
		_,
		closedOnRunReturn := runWorker(worker.Run)
	defer func() {
		<-closedOnRunReturn
	}()
	defer cancelRun()

	// runWorker switches context to the dedicated goroutine that runs worker.Run.
	// We could not avoid this race condition, but adding small wait on this should suffice.
	waitUntilRunning(worker)

	assert.True(worker.IsRunning(), "IsRunning: just started running")
	assert.False(worker.IsPaused(), "IsPaused: just started running")
	assert.False(worker.IsEnded(), "IsEnded: just started running")

	cont, err := worker.Pause(context.Background(), time.Hour)
	assert.NoError(err)
	assert.False(worker.IsRunning(), "IsRunning: right after Pause returns")
	assert.True(worker.IsPaused(), "IsPaused: right after Pause returns")
	assert.False(worker.IsEnded(), "IsEnded: right after Pause returns")

	dur := time.Millisecond
	select {
	case taskCh <- idParam{0}:
		t.Errorf("paramCh must not receive until continueWorker is called")
	case <-time.After(dur):
	}

	cont()

	assert.True(worker.IsRunning(), "IsRunning: right after continueWorker returns")
	assert.False(worker.IsPaused(), "IsPaused: right after Pause returns")
	assert.False(worker.IsEnded(), "IsEnded: right after Pause returns")

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
		runWorker := initWorker()

	_,
		cancelRun,
		_,
		closedOnRunReturn := runWorker(worker.Run)
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
		runWorker := initWorker()

	_,
		cancelRun,
		_,
		closedOnRunReturn := runWorker(worker.Run)
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
