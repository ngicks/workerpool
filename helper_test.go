package workerpool

import (
	"context"
	"strconv"
	"sync"
)

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
	WorkerId         string
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

func newWorkFn() *workFn {
	w := &workFn{}
	w.init()
	return w
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

func (w *workFn) ExhaustCalledCh() {
	for {
		select {
		case <-w.called:
		default:
			return
		}
	}
}

func (w *workFn) Exec(ctx context.Context, id string, param idParam) error {
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

type hookArg struct {
	Id    string
	Param idParam
	Err   error
}

type onStartArg struct {
	Ctx context.Context
	Id  string
}

type recorderHook struct {
	sync.Mutex
	onReceive    chan struct{}
	onDone       chan struct{}
	receivedArgs []hookArg
	doneArgs     []hookArg

	onStartArg onStartArg
}

func newRecorderHook() *recorderHook {
	r := &recorderHook{}
	r.init()
	return r
}

func (r *recorderHook) init() {
	r.onReceive = make(chan struct{}, 1) // buffering these channel ease race condition
	r.onDone = make(chan struct{}, 1)
}

func (r *recorderHook) onStart(ctx context.Context, id string) {
	r.Lock()
	r.onStartArg = onStartArg{Ctx: ctx, Id: id}
	r.Unlock()
}

func (r *recorderHook) onTaskReceived(id string, param idParam) {
	r.Lock()
	r.receivedArgs = append(r.receivedArgs, hookArg{Id: id, Param: param})
	r.Unlock()

	select {
	case r.onReceive <- struct{}{}:
	default:
	}
}

func (r *recorderHook) onTaskDone(id string, param idParam, err error) {
	r.Lock()
	r.doneArgs = append(r.doneArgs,
		hookArg{
			Id:    id,
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
