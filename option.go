package workerpool

import (
	"context"
	"log"
	"time"
)

// Option is an option that changes Pool instance.
// This can be used in New.
type Option[K comparable, T any] func(p *Pool[K, T])

func SetTaskChannel[K comparable, T any](taskCh chan T) Option[K, T] {
	if taskCh == nil {
		panic("SetTaskChannel: taskCh must not be nil")
	}
	return func(w *Pool[K, T]) {
		w.taskCh = taskCh
		w.constructor.TaskCh = taskCh
	}
}

// SetDefaultAbnormalReturnCb is an Option that replaces p's abnormal-return cb with cb.
//
// cb is called if and only if WorkFn is returned abnormally.
// cb may be called multiple times, simultaneously.
func SetAbnormalReturnCb[K comparable, T any](cb func(err error)) Option[K, T] {
	if cb == nil {
		panic("SetAbnormalReturnCb: cb must not be nil")
	}
	return func(p *Pool[K, T]) {
		p.onAbnormalReturn = cb
	}
}

// SetLogOnAbnormalReturn is an Option that
// replaces abnormal-return cb with one that simply calls log.Println with the error.
func SetLogOnAbnormalReturn[K comparable, T any]() Option[K, T] {
	return func(p *Pool[K, T]) {
		p.onAbnormalReturn = func(err error) { log.Println(err) }
	}
}

// SetHook is an Option that sets onWorkerStart, onTaskReceive and onTaskDone hooks.
// Each function can be nil.
//
// onWorkerStart will be called once a worker starts.
// It also can be called with same worker id's multiple times
// if workers are Added after Removed and the id pool reuses id's.
// ctx is cancelled when Run returns.
//
// onTaskReceive and onTaskDone will be called each time the worker receive and done received task.
func SetHook[K comparable, T any](
	onWorkerStart func(ctx context.Context, id K),
	onTaskReceive func(K, T),
	onTaskDone func(K, T, error),
) Option[K, T] {
	return func(p *Pool[K, T]) {
		p.constructor.OnStart = onWorkerStart
		p.constructor.OnReceive = onTaskReceive
		p.constructor.OnDone = onTaskDone
	}
}

// DisableActiveWorkerNumRecord is an Option that disables
// p's default active-worker-record behavior.
// If this option is passed to New, p's Len always reports 0 active worker.
func DisableActiveWorkerNumRecord[K comparable, T any]() Option[K, T] {
	return func(p *Pool[K, T]) {
		p.constructor.recordReceive = nil
		p.constructor.recordDone = nil
	}
}

type ManagerOption[K comparable, T any] func(wp *Manager[K, T])

func SetRemovalBatchSize[K comparable, T any](size int) ManagerOption[K, T] {
	if size <= 0 {
		panic("SetRemovalBatchSize: size must be positive and non-zero")
	}
	return func(wp *Manager[K, T]) {
		wp.removalBatchSize = size
	}
}

func SetMaxWaiting[K comparable, T any](maxWaiting int) ManagerOption[K, T] {
	if maxWaiting < 0 {
		panic("SetRemovalInterval: maxWaiting must be positive")
	}

	return func(wp *Manager[K, T]) {
		wp.maxWaiting = maxWaiting
	}
}

func SetRemovalInterval[K comparable, T any](interval time.Duration) ManagerOption[K, T] {
	if interval <= 0 {
		panic("SetRemovalInterval: interval must be positive and non-zero")
	}
	return func(wp *Manager[K, T]) {
		wp.removalInterval = interval
	}
}
