package workerpool

import (
	"log"
	"time"
)

// Option is an option that changes Pool instance.
// This can be used in New.
type Option[T any] func(p *Pool[T])

func SetTaskChannel[T any](taskCh chan T) Option[T] {
	if taskCh == nil {
		panic("ch is nil")
	}
	return func(w *Pool[T]) {
		w.taskCh = taskCh
		w.constructor.TaskCh = taskCh
	}
}

// SetDefaultAbnormalReturnCb is an Option that replaces p's abnormal-return cb with cb.
//
// cb is called if and only if WorkFn is returned abnormally.
// cb may be called multiple times, simultaneously.
func SetAbnormalReturnCb[T any](cb func(err error)) Option[T] {
	if cb == nil {
		panic("cb is nil")
	}
	return func(p *Pool[T]) {
		p.onAbnormalReturn = cb
	}
}

// SetLogOnAbnormalReturn is an Option that
// replaces abnormal-return cb which simply calls log.Println with an error.
func SetLogOnAbnormalReturn[T any]() Option[T] {
	return func(p *Pool[T]) {
		p.onAbnormalReturn = func(err error) { log.Println(err) }
	}
}

// SetHook is an Option that sets onTaskReceive and onTaskDone hooks.
func SetHook[T any](onTaskReceive func(T), onTaskDone func(T, error)) Option[T] {
	return func(p *Pool[T]) {
		p.constructor.OnReceive = onTaskReceive
		p.constructor.OnDone = onTaskDone
	}
}

// DisableActiveWorkerNumRecord is an Option that disables
// p's default active-worker-record behavior.
// If this option is passed to New, p's ActiveWorkerNum always returns 0.
func DisableActiveWorkerNumRecord[T any]() Option[T] {
	return func(p *Pool[T]) {
		p.constructor.recordReceive = nil
		p.constructor.recordDone = nil
	}
}

type ManagerOption[T any] func(wp *Manager[T])

func SetRemovalBatchSize[T any](size int) ManagerOption[T] {
	return func(wp *Manager[T]) {
		wp.removalBatchSize = size
	}
}

func SetMaxWaiting[T any](maxWaiting int) ManagerOption[T] {
	return func(wp *Manager[T]) {
		wp.maxWaiting = maxWaiting
	}
}

func SetRemovalInterval[T any](interval time.Duration) ManagerOption[T] {
	return func(wp *Manager[T]) {
		wp.removalInterval = interval
	}
}
