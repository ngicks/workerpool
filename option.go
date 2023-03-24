package workerpool

import (
	"log"
)

// Option is an option that changes Container instance.
// This can be used in New.
type Option[K comparable, T any] func(p *Container[K, T])

func SetTaskChannel[K comparable, T any](taskCh chan T) Option[K, T] {
	if taskCh == nil {
		panic("SetTaskChannel: taskCh must not be nil")
	}
	return func(w *Container[K, T]) {
		w.taskCh = taskCh
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
	return func(p *Container[K, T]) {
		p.onAbnormalReturn = cb
	}
}

// SetLogOnAbnormalReturn is an Option that
// replaces abnormal-return cb with one that simply calls log.Println with the error.
func SetLogOnAbnormalReturn[K comparable, T any]() Option[K, T] {
	return func(p *Container[K, T]) {
		p.onAbnormalReturn = func(err error) { log.Println(err) }
	}
}

// SetShouldRecover is an Option that
// sets whether it should recover on worker panic or not.
func SetShouldRecover[K comparable, T any](shouldRecover bool) Option[K, T] {
	return func(p *Container[K, T]) {
		p.shouldRecover = shouldRecover
	}
}
