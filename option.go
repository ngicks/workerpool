package workerpool

import (
	"log"
)

// Option is an option that changes WorkerPool instance.
// This can be used in NewWorkerPool.
type Option[T any] func(w *WorkerPool[T])

// SetDefaultAbnormalReturnCb is an Option that
// overrides abnormal-return cb with cb.
//
// cb is called if and only if WorkFn is returned abnormally.
// cb may be called multiple time simultaneously.
func SetAbnormalReturnCb[T any](cb func(err error)) Option[T] {
	if cb == nil {
		panic("cb is nil")
	}
	return func(w *WorkerPool[T]) {
		w.abnormalReturnCb = cb
	}
}

// SetLogOnAbnormalReturn is an Option that,
//
//   - overrides abnormal-return cb.
//   - simply call log.Println with an error.
//
// cb is called if and only if Worker returned abnormally.
// cb may be called multiple times, simultaneously.
func SetLogOnAbnormalReturn[T any]() Option[T] {
	return func(w *WorkerPool[T]) {
		w.abnormalReturnCb = func(err error) { log.Println(err) }
	}
}

// SetWorkerConstructor sets workerConstructor and associated paramCh.
// params for Worker must be sent through this paramCh.
func SetWorkerConstructor[T any](
	paramCh chan T,
	workerConstructor WorkerConstructor[T],
) Option[T] {
	if paramCh == nil {
		panic("paramCh is nil")
	}
	if workerConstructor == nil {
		panic("workerConstructor is nil")
	}
	return func(w *WorkerPool[T]) {
		w.paramCh = paramCh
		w.workerConstructor = workerConstructor
	}
}
