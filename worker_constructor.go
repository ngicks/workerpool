package workerpool

import "context"

// WorkerConstructor is aliased type of constructor
// (technically not an alias, since it does not allow type params with it.)
// Both or either of onTaskReceived, onTaskDone can be nil.
type WorkerConstructor[T any] func(onTaskReceived func(param T), onTaskDone func(param T, err error)) Worker[T]

// DefaultWorkerConstructor is the default function for WorkerConstructor.
// workFn and paramCh must not be nil. Both or either of onTaskReceived, onTaskDone can be nil.
func DefaultWorkerConstructor[T any](
	workFn func(ctx context.Context, param T) error,
	paramCh <-chan T,
	onTaskReceived func(param T),
	onTaskDone func(param T, err error),
) WorkerConstructor[T] {
	return func(onTaskReceived_ func(param T), onTaskDone_ func(param T, err error)) Worker[T] {
		// Hope they will be optimized by the wise compiler.
		combinedOnTaskReceived := func(param T) {
			if onTaskReceived != nil {
				onTaskReceived(param)
			}
			if onTaskReceived_ != nil {
				onTaskReceived_(param)
			}
		}
		combinedOnTaskDone := func(param T, err error) {
			if onTaskDone != nil {
				onTaskDone(param, err)
			}
			if onTaskDone_ != nil {
				onTaskDone_(param, err)
			}
		}
		return NewWorker[T](WorkFn[T](workFn), paramCh, combinedOnTaskReceived, combinedOnTaskDone)
	}
}
