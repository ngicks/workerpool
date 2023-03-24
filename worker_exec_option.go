package workerpool

type executorWorkerOption[K comparable, T any] func(w *ExecutorWorker[K, T])

func SetOnStateChange[K comparable, T any](onStateChange func(s WorkingState)) executorWorkerOption[K, T] {
	return func(w *ExecutorWorker[K, T]) {
		w.onStateChange = onStateChange
	}
}
