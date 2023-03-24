package workerpool

import "time"

type ControllerOption[K comparable, T any] func(c *Controller[K, T])

func Set_RemovalBatchSize[K comparable, T any](size int) ControllerOption[K, T] {
	if size <= 0 {
		panic("SetRemovalBatchSize: size must be positive and non-zero")
	}
	return func(c *Controller[K, T]) {
		c.removalBatchSize = size
	}
}

func Set_MaxWaiting[K comparable, T any](maxWaiting int) ControllerOption[K, T] {
	if maxWaiting < 0 {
		panic("SetRemovalInterval: maxWaiting must be positive")
	}

	return func(c *Controller[K, T]) {
		c.maxWaiting = maxWaiting
	}
}

func Set_RemovalInterval[K comparable, T any](interval time.Duration) ControllerOption[K, T] {
	if interval <= 0 {
		panic("SetRemovalInterval: interval must be positive and non-zero")
	}
	return func(c *Controller[K, T]) {
		c.removalInterval = interval
	}
}
