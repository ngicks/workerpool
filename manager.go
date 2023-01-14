package workerpool

import (
	"context"
	"time"

	"github.com/ngicks/gommon/pkg/common"
)

var (
	defaultMaxWaiting       = 5
	defaultRemovalBatchSize = 5
	defaultRemovalInterval  = 2 * time.Second
)

type Manager[K comparable, T any] struct {
	pool *Pool[K, T]

	taskCh           chan T
	maxWorker        int
	maxWaiting       int
	removalBatchSize int
	removalInterval  time.Duration

	timerFactory func() common.Timer
}

func NewManager[K comparable, T any](pool *Pool[K, T], maxWorker int, options ...ManagerOption[K, T]) *Manager[K, T] {
	if size := pool.constructor.IdPool.SizeHint(); size > 0 && size < maxWorker {
		maxWorker = size
	}
	m := &Manager[K, T]{
		pool:             pool,
		taskCh:           make(chan T),
		maxWorker:        maxWorker,
		maxWaiting:       defaultMaxWaiting,
		removalBatchSize: defaultRemovalBatchSize,
		removalInterval:  defaultRemovalInterval,
		timerFactory:     timerFactory,
	}

	for _, opt := range options {
		opt(m)
	}

	return m
}

func (m *Manager[K, T]) Run(ctx context.Context) (task T, hadPending bool, err error) {
	timer := m.timerFactory()
	resetTimer := func() {
		timer.Reset(m.removalInterval)
	}
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			var zero T
			return zero, false, nil
		case <-timer.C():
			alive, sleeping, active := m.pool.Len()
			if alive == 0 {
				// does not need to reset timer here...
				// until worker is added.
				// But is it hard to test out?
				continue
			}
			waiting := (alive + sleeping) - active

			if waiting > m.maxWaiting {
				delta := waiting - m.maxWaiting
				if delta > m.removalBatchSize {
					delta = m.removalBatchSize
				}
				m.pool.Remove(delta)
			}
			resetTimer()
		case task, ok := <-m.taskCh:
			if !ok {
				var zero T
				return zero, false, ErrInputChanClosed
			}

			timer.Stop()
			select {
			case m.pool.Sender() <- task:
			default:
				alive, sleeping, _ := m.pool.Len()
				delta := m.maxWorker - (alive + sleeping)
				if delta > 0 {
					if delta > (m.maxWaiting + 1) {
						delta = m.maxWaiting + 1
					}
					m.pool.Add(delta)
				}
				select {
				case m.pool.Sender() <- task:
				case <-ctx.Done():
					return task, true, nil
				}
			}
			resetTimer()
		}
	}
}

func (m *Manager[K, T]) MaxWorker() int {
	return m.maxWorker
}

// Sender is getter of a sender side of the task channel,
// where you can send tasks to workers.
func (m *Manager[K, T]) Sender() chan<- T {
	return m.taskCh
}

func (m *Manager[K, T]) WaitUntil(condition func(alive, sleeping, active int) bool, action ...func()) {
	m.pool.WaitUntil(condition, action...)
}

// Len returns number of workers.
// alive is running workers. sleeping is workers removed by Remove but still working on its task.
func (m *Manager[K, T]) Len() (alive, sleeping, active int) {
	return m.pool.Len()
}

// Kill kills all workers.
func (m *Manager[K, T]) Kill() {
	m.pool.Kill()
}

func (m *Manager[K, T]) Pause(
	ctx context.Context,
	timeout time.Duration,
) (continueWorkers func() (cancelled bool), err error) {
	return m.pool.Pause(ctx, timeout)
}

// Wait waits for all workers to stop.
// Calling this without Kill and/or Remove all workers may block forever.
func (m *Manager[K, T]) Wait() {
	m.pool.Wait()
}
