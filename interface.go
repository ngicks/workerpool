package workerpool

import (
	"context"
	"errors"
)

type WorkerPool[K comparable, T any] interface {
	SetHook(h Hook[K, T])
	Get() (w Worker[K, T], ok bool)
	Put(w Worker[K, T])
}

type Hook[K comparable, T any] struct {
	OnStateChange func(K, WorkingState)
}

var (
	ErrAlreadyRunning  = errors.New("already running")
	ErrInputChanClosed = errors.New("input chan closed")
	ErrNotRunning      = errors.New("not running")
)

type Worker[K comparable, T any] interface {
	Id() K
	Run(ctx context.Context, taskCh <-chan T) (killed bool, err error)
	Pause(ctx context.Context, fn func(ctx context.Context)) (err error)
	Kill()
	State() WorkingState
	WaitUntil(condition func(state WorkingState) bool, actions ...func())
}

// WorkerInitiator is an interface for additional methods.
type WorkerInitiator interface {
	Start() error
	Stop() error
}
