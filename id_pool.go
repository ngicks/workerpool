package workerpool

import (
	"sync"

	"github.com/google/uuid"
	"github.com/ngicks/generic/slice"
	"github.com/ngicks/genericsync"
)

// IdPool is a collection of ids.
// Implementations may decide to simply use sync.Pool,
// or to use slices, strictly limiting contents of the pool.
type IdPool[K comparable] interface {
	// Get returns an id when ok is true.
	// Ids might be reused if id is stored back to the pool with Put.
	Get() (x K, ok bool)
	// Put adds x to the pool.
	// Putting x allows it to be reused with Get. It is however the implementation decision.
	Put(x K)
	// SizeHint returns number of contents remaining in the pool.
	// It returns negative value if content length is unknown.
	SizeHint() int
}

// SyncIdPool is a wrapper of sync.Pool that implements the IdPool.
type SyncIdPool[K comparable] struct {
	pool genericsync.Pool[K]
}

func NewSyncIdPool[K comparable](new func() K) *SyncIdPool[K] {
	p := genericsync.Pool[K]{}
	p.SetNew(new)
	return &SyncIdPool[K]{
		pool: p,
	}
}

func (p *SyncIdPool[K]) Get() (x K, ok bool) {
	return p.pool.Get(), true
}

func (p *SyncIdPool[K]) Put(x K) {
	p.pool.Put(x)
}

func (p *SyncIdPool[K]) SizeHint() int {
	return -1
}

// UuidPool is an IdPool that generates uuid v4 ids.
type UuidPool SyncIdPool[string]

func NewUuidPool() *UuidPool {
	p := genericsync.Pool[string]{}
	p.SetNew(uuid.NewString)
	return &UuidPool{
		pool: p,
	}
}

func (p *UuidPool) Get() (x string, ok bool) {
	return p.pool.Get(), true
}

func (p *UuidPool) Put(x string) {
	p.pool.Put(x)
}

func (p *UuidPool) SizeHint() int {
	return -1
}

type QueueLike[T any] interface {
	Push(v T)
	Pop() (v T, popped bool)
	Len() int
}

// FixedIdPool is an id pool that returns ordered, pre-generated ids.
type FixedIdPool[K comparable] struct {
	mu sync.Mutex
	q  QueueLike[K]
}

// NewQueueIdPool returns a queue-backed fixed id pool, which consumes and adds ids FIFO order.
// queue will be mutated on each call of Get.
// If you need to avoid this, use NewQueueIdPoolCloned.
func NewQueueIdPool[K comparable](queue []K) *FixedIdPool[K] {
	q := slice.Queue[K](queue)
	return &FixedIdPool[K]{
		q: &q,
	}
}

func NewQueueIdPoolCloned[K comparable](queue []K) *FixedIdPool[K] {
	cloned := make([]K, len(queue))
	copy(cloned, queue)
	return NewQueueIdPool(cloned)
}

// NewStackIdPool returns a stack-backed fixed id pool, which consumes and adds ids LIFO order.
// stack will be mutated on each call of Get.
// If you need to avoid this, use NewStackIdPoolCloned.
func NewStackIdPool[K comparable](stack []K) *FixedIdPool[K] {
	q := slice.Stack[K](stack)
	return &FixedIdPool[K]{
		q: &q,
	}
}

func NewStackIdPoolCloned[K comparable](queue []K) *FixedIdPool[K] {
	cloned := make([]K, len(queue))
	copy(cloned, queue)
	return NewStackIdPool(cloned)
}

func (p *FixedIdPool[K]) Get() (x K, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.q.Pop()
}
func (p *FixedIdPool[K]) Put(x K) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.q.Push(x)
}
func (p *FixedIdPool[K]) SizeHint() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.q.Len()
}

// LimitedIdPool is an id pool that generates limited number of K.
// K stored back to the pool with Put() might be reused.
type LimitedIdPool[K comparable] struct {
	mu        sync.Mutex
	remaining uint
	max       uint
	pool      genericsync.Pool[K]
}

func NewLimitedIdPool[K comparable](new func() K, size uint) *LimitedIdPool[K] {
	p := genericsync.Pool[K]{}
	p.SetNew(new)
	return &LimitedIdPool[K]{
		remaining: size,
		max:       size,
		pool:      p,
	}
}

func (p *LimitedIdPool[K]) Get() (x K, ok bool) {
	p.mu.Lock()
	if p.remaining == 0 {
		p.mu.Unlock()
		return x, false
	}
	p.remaining--
	p.mu.Unlock()

	return p.pool.Get(), true
}
func (p *LimitedIdPool[K]) Put(x K) {
	p.mu.Lock()
	if p.remaining < p.max {
		p.remaining++
	}
	p.mu.Unlock()

	p.pool.Put(x)
}
func (p *LimitedIdPool[K]) SizeHint() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return int(p.remaining)
}
