package workerpool

import (
	"sync"

	"github.com/google/uuid"
	"github.com/ngicks/type-param-common/slice"
	syncparam "github.com/ngicks/type-param-common/sync-param"
)

type IdPool[K comparable] interface {
	Get() (x K, ok bool)
	Put(x K)
	SizeHint() int
}

type SyncIdPool[K comparable] struct {
	pool syncparam.Pool[K]
}

func NewSyncIdPool[K comparable](new func() K) *SyncIdPool[K] {
	return &SyncIdPool[K]{
		pool: syncparam.NewPool(new),
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
	return &UuidPool{
		pool: syncparam.NewPool(uuid.NewString),
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

type FixedIdPool[K comparable] struct {
	mu sync.Mutex
	q  slice.Queue[K]
}

// NewFixedIdPool returns a fixed id pool.
// queue will be mutated on each call of Get.
// If you need to avoid this, use NewFixedIdPoolCloned.
func NewFixedIdPool[K comparable](queue []K) *FixedIdPool[K] {
	return &FixedIdPool[K]{
		q: queue,
	}
}

func NewFixedIdPoolCloned[K comparable](queue []K) *FixedIdPool[K] {
	cloned := make([]K, len(queue))
	copy(cloned, queue)
	return NewFixedIdPool(cloned)
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

// LimitedIdPool is a id pool that generates limited number of K.
// K stored back to the pool with Put() might be reused.
type LimitedIdPool[K comparable] struct {
	mu        sync.Mutex
	remaining uint
	max       uint
	pool      syncparam.Pool[K]
}

func NewLimitedIdPool[K comparable](new func() K, size uint) *LimitedIdPool[K] {
	return &LimitedIdPool[K]{
		remaining: size,
		max:       size,
		pool:      syncparam.NewPool(new),
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
