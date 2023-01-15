package workerpool

import (
	"encoding/binary"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ngicks/gommon/pkg/randstr"
	"github.com/ngicks/type-param-common/set"
	"github.com/ngicks/type-param-common/util"
	"github.com/stretchr/testify/assert"
)

func testIdPool[K comparable](t *testing.T, idPool IdPool[K], name string) {
	assert := assert.New(t)

	if size := idPool.SizeHint(); size > 0 && size < 100 {
		t.Fatalf("id pool size is %d, want = -1,  100 or more", size)
	}

	done := make(chan struct{})
	idChan := make(chan K, 100)
	ids := make([]K, 0)

	go func() {
		for id := range idChan {
			ids = append(ids, id)
		}
		close(done)
	}()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, ok := idPool.Get()
			assert.True(ok, "name = %s, id = %+v", name, id)
			idChan <- id
		}()
	}

	wg.Wait()
	close(idChan)
	<-done

	set := set.New[K]()
	for _, v := range ids {
		set.Add(v)
	}

	assert.Equal(100, set.Len(), "name = %s", name)
	assert.Equal(100, len(ids), "name = %s", name)
}

func testIdPoolRace[K comparable](t *testing.T, idPool IdPool[K], name string) {
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-time.After(time.Duration(rand.Int31n(1000)))
			id, _ := idPool.Get()
			idPool.Put(id)
		}()
	}
	wg.Wait()
}

var randGen *randstr.Generator = randstr.New()

func randStr() string {
	return util.Must(randGen.String())
}

func randNum() uint64 {
	return binary.BigEndian.Uint64(util.Must(randGen.BytesLen(8)))
}

func randSlice(n int) []string {
	ret := make([]string, n)
	for i := 0; i < n; i++ {
		ret[i] = randStr()
	}
	return ret
}

func TestIdPool(t *testing.T) {
	testIdPool[string](t, NewUuidPool(), "uuid")
	testIdPool[string](t, NewSyncIdPool(randStr), "sync pool string")
	testIdPool[uint64](t, NewSyncIdPool(randNum), "sync pool uint64")
	testIdPool[string](t, NewQueueIdPool(randSlice(100)), "queue id pool string")
	testIdPool[string](t, NewQueueIdPoolCloned(randSlice(100)), "queue id pool string")
	testIdPool[string](t, NewStackIdPool(randSlice(100)), "stack id pool string")
	testIdPool[string](t, NewStackIdPoolCloned(randSlice(100)), "stack id pool string")
	testIdPool[string](t, NewLimitedIdPool(randStr, 100), "limited id pool string")

	testIdPoolRace[string](t, NewUuidPool(), "uuid")
	testIdPoolRace[string](t, NewSyncIdPool(randStr), "sync pool string")
	testIdPoolRace[uint64](t, NewSyncIdPool(randNum), "sync pool uint64")
	testIdPoolRace[string](t, NewQueueIdPool(randSlice(100)), "queue id pool string")
	testIdPoolRace[string](t, NewQueueIdPoolCloned(randSlice(100)), "queue id pool string")
	testIdPoolRace[string](t, NewStackIdPool(randSlice(100)), "stack id pool string")
	testIdPoolRace[string](t, NewStackIdPoolCloned(randSlice(100)), "stack id pool string")
	testIdPoolRace[string](t, NewLimitedIdPool(randStr, 100), "limited id pool string")
}

func TestIdPoolLen(t *testing.T) {
	assert := assert.New(t)

	for _, p := range []IdPool[string]{
		NewUuidPool(),
		NewSyncIdPool(randStr),
	} {
		for i := 0; i < 1000; i++ {
			assert.Equal(-1, p.SizeHint())
			v, _ := p.Get()
			if rand.Int31n(30) == 0 {
				p.Put(v)
			}
		}
	}

	for _, limited := range []IdPool[string]{
		NewQueueIdPool(randSlice(100)),
		NewQueueIdPoolCloned(randSlice(100)),
		NewStackIdPool(randSlice(100)),
		NewStackIdPoolCloned(randSlice(100)),
		NewLimitedIdPool(randStr, 100),
	} {
		ids := make([]string, 50)
		for i := 0; i < 50; i++ {
			assert.Equal(100-i, limited.SizeHint())
			v, _ := limited.Get()
			ids[i] = v
		}
		for i := 0; i < 10; i++ {
			limited.Put(ids[i])
			assert.Equal(50+i+1, limited.SizeHint())
		}
	}
}

func TestIdPoolFixed(t *testing.T) {
	assert := assert.New(t)

	for _, tc := range []struct {
		name            string
		constructor     func(slice []string) IdPool[string]
		fifo            bool
		shouldPropagate bool
	}{
		{"NewQueueIdPool", func(q []string) IdPool[string] { return NewQueueIdPool(q) }, true, true},
		{"NewQueueIdPoolCloned", func(q []string) IdPool[string] { return NewQueueIdPoolCloned(q) }, true, false},
		{"NewStackIdPool", func(q []string) IdPool[string] { return NewStackIdPool(q) }, false, true},
		{"NewStackIdPoolCloned", func(q []string) IdPool[string] { return NewStackIdPoolCloned(q) }, false, false},
	} {
		inputSlice := randSlice(100)
		p := tc.constructor(inputSlice)

		for i := 0; i < 100; i++ {
			var inputValue, msg string
			if tc.fifo {
				inputValue = inputSlice[i]
				msg = "must be FIFO"
			} else {
				inputValue = inputSlice[len(inputSlice)-1-i]
				msg = "must be LIFO"
			}
			v, _ := p.Get()
			if !assert.Equal(inputValue, v, msg) {
				break
			}

			var assertion func(expected interface{}, actual interface{}, msgAndArgs ...interface{}) bool
			if tc.shouldPropagate {
				assertion = assert.Equal
				msg = "%s: input slice is not mutated"
			} else {
				assertion = assert.NotEqual
				msg = "%s: input slice is mutated"
			}

			var sliceValue string
			if tc.fifo {
				sliceValue = inputSlice[i]
			} else {
				sliceValue = inputSlice[len(inputSlice)-1-i]
			}

			if !assertion("", sliceValue, msg, tc.name) {
				break
			}
		}

		for i := 0; i < 5; i++ {
			_, ok := p.Get()
			assert.False(ok)
		}

		p.Put("1")
		p.Put("2")
		p.Put("3")

		for i := 0; i < 3; i++ {
			v, _ := p.Get()
			var expected int
			if tc.fifo {
				expected = i + 1
			} else {
				expected = 3 - i
			}
			if !assert.Equal(strconv.FormatInt(int64(expected), 10), v) {
				break
			}
		}
	}
}

func TestIdPoolLimited(t *testing.T) {
	assert := assert.New(t)

	var count int
	increment := func() int {
		c := count
		count++
		return c
	}
	p := NewLimitedIdPool(increment, 5)

	for i := 0; i < 5; i++ {
		v, _ := p.Get()
		assert.Equal(i, v)
	}

	for i := 0; i < 5; i++ {
		v, ok := p.Get()
		assert.Equal(0, v)
		assert.False(ok)
	}

	p.Put(1)
	p.Put(4)
	var ok bool
	// values can be other than 1 or 4; with race build flag, pool drops some value.
	_, ok = p.Get()
	assert.True(ok)
	_, ok = p.Get()
	assert.True(ok)
	_, ok = p.Get()
	assert.False(ok)
}
