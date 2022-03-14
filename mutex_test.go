package concurrent

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMutex(t *testing.T) {
	for i := 0; i < 10; i++ {
		m := Mutex{}

		// try lock
		assert.True(t, m.TryLock())
		m.Unlock()

		// try lock twice
		assert.True(t, m.TryLock())
		assert.False(t, m.TryLock())
		m.Unlock()

		// try lock twice with timeout, timeout should bigger than time.Millisecond
		ts := time.Now()
		assert.True(t, m.TryLock())
		assert.False(t, m.TryLockTimeout(time.Millisecond))
		assert.True(t, time.Now().Sub(ts) > time.Millisecond)
		m.Unlock()

		// try unLock twice
		m.Lock()
		assert.NotPanics(t, func() {
			m.Unlock()
		})

		assert.Panics(t, func() {
			m.Unlock()
		})

		// try lock with timeout not bigger than time.Millisecond
		ts = time.Now()
		assert.True(t, m.TryLockTimeout(time.Second))
		assert.True(t, time.Now().Sub(ts) < time.Millisecond)

		// try lock with timeout bigger than time.Millisecond
		ts = time.Now()
		assert.False(t, m.TryLockTimeout(time.Microsecond))
		assert.True(t, time.Now().Sub(ts) > time.Microsecond)
		m.Unlock()

		// try unlocked
		ts = time.Now()
		assert.True(t, m.TryLockTimeout(time.Microsecond))
		go func(m *Mutex) {
			time.Sleep(2 * time.Millisecond)
			m.Unlock()
		}(&m)

		<-m.Unlocked()
		assert.True(t, time.Now().Sub(ts) > 2*time.Millisecond)

		// try panic 100 times
		wg := &sync.WaitGroup{}
		state := int32(0)
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer func() {
					if e := recover(); e != nil {
						atomic.AddInt32(&state, 1)
					}

					wg.Done()
				}()
				m.Unlock()
			}()
		}

		wg.Wait()
		assert.Equal(t, int32(100), state)

		// try goroutine lock 100 times and success only one time.
		ts = time.Now()
		state = 0
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				if m.TryLock() {
					atomic.AddInt32(&state, 1)
				}

				wg.Done()
			}()
		}

		wg.Wait()
		assert.Equal(t, int32(1), state)
		assert.True(t, time.Now().Sub(ts) < time.Millisecond)
		m.Unlock()

		// try goroutine lock 100 times.
		ts = time.Now()
		state = 0
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				if m.TryLockTimeout(time.Millisecond) {
					atomic.AddInt32(&state, 1)
					<-time.After(time.Microsecond)
					m.Unlock()
				}

				wg.Done()
			}()
		}

		wg.Wait()
		assert.True(t, state > 10)
		assert.True(t, time.Now().Sub(ts) < time.Second)
	}
}
