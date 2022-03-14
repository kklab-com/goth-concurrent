package concurrent

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRoutineService(t *testing.T) {
	rs := NewRoutinePool(3, 1)
	assert.Equal(t, 1, rs.Submit(func() interface{} {
		return 1
	}).Get())

	assert.Nil(t, rs.Execute(func() {}).Get())
	time.Sleep(time.Millisecond)
	assert.False(t, rs.IsShutdown())
	rs.ShutdownGracefully()
	assert.Panics(t, rs.ShutdownGracefully)
	assert.Panics(t, rs.ShutdownImmediately)
	assert.True(t, rs.IsShutdown())
}

func TestRoutineServiceSubmitGracefully(t *testing.T) {
	tick := 100
	wg := sync.WaitGroup{}
	for i := 0; i < tick; i++ {
		wg.Add(1)
		go func() {
			rs := NewRoutinePool(3, 1)
			c := int32(0)
			for i := 0; i < 10000; i++ {
				rs.Submit(func() interface{} {
					time.Sleep(time.Microsecond)
					return atomic.AddInt32(&c, 1)
				})
			}

			rs.ShutdownGracefully()
			assert.Equal(t, int64(10000), int64(rs.DoneTasks()))
			assert.Equal(t, int32(10000), c)

			for i := 0; i < 10000; i++ {
				rs.Submit(func() interface{} {
					time.Sleep(time.Microsecond)
					return atomic.AddInt32(&c, 1)
				})
			}

			assert.Equal(t, uint64(10000), rs.DoneTasks())
			assert.Equal(t, int32(10000), c)
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestRoutineServiceSubmitGracefullyF(t *testing.T) {
	rs := NewRoutinePool(100, 1)
	tc := 10000
	c := int32(0)
	cc := int32(0)
	for i := 0; i < tc; i++ {
		ic := i
		rs.Submit(func() interface{} {
			time.Sleep(time.Microsecond)
			atomic.AddInt32(&c, int32(ic))
			return &cc
		}).Then(func(parent Future) interface{} {
			return atomic.AddInt32(parent.Get().(*int32), 1)
		})
	}

	rs.ShutdownGracefully()
	assert.Equal(t, uint64(tc), rs.DoneTasks())
	assert.Equal(t, int32((tc-1)*tc/2), c)
	assert.Equal(t, int32(tc), cc)
	for i := 0; i < tc; i++ {
		rs.Submit(func() interface{} {
			return atomic.AddInt32(&c, 1)
		})
	}

	assert.Equal(t, uint64(tc), rs.DoneTasks())
	assert.Equal(t, int32((tc-1)*tc/2), c)
	assert.Equal(t, int32(tc), cc)
}

func TestRoutineServiceSubmitImmediately(t *testing.T) {
	tick := 100
	wg := sync.WaitGroup{}
	for i := 0; i < tick; i++ {
		wg.Add(1)
		go func() {
			rs := NewRoutinePool(3, 1)
			c := int32(0)
			go func() {
				time.Sleep(time.Microsecond)
				rs.ShutdownImmediately()
			}()

			for i := 0; i < 10000; i++ {
				rs.Submit(func() interface{} {
					time.Sleep(time.Microsecond)
					return atomic.AddInt32(&c, 1)
				})
			}

			assert.Greater(t, int32(10000), c)
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestRoutineServiceSubmitImmediately10000(t *testing.T) {
	tick := 100
	wg := sync.WaitGroup{}
	for i := 0; i < tick; i++ {
		wg.Add(1)
		go func() {
			rs := NewRoutinePool(3, 10000)
			c := int32(0)

			for i := 0; i < 10000; i++ {
				rs.Submit(func() interface{} {
					time.Sleep(time.Microsecond)
					return atomic.AddInt32(&c, 1)
				})
			}

			rs.ShutdownImmediately()
			assert.Greater(t, int32(10000), c)
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestRoutineServiceSubmitImmediatelyUnLimit(t *testing.T) {
	tick := 100
	wg := sync.WaitGroup{}
	for i := 0; i < tick; i++ {
		wg.Add(1)
		go func() {
			rs := NewRoutinePool(3, -1)
			c := int32(0)

			for i := 0; i < 10000; i++ {
				rs.Submit(func() interface{} {
					time.Sleep(time.Microsecond)
					return atomic.AddInt32(&c, 1)
				})
			}

			rs.ShutdownImmediately()
			assert.Greater(t, int32(10000), c)
			wg.Done()
		}()
	}

	wg.Wait()
}
