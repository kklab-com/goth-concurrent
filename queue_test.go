package concurrent

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	q := &Queue{}
	q.Push(1)
	q.Push(2)
	assert.EqualValues(t, 2, q.Len())
	assert.EqualValues(t, q.Pop(), 1)
	assert.EqualValues(t, q.Pop(), 2)
	assert.EqualValues(t, 0, q.Len())
	q.Push(1)
	q.Push(2)
	assert.EqualValues(t, q.Pop(), 1)
	q.Reset()
	assert.EqualValues(t, 0, q.Len())
	assert.Nil(t, q.Pop())
}

func TestQueue1(t *testing.T) {
	q := &Queue{}
	wgw := sync.WaitGroup{}
	fs := int32(0)
	for i := 0; i < 3; i++ {
		wgw.Add(100)
		go func() {
			atomic.AddInt32(&fs, 1)
			for i := 0; i < 100; i++ {
				q.Push(1)
				wgw.Done()
			}
		}()
	}

	for atomic.LoadInt32(&fs) != 3 {
	}

	wgr := sync.WaitGroup{}
	for i := 0; i < 3; i++ {
		wgr.Add(100)
		go func() {
			for i := 0; i < 100; i++ {
				assert.Equal(t, 1, q.Pop())
				wgr.Done()
			}
		}()
	}

	wgw.Wait()
	wgr.Wait()
	assert.EqualValues(t, 0, q.Len())
}

func TestBlockingQueue(t *testing.T) {
	q := NewUnlimitedBlockingQueue()
	q.Push(1)
	q.Push(2)
	assert.EqualValues(t, 2, q.Len())
	assert.EqualValues(t, 1, q.Pop())
	assert.EqualValues(t, 2, q.Pop())
	assert.EqualValues(t, 0, q.Len())
	ts := time.Now()
	go func() {
		time.Sleep(time.Millisecond)
		q.Push(1)
	}()

	assert.NotNil(t, q.Pop())
	println(time.Now().Sub(ts))
	assert.True(t, time.Now().Sub(ts) >= time.Millisecond)
	assert.EqualValues(t, 0, q.Len())
}

func TestBlockingQueue1000(t *testing.T) {
	bq := NewUnlimitedBlockingQueue()
	for i := 0; i < 1000; i++ {
		bq.Push(i)
	}

	for i := 0; i < 1000; i++ {
		assert.Equal(t, i, bq.Pop())
	}

	assert.Equal(t, 0, bq.Len())
}

func TestBlockingQueueGo1000(t *testing.T) {
	bq := NewUnlimitedBlockingQueue()
	wg := WaitGroup{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			bq.Push(1)
			wg.Done()
		}()
	}

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			bq.Pop()
			wg.Done()
		}()
	}

	wg.Wait()
	assert.Equal(t, 0, bq.Len())
}

func TestBlockingQueueSize(t *testing.T) {
	bq := NewBlockingQueue(10)
	for i := 0; i < 10; i++ {
		bq.Push(1)
	}

	assert.False(t, bq.TryPush(1))
	assert.False(t, bq.PushTimeout(1, time.Microsecond))
	for i := 0; i < 10; i++ {
		assert.Equal(t, 1, bq.TryPop())
	}

	assert.Nil(t, bq.TryPop())
	assert.Nil(t, bq.PopTimeout(time.Microsecond))
	assert.Zero(t, bq.Len())
	assert.Equal(t, 10, bq.Cap())

	for i := 0; i < 100; i++ {
		go bq.Push(1)
	}

	for i := 0; i < 100; i++ {
		bq.Pop()
	}

	assert.Zero(t, bq.Len())
	assert.Equal(t, 10, bq.Cap())
}
