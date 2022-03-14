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
	q := &BlockingQueue{}
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
	ts := time.Now()
	go func() {
		time.Sleep(time.Millisecond)
		q.Push(1)
	}()

	assert.NotNil(t, q.Pop())
	assert.True(t, time.Now().Sub(ts) > time.Millisecond)
	assert.EqualValues(t, 0, q.Len())
}

func TestBlockingQueue1(t *testing.T) {
	gwg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		gwg.Add(1)
		go func() {
			q := &BlockingQueue{}
			q.Push(1)
			ts := time.Now()
			wg := sync.WaitGroup{}
			sc := 0
			for i := 0; i < 100; i++ {
				wg.Add(1)
				go func() {
					ts := time.Now()
					wg.Done()
					q.Pop()
					if time.Now().Sub(ts) < time.Millisecond {
						sc++
					}
				}()
			}

			wg.Wait()
			go func() {
				time.Sleep(time.Millisecond)
				q.Close()
			}()

			q.Pop()
			assert.Equal(t, 1, sc)
			assert.True(t, time.Now().Sub(ts) > time.Millisecond)
			assert.EqualValues(t, 0, q.Len())
			gwg.Done()
		}()
	}

	gwg.Wait()
}
