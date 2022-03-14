package concurrent

import (
	"container/list"
	"sync"
	"sync/atomic"
)

type Queue struct {
	op
	noLock bool
	iq     list.List
}

func (q *Queue) Push(obj interface{}) {
	if obj == nil {
		return
	}

	if !q.noLock {
		q.acquire()
		defer q.release()
	}

	q.iq.PushBack(obj)
}

func (q *Queue) Pop() interface{} {
	if !q.noLock {
		q.acquire()
		defer q.release()
	}

	if v := q.iq.Front(); v != nil {
		return q.iq.Remove(v)
	}

	return nil
}

func (q *Queue) Reset() *Queue {
	if !q.noLock {
		q.acquire()
		defer q.release()
	}

	q.iq.Init()
	return q
}

func (q *Queue) Len() int {
	if !q.noLock {
		q.acquire()
		defer q.release()
	}

	return q.iq.Len()
}

type BlockingQueue struct {
	Queue
	init  sync.Once
	s     *sync.Cond
	reset chan struct{}
	close int32
}

func (q *BlockingQueue) lazyInit() {
	q.init.Do(func() {
		q.Queue.noLock = true
		q.s = sync.NewCond(&sync.Mutex{})
		if q.reset == nil {
			q.reset = make(chan struct{})
		}
	})
}

func (q *BlockingQueue) Push(obj interface{}) {
	q.lazyInit()
	q.s.L.Lock()
	if atomic.LoadInt32(&q.close) == 1 {
		q.s.L.Unlock()
		return
	}

	q.Queue.Push(obj)
	q.s.Signal()
	q.s.L.Unlock()
}

func (q *BlockingQueue) Pop() interface{} {
	q.lazyInit()
	q.s.L.Lock()
	if atomic.LoadInt32(&q.close) == 1 {
		q.s.L.Unlock()
		return nil
	}

	v := q.Queue.Pop()
	for reset := q.reset; v == nil; v = q.Queue.Pop() {
		q.s.Wait()
		select {
		case <-reset:
			q.s.L.Unlock()
			return nil
		default:
		}
	}

	q.s.L.Unlock()
	return v
}

func (q *BlockingQueue) Reset() *BlockingQueue {
	q.lazyInit()
	q.s.L.Lock()
	if atomic.LoadInt32(&q.close) == 1 {
		q.s.L.Unlock()
		return nil
	}

	q.Queue.Reset()
	if q.reset != nil {
		close(q.reset)
	}

	q.reset = make(chan struct{})
	q.s.Broadcast()
	q.s.L.Unlock()
	return q
}

func (q *BlockingQueue) Len() int {
	return q.Queue.Len()
}

func (q *BlockingQueue) Close() {
	q.lazyInit()
	q.s.L.Lock()
	atomic.StoreInt32(&q.close, 1)
	close(q.reset)
	q.s.Broadcast()
	q.s.L.Unlock()
}
