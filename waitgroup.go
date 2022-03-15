package concurrent

import (
	"sync"
	"sync/atomic"
)

type WaitGroup struct {
	once  sync.Once
	rl    sync.Locker
	s     *sync.Cond
	delta int64
}

func (w *WaitGroup) lazyInit() {
	w.once.Do(func() {
		l := sync.RWMutex{}
		w.rl = l.RLocker()
		w.s = sync.NewCond(&l)
	})
}

func (w *WaitGroup) Add(delta int) {
	w.lazyInit()
	w.s.L.Lock()
	if atomic.LoadInt64(&w.delta)+int64(delta) < 0 {
		w.s.L.Unlock()
		return
	}

	if atomic.AddInt64(&w.delta, int64(delta)) == 0 {
		w.s.Broadcast()
	}

	w.s.L.Unlock()
}

func (w *WaitGroup) Done() {
	w.Add(-1)
}

func (w *WaitGroup) Wait() {
	w.lazyInit()
	w.s.L.Lock()
	for atomic.LoadInt64(&w.delta) != 0 {
		w.s.Wait()
	}

	w.s.L.Unlock()
}

func (w *WaitGroup) Remain() int {
	w.lazyInit()
	w.rl.Lock()
	defer w.rl.Unlock()
	return int(w.delta)
}

func (w *WaitGroup) Reset() {
	w.Add(int(w.delta * -1))
	if w.delta > 0 {
		for v := atomic.LoadInt64(&w.delta); v > 0; v = atomic.LoadInt64(&w.delta) {
			w.Add(int(v * -1))
		}
	}
}
