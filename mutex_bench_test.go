package concurrent

import (
	"sync"
	"testing"
	"time"
)

func BenchmarkMainSMutexFunc(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m := &sync.Mutex{}
			for i := 0; i < 100; i++ {
				m.Lock()
				m.Unlock()
			}

			m.Lock()
			go func(m *sync.Mutex) {
				m.Unlock()
			}(m)

			m.Lock()
		}
	})
}

func BenchmarkMainCMutexFunc(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m := &Mutex{}
			for i := 0; i < 100; i++ {
				m.Lock()
				m.Unlock()
			}

			m.Lock()
			go func(m *Mutex) {
				m.Unlock()
			}(m)

			m.Lock()
		}
	})
}

func BenchmarkMainCMutexTFunc(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m := &Mutex{}
			for i := 0; i < 100; i++ {
				m.TryLockTimeout(time.Second)
				m.Unlock()
			}

			m.Lock()
			go func(m *Mutex) {
				m.Unlock()
			}(m)

			m.Lock()
		}
	})
}

func BenchmarkMainChFunc(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ch := make(chan int, 1)
			ch <- 1
			for i := 0; i < 100; i++ {
				<-ch
				ch <- 1
			}

			<-ch
			go func(ch chan int) {
				close(ch)
			}(ch)

			<-ch
		}
	})
}
