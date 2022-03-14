package concurrent

import (
	"container/list"
	"testing"
)

func BenchmarkMainOQueue(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// no thread safe
			queue := list.List{}
			for i := 0; i < 100; i++ {
				queue.PushBack(i)
			}

			for i := 0; i < 100; i++ {
				queue.Remove(queue.Front())
			}
		}
	})
}

func BenchmarkMainGQueue(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			queue := Queue{}
			for i := 0; i < 100; i++ {
				queue.Push(i)
			}

			for i := 0; i < 100; i++ {
				queue.Pop()
			}
		}
	})
}

func BenchmarkMainGBQueue(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			queue := BlockingQueue{}
			for i := 0; i < 100; i++ {
				queue.Push(i)
			}

			for i := 0; i < 100; i++ {
				queue.Pop()
			}

			queue.Reset()
		}
	})
}

func BenchmarkMainChannel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ch := make(chan int, 100)
			for i := 0; i < 100; i++ {
				ch <- i
			}

			for i := 0; i < 100; i++ {
				<-ch
			}

			close(ch)
		}
	})
}
