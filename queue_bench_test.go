package concurrent

import (
	"container/list"
	"testing"
)

func BenchmarkMainChanQueue(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ch := make(chan any, 40)
			ch <- 1
			<-ch
		}
	})
}

func BenchmarkMainGothBQueue(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ch := NewBlockingQueue(40)
			ch.Push(1)
			ch.Pop()
		}
	})
}

func BenchmarkMainChanQueue60(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ch := make(chan any, 60)
			ch <- 1
			<-ch
		}
	})
}

func BenchmarkMainGothBQueue60(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ch := NewBlockingQueue(60)
			ch.Push(1)
			ch.Pop()
		}
	})
}

func BenchmarkMainQueue(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// no thread safe
			queue := list.List{}
			for i := 0; i < 10; i++ {
				queue.PushBack(i)
			}

			for i := 0; i < 10; i++ {
				queue.Remove(queue.Front())
			}
		}
	})
}

func BenchmarkMainChannel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ch := make(chan int, 100)
			for i := 0; i < 10; i++ {
				ch <- i
			}

			for i := 0; i < 10; i++ {
				<-ch
			}

			close(ch)
		}
	})
}

func BenchmarkMainGothList(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			list := List{}
			for i := 0; i < 10; i++ {
				list.PushBack(i)
			}

			for i := 0; i < 10; i++ {
				list.Remove(list.Front())
			}
		}
	})
}

func BenchmarkMainGothQueue(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			queue := Queue{}
			for i := 0; i < 10; i++ {
				queue.Push(i)
			}

			for i := 0; i < 10; i++ {
				queue.Pop()
			}
		}
	})
}

func BenchmarkMainGothBlockingQueue(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			queue := NewUnlimitedBlockingQueue()
			for i := 0; i < 10; i++ {
				queue.Push(i)
			}

			for i := 0; i < 10; i++ {
				queue.Pop()
			}
		}
	})
}
