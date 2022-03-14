package concurrent

import (
	"fmt"
	"runtime"
	"sync/atomic"
)

const (
	gorWait = iota
	gorRun
	gorDone
	gorPendingShutdown
	gorShutdown
)

var ErrShutdownTwice = fmt.Errorf("shutdown a shutdown routine service")

type RoutineService interface {
	Submit(func() interface{}) Future
	Execute(func()) Future
	MaxRoutines() int
	StandbyRoutines() int
	MaxTasks() int
	Tasks() int
	DoneTasks() uint64
	IsShutdown() bool
	ShutdownGracefully()
	ShutdownImmediately()
}

type routineWorker struct {
	routine  *routine
	shutdown Future
	state    int
	n        int
}

func (r *routineWorker) run() {
	go func(r *routineWorker) {
		for !r.shutdown.IsDone() {
			if v := r.routine.tq.Pop(); v == nil {
				select {
				case <-r.shutdown.Done():
					break
				}
			} else {
				rt := v.(*routineTask)
				r.state = gorRun
				atomic.AddInt32(&r.routine.standbyRoutines, 1)
				rt.t()
				r.state = gorDone
				atomic.AddInt32(&r.routine.standbyRoutines, -1)
				atomic.AddUint64(&r.routine.doneTasks, 1)
			}

			r.state = gorWait
		}

		r.state = gorShutdown
	}(r)
}

type routineTask struct {
	f Future
	t func()
}

type routine struct {
	state           int32
	maxRoutines     int
	standbyRoutines int32
	maxQueuedTasks  int
	doneTasks       uint64
	tq              BlockingQueue
	routineSlots    map[int]*routineWorker
}

func (r *routine) MaxRoutines() int {
	return r.maxRoutines
}

func (r *routine) StandbyRoutines() int {
	return int(r.standbyRoutines)
}

func (r *routine) MaxTasks() int {
	return r.maxQueuedTasks
}

func (r *routine) Tasks() int {
	return r.tq.Len()
}

func (r *routine) DoneTasks() uint64 {
	return r.doneTasks
}

func (r *routine) buildTask(submit func() interface{}, execute func()) (Future, func()) {
	sf := submit
	ef := execute
	rf := NewFuture()
	task := func() {
		rf := rf
		defer func() {
			if e := recover(); e != nil {
				if cast, ok := e.(error); ok {
					rf.Completable().Fail(cast)
				} else {
					rf.Completable().Fail(fmt.Errorf("%v", e))
				}
			}
		}()

		if sf != nil {
			rf.Completable().Complete(sf())
		}

		if ef != nil {
			ef()
			rf.Completable().Complete(nil)
		}
	}

	return rf, task
}

func (r *routine) pushTask(rf Future, task func()) *routineTask {
	if r.maxQueuedTasks >= 0 {
		for r.tq.Len()-int(r.standbyRoutines) > r.maxQueuedTasks {
			if atomic.LoadInt32(&r.state) != gorRun {
				rf.Completable().Cancel()
				return &routineTask{
					f: rf,
					t: task,
				}
			}

			runtime.Gosched()
		}
	}

	rt := &routineTask{
		f: rf,
		t: task,
	}

	r.tq.Push(rt)
	return rt
}

func (r *routine) Submit(f func() interface{}) Future {
	if atomic.LoadInt32(&r.state) != gorRun {
		return NewCancelledFuture()
	}

	return r.pushTask(r.buildTask(f, nil)).f
}

func (r *routine) Execute(f func()) Future {
	if atomic.LoadInt32(&r.state) != gorRun {
		return NewCancelledFuture()
	}

	return r.pushTask(r.buildTask(nil, f)).f
}

func (r *routine) IsShutdown() bool {
	return atomic.LoadInt32(&r.state) == gorShutdown
}

func (r *routine) ShutdownGracefully() {
	if !atomic.CompareAndSwapInt32(&r.state, gorRun, gorPendingShutdown) {
		panic(ErrShutdownTwice)
	}

	for r.tq.Len() > 0 {
		runtime.Gosched()
	}

	for _, routine := range r.routineSlots {
		routine.shutdown.Completable().Complete(nil)
	}

	r.tq.Close()
	for _, routine := range r.routineSlots {
		for routine.state != gorShutdown {
			runtime.Gosched()
		}
	}

	atomic.StoreInt32(&r.state, gorShutdown)
}

func (r *routine) ShutdownImmediately() {
	if !atomic.CompareAndSwapInt32(&r.state, gorRun, gorPendingShutdown) {
		panic(ErrShutdownTwice)
	}

	for _, routine := range r.routineSlots {
		routine.shutdown.Completable().Complete(nil)
	}

	r.tq.Close()
	for _, routine := range r.routineSlots {
		for routine.state != gorShutdown {
			runtime.Gosched()
		}
	}

	atomic.StoreInt32(&r.state, gorShutdown)
}

// NewRoutinePool
// maxQueuedTasks -1 for unlimited
func NewRoutinePool(nRoutines int, maxQueuedTasks int) RoutineService {
	r := &routine{
		state:           gorRun,
		maxRoutines:     nRoutines,
		standbyRoutines: int32(nRoutines),
		maxQueuedTasks:  maxQueuedTasks,
		routineSlots:    map[int]*routineWorker{},
	}

	for i := 0; i < nRoutines; i++ {
		rw := &routineWorker{
			routine:  r,
			n:        i,
			shutdown: NewFuture(),
		}

		r.routineSlots[i] = rw
		rw.run()
	}

	return r
}
