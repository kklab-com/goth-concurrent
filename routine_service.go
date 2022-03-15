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

var ErrShutdownTwice = fmt.Errorf("shutdown a shutdown rs service")
var ErrTaskQueueFull = fmt.Errorf("task queue is full")
var ErrShutdown = fmt.Errorf("shutdown")

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

type routine struct {
	rs       *routines
	shutdown Future
	state    int
	n        int
}

func (r *routine) run() {
	go func(r *routine) {
		for !r.shutdown.IsDone() {
			if v := r.rs.tq.Pop(); v == nil {
				select {
				case <-r.shutdown.Done():
					break
				}
			} else {
				rt := v.(*routineTask)
				r.state = gorRun
				atomic.AddInt32(&r.rs.standbyRoutines, 1)
				rt.t()
				r.state = gorDone
				atomic.AddInt32(&r.rs.standbyRoutines, -1)
				atomic.AddUint64(&r.rs.doneTasks, 1)
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

type routines struct {
	blocking        bool
	state           int32
	maxRoutines     int
	standbyRoutines int32
	maxQueuedTasks  int
	doneTasks       uint64
	tq              BlockingQueue
	routineSlots    map[int]*routine
}

func (r *routines) MaxRoutines() int {
	return r.maxRoutines
}

func (r *routines) StandbyRoutines() int {
	return int(r.standbyRoutines)
}

func (r *routines) MaxTasks() int {
	return r.maxQueuedTasks
}

func (r *routines) Tasks() int {
	return r.tq.Len()
}

func (r *routines) DoneTasks() uint64 {
	return r.doneTasks
}

func (r *routines) buildTask(submit func() interface{}, execute func()) (Future, func()) {
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

func (r *routines) pushTask(rf Future, task func()) *routineTask {
	if r.maxQueuedTasks >= 0 {
		for r.tq.Len()-int(r.standbyRoutines) > r.maxQueuedTasks {
			if !r.blocking {
				rf.Completable().Fail(ErrTaskQueueFull)
				return &routineTask{
					f: rf,
					t: task,
				}
			} else if atomic.LoadInt32(&r.state) != gorRun {
				rf.Completable().Fail(ErrShutdown)
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

func (r *routines) Submit(f func() interface{}) Future {
	if atomic.LoadInt32(&r.state) != gorRun {
		return NewFailedFuture(ErrShutdown)
	}

	return r.pushTask(r.buildTask(f, nil)).f
}

func (r *routines) Execute(f func()) Future {
	if atomic.LoadInt32(&r.state) != gorRun {
		return NewFailedFuture(ErrShutdown)
	}

	return r.pushTask(r.buildTask(nil, f)).f
}

func (r *routines) IsShutdown() bool {
	return atomic.LoadInt32(&r.state) == gorShutdown
}

// ShutdownGracefully decline all incoming task, wait for all queued tasks done,
// shutdown all routine then return
func (r *routines) ShutdownGracefully() {
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

// ShutdownImmediately decline all incoming task, drop all queued task,
// shutdown all routine then return
func (r *routines) ShutdownImmediately() {
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

func newRoutinePool(nRoutines int, maxQueuedTasks int, blocking bool) RoutineService {
	r := &routines{
		blocking:        blocking,
		state:           gorRun,
		maxRoutines:     nRoutines,
		standbyRoutines: int32(nRoutines),
		maxQueuedTasks:  maxQueuedTasks,
		routineSlots:    map[int]*routine{},
	}

	for i := 0; i < nRoutines; i++ {
		rw := &routine{
			rs:       r,
			n:        i,
			shutdown: NewFuture(),
		}

		r.routineSlots[i] = rw
		rw.run()
	}

	return r
}

// NewRoutinePool create a fix rs pool with limited task queue
// Submit/Execute will return a FailFuture when task queue is full
// maxQueuedTasks -1 for unlimited tasks
func NewRoutinePool(nRoutines int, maxQueuedTasks int) RoutineService {
	return newRoutinePool(nRoutines, maxQueuedTasks, false)
}

// NewBlockingRoutinePool create a fix rs pool with limited task queue
// Submit/Execute will be blocking when task queue is full
// maxQueuedTasks -1 for unlimited tasks
func NewBlockingRoutinePool(nRoutines int, maxQueuedTasks int) RoutineService {
	return newRoutinePool(nRoutines, maxQueuedTasks, true)
}
