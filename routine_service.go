package concurrent

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
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
	Submit(func() interface{}) RoutineFuture
	Execute(func()) RoutineFuture
	MaxRoutines() int
	StandbyRoutines() int
	MaxTasks() int
	Tasks() int
	DoneTasks() uint64
	IsShutdown() bool
	ShutdownGracefully()
	ShutdownImmediately()
}

type RoutineFuture interface {
	Future
	RoutineId() int
}

type defaultRoutineFuture struct {
	Future
	r *routine
}

func (drf *defaultRoutineFuture) Await() Future {
	drf.Future.Await()
	return drf
}

func (drf *defaultRoutineFuture) AwaitTimeout(timeout time.Duration) Future {
	drf.Future.AwaitTimeout(timeout)
	return drf
}
func (drf *defaultRoutineFuture) AddListener(listener FutureListener) Future {
	drf.Future.AddListener(listener)
	return drf
}

func (drf *defaultRoutineFuture) RoutineId() int {
	if drf.r != nil {
		return drf.r.n
	}

	return 0
}

type routine struct {
	rs       *routines
	shutdown Future
	state    int
	n        int
}

func (r *routine) getGoRoutineId() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}

	return id
}

func (r *routine) run() {
	go func(r *routine) {
		r.n = r.getGoRoutineId()
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
				rt.t(r)
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
	f RoutineFuture
	t func(r *routine)
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

func (rs *routines) MaxRoutines() int {
	return rs.maxRoutines
}

func (rs *routines) StandbyRoutines() int {
	return int(rs.standbyRoutines)
}

func (rs *routines) MaxTasks() int {
	return rs.maxQueuedTasks
}

func (rs *routines) Tasks() int {
	return rs.tq.Len()
}

func (rs *routines) DoneTasks() uint64 {
	return rs.doneTasks
}

func (rs *routines) buildTask(submit func() interface{}, execute func()) (RoutineFuture, func(r *routine)) {
	sf := submit
	ef := execute
	rf := &defaultRoutineFuture{Future: NewFuture()}
	task := func(r *routine) {
		rf := rf
		rf.r = r
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

func (rs *routines) pushTask(rf RoutineFuture, task func(r *routine)) *routineTask {
	if rs.maxQueuedTasks >= 0 {
		for rs.tq.Len()-int(rs.standbyRoutines) > rs.maxQueuedTasks {
			if !rs.blocking {
				rf.Completable().Fail(ErrTaskQueueFull)
				return &routineTask{
					f: rf,
					t: task,
				}
			} else if atomic.LoadInt32(&rs.state) != gorRun {
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

	rs.tq.Push(rt)
	return rt
}

func (rs *routines) Submit(f func() interface{}) RoutineFuture {
	if atomic.LoadInt32(&rs.state) != gorRun {
		return &defaultRoutineFuture{Future: NewFailedFuture(ErrShutdown)}
	}

	return rs.pushTask(rs.buildTask(f, nil)).f
}

func (rs *routines) Execute(f func()) RoutineFuture {
	if atomic.LoadInt32(&rs.state) != gorRun {
		return &defaultRoutineFuture{Future: NewFailedFuture(ErrShutdown)}
	}

	return rs.pushTask(rs.buildTask(nil, f)).f
}

func (rs *routines) IsShutdown() bool {
	return atomic.LoadInt32(&rs.state) == gorShutdown
}

// ShutdownGracefully decline all incoming task, wait for all queued tasks done,
// shutdown all routine then return
func (rs *routines) ShutdownGracefully() {
	if !atomic.CompareAndSwapInt32(&rs.state, gorRun, gorPendingShutdown) {
		panic(ErrShutdownTwice)
	}

	for rs.tq.Len() > 0 {
		runtime.Gosched()
	}

	for _, routine := range rs.routineSlots {
		routine.shutdown.Completable().Complete(nil)
	}

	rs.tq.Close()
	for _, routine := range rs.routineSlots {
		for routine.state != gorShutdown {
			runtime.Gosched()
		}
	}

	atomic.StoreInt32(&rs.state, gorShutdown)
}

// ShutdownImmediately decline all incoming task, drop all queued task,
// shutdown all routine then return
func (rs *routines) ShutdownImmediately() {
	if !atomic.CompareAndSwapInt32(&rs.state, gorRun, gorPendingShutdown) {
		panic(ErrShutdownTwice)
	}

	for _, routine := range rs.routineSlots {
		routine.shutdown.Completable().Complete(nil)
	}

	rs.tq.Close()
	for _, routine := range rs.routineSlots {
		for routine.state != gorShutdown {
			runtime.Gosched()
		}
	}

	atomic.StoreInt32(&rs.state, gorShutdown)
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
