package concurrent

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	gorReady = iota
	gorRunning
	gorPendingShutdown
	gorShutdown
)

var ErrShutdownTwice = fmt.Errorf("shutdown a shutdown rs service")
var ErrTaskQueueFull = fmt.Errorf("task queue is full")
var ErrShutdown = fmt.Errorf("shutdown")

type SubmitTask func(r Routine) interface{}

type RoutineService interface {
	Submit(SubmitTask) RoutineFuture
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
		return drf.r.RoutineId()
	}

	return 0
}

type Routine interface {
	RoutineId() int
	Local() *sync.Map
}

type routine struct {
	rs        *routines
	shutdown  Future
	kv        sync.Map
	state     int
	rid       uint32
	doneCount uint64
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
		atomic.StoreUint32(&r.rid, uint32(r.getGoRoutineId()))
		for !r.shutdown.IsDone() {
			if v := r.rs.tq.PopTimeout(time.Millisecond); v == nil {
				if r.shutdown.IsDone() {
					break
				}
			} else {
				r.state = gorRunning
				v.(*routineTask).t(r)
				r.doneCount++
				r.state = gorReady
			}
		}

		r.state = gorShutdown
	}(r)
}

func (r *routine) RoutineId() int {
	return int(r.rid)
}

func (r *routine) Local() *sync.Map {
	return &r.kv
}

type routineTask struct {
	f RoutineFuture
	t func(r *routine)
}

type routines struct {
	blocking       bool
	state          int32
	maxRoutines    int
	maxQueuedTasks int
	tq             BlockingQueue
	routineList    []*routine
}

func (rs *routines) MaxRoutines() int {
	return rs.maxRoutines
}

func (rs *routines) StandbyRoutines() int {
	return func() int {
		v := 0
		for _, r := range rs.routineList {
			if r.state == gorReady {
				v++
			}
		}

		return v
	}()
}

func (rs *routines) MaxTasks() int {
	return rs.maxQueuedTasks
}

func (rs *routines) Tasks() int {
	return rs.tq.Len()
}

func (rs *routines) DoneTasks() uint64 {
	return func() uint64 {
		v := uint64(0)
		for _, r := range rs.routineList {
			v += r.doneCount
		}

		return v
	}()
}

func (rs *routines) buildTask(submit SubmitTask) (RoutineFuture, func(r *routine)) {
	sf := submit
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
			rf.Completable().Complete(sf(r))
		}
	}

	return rf, task
}

func (rs *routines) pushTask(rf RoutineFuture, task func(r *routine)) *routineTask {
	if rs.maxQueuedTasks >= 0 {
		for rs.tq.Len()-rs.StandbyRoutines() > rs.maxQueuedTasks {
			if !rs.blocking {
				rf.Completable().Fail(ErrTaskQueueFull)
				return &routineTask{
					f: rf,
					t: task,
				}
			} else if atomic.LoadInt32(&rs.state) != gorRunning {
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

	if rs.maxQueuedTasks < 0 {
		rs.tq.PushTimeout(rt, time.Microsecond)
		return rt
	} else {
		if !rs.blocking {
			if rs.tq.PushTimeout(rt, time.Microsecond) {
				return rt
			}

			rf.Completable().Fail(ErrTaskQueueFull)
			return rt
		}

		for !rs.tq.PushTimeout(rt, time.Microsecond) && rs.state == gorRunning {
		}

		return rt
	}
}

func (rs *routines) Submit(f SubmitTask) RoutineFuture {
	if atomic.LoadInt32(&rs.state) != gorRunning {
		return &defaultRoutineFuture{Future: NewFailedFuture(ErrShutdown)}
	}

	return rs.pushTask(rs.buildTask(f)).f
}

func (rs *routines) IsShutdown() bool {
	return atomic.LoadInt32(&rs.state) == gorShutdown
}

// ShutdownGracefully decline all incoming task, wait for all queued tasks done,
// shutdown all routine then return
func (rs *routines) ShutdownGracefully() {
	if !atomic.CompareAndSwapInt32(&rs.state, gorRunning, gorPendingShutdown) {
		panic(ErrShutdownTwice)
	}

	for rs.tq.Len() > 0 {
		runtime.Gosched()
	}

	for _, r := range rs.routineList {
		r.shutdown.Completable().Complete(nil)
	}

	for _, r := range rs.routineList {
		for r.state != gorShutdown {
			runtime.Gosched()
		}
	}

	atomic.StoreInt32(&rs.state, gorShutdown)
}

// ShutdownImmediately decline all incoming task, drop all queued task,
// shutdown all routine then return
func (rs *routines) ShutdownImmediately() {
	if !atomic.CompareAndSwapInt32(&rs.state, gorRunning, gorPendingShutdown) {
		panic(ErrShutdownTwice)
	}

	for _, r := range rs.routineList {
		r.shutdown.Completable().Complete(nil)
	}

	atomic.StoreInt32(&rs.state, gorShutdown)
}

func newRoutinePool(nRoutines int, maxQueuedTasks int, blocking bool) RoutineService {
	r := &routines{
		blocking:       blocking,
		tq:             NewBlockingQueue(maxQueuedTasks),
		state:          gorRunning,
		maxRoutines:    nRoutines,
		maxQueuedTasks: maxQueuedTasks,
	}

	for i := 0; i < nRoutines; i++ {
		rw := &routine{
			rs:       r,
			shutdown: NewFuture(),
		}

		r.routineList = append(r.routineList, rw)
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
