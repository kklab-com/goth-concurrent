package concurrent

import (
	"runtime"
	"sync/atomic"
	"time"
)

const (
	mutexReleased = iota
	mutexLocked
	mutexSet
)

const sleepDuration = time.Second

var passChan = make(chan struct{})

func init() {
	close(passChan)
}

type TryLocker interface {
	TryLock() bool
	TryLockTimeout(timeout time.Duration) bool
}
type op struct {
	state int32
}

func (o *op) acquire() {
	for i := 0; !atomic.CompareAndSwapInt32(&o.state, 0, 1); i++ {
		runtime.Gosched()
	}
}

func (o *op) tryAcquire() bool {
	return atomic.CompareAndSwapInt32(&o.state, 0, 1)
}

func (o *op) release() {
	atomic.StoreInt32(&o.state, 0)
}

type Mutex struct {
	ls  int32
	sig chan struct{}
}

// TryLock
// try to lock Mutex if it is unlocked
// it will return `true` when get lock success or `false` on fail
func (m *Mutex) TryLock() bool {
	return m.tryLock()
}

// TryLockTimeout
// try to get lock with timeout, if it can't get lock until timeout, it will return `false`
// it will return `true` when get lock success or `false` on fail
func (m *Mutex) TryLockTimeout(timeout time.Duration) bool {
	if m.tryLock() {
		return true
	}

	tt := time.Now().Add(timeout)
	tu := tt.Unix()
	tn := tt.Nanosecond()
	for i := 0; !m.tryLock(); i++ {
		nt := time.Now()
		if nt.Unix() >= tu && nt.Nanosecond() > tn {
			return false
		}

		if i < 64 {
			runtime.Gosched()
		} else {
			wait := tt.Sub(nt)
			if wait > sleepDuration {
				wait = sleepDuration
			}

			i = 0
			select {
			case <-m.sig:
			case <-time.After(wait):
			}
		}
	}

	return true
}

// Lock
// wait and get lock
func (m *Mutex) Lock() {
	if m.tryLock() {
		return
	}

	for i := 0; !m.tryLock(); i++ {
		if i < 64 {
			runtime.Gosched()
		} else {
			i = 0
			select {
			case <-m.sig:
			case <-time.After(sleepDuration):
			}
		}
	}
}

func (m *Mutex) tryLock() bool {
	if atomic.CompareAndSwapInt32(&m.ls, mutexReleased, mutexLocked) {
		m.sig = make(chan struct{})
		atomic.StoreInt32(&m.ls, mutexSet)
		return true
	}

	return false
}

// Unlock
// unlock mutex
func (m *Mutex) Unlock() {
	sig := m.sig
	if !atomic.CompareAndSwapInt32(&m.ls, mutexSet, mutexReleased) {
		if atomic.LoadInt32(&m.ls) == mutexLocked {
			runtime.Gosched()
			m.Unlock()
			return
		} else {
			panic("concurrent: unlock of unlocked mutex")
		}
	}

	close(sig)
}

// Unlocked
// wait until current lock release
func (m *Mutex) Unlocked() chan struct{} {
	switch atomic.LoadInt32(&m.ls) {
	case mutexReleased:
		return passChan
	case mutexLocked:
		return m.Unlocked()
	default:
		return m.sig
	}
}

// IsLocked
// check locker status
func (m *Mutex) IsLocked() bool {
	return atomic.LoadInt32(&m.ls) != mutexReleased
}
