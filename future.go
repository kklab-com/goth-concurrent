package concurrent

import (
	"container/list"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"
)

const (
	stateWait = iota
	stateSuccess
	stateCancel
	stateFail
)

type ChainFuture interface {
	Future
	Prev() ChainFuture
	Then(fn func(prev Future) any) (future ChainFuture)
	ThenAsync(fn func(prev Future) any) (future ChainFuture)
}

type Future interface {
	Immutable
	Completable() Completable
	Immutable() Immutable
	Chainable() ChainFuture
}

type Immutable interface {
	Get() any
	GetTimeout(timeout time.Duration) any
	GetNow() any
	Done() <-chan struct{}
	Await() Future
	AwaitTimeout(timeout time.Duration) Future
	IsDone() bool
	IsSuccess() bool
	IsCancelled() bool
	IsFail() bool
	Error() error
	AddListener(listener FutureListener) Future
}

type Completable interface {
	Complete(obj any) bool
	Cancel() bool
	Fail(err error) bool
}

type Settable interface {
	Set(obj any)
}

type CastFuture[T any] interface {
	Get() (obj T)
	GetTimeout(timeout time.Duration) (obj T)
	GetNow() (obj T)
	Done() <-chan struct{}
	Await() CastFuture[T]
	AwaitTimeout(timeout time.Duration) CastFuture[T]
	IsDone() bool
	IsSuccess() bool
	IsCancelled() bool
	IsFail() bool
	Error() error
	AddListener(listener FutureListener) CastFuture[T]
	Completable() Completable
	Chainable() ChainFuture
	BaseFuture() Future
}

func NewCastFuture[T any]() CastFuture[T] {
	var an any = &DefaultCastFuture[T]{Future: NewFuture()}
	return an.(CastFuture[T])
}

func WrapCastFuture[T any](future Future) CastFuture[T] {
	var an any = &DefaultCastFuture[T]{Future: future}
	return an.(CastFuture[T])
}

type DefaultCastFuture[T any] struct {
	Future
}

func (f *DefaultCastFuture[T]) Get() (obj T) {
	if v := f.Future.Get(); v != nil {
		obj = v.(T)
	}

	return
}

func (f *DefaultCastFuture[T]) GetTimeout(timeout time.Duration) (obj T) {
	if v := f.Future.GetTimeout(timeout); v != nil {
		obj = v.(T)
	}

	return
}

func (f *DefaultCastFuture[T]) GetNow() (obj T) {
	if v := f.Future.GetNow(); v != nil {
		obj = v.(T)
	}

	return
}

func (f *DefaultCastFuture[T]) Await() CastFuture[T] {
	f.BaseFuture().Get()
	return f
}

func (f *DefaultCastFuture[T]) AwaitTimeout(timeout time.Duration) CastFuture[T] {
	f.BaseFuture().GetTimeout(timeout)
	return f
}

func (f *DefaultCastFuture[T]) AddListener(listener FutureListener) CastFuture[T] {
	f.BaseFuture().AddListener(listener)
	return f
}

func (f *DefaultCastFuture[T]) BaseFuture() Future {
	return f.Future
}

func NewFuture() Future {
	return newDefaultFuture()
}

func NewCompletedFuture(obj any) Future {
	f := NewFuture()
	f.Completable().Complete(obj)
	return f
}

func NewCancelledFuture() Future {
	f := NewFuture()
	f.Completable().Cancel()
	return f
}

func NewFailedFuture(err error) Future {
	f := NewFuture()
	f.Completable().Fail(err)
	return f
}

func NewChainFuture(future ChainFuture) ChainFuture {
	f := newDefaultFuture()
	f.prev = future
	return f
}

type DefaultFuture struct {
	op
	ch        chan struct{}
	obj       any
	state     int32
	err       error
	listeners list.List
	prev      ChainFuture
}

func newDefaultFuture() *DefaultFuture {
	var f = &DefaultFuture{}
	f.ch = make(chan struct{})
	return f
}

func (f *DefaultFuture) Prev() ChainFuture {
	return f.prev
}

func (f *DefaultFuture) self() Future {
	return f
}

func (f *DefaultFuture) Completable() Completable {
	return f.self().(Completable)
}

func (f *DefaultFuture) Immutable() Immutable {
	return f.self().(Immutable)
}

func (f *DefaultFuture) Chainable() ChainFuture {
	return f.self().(ChainFuture)
}

func (f *DefaultFuture) Future() Future {
	return f.self().(Future)
}

func (f *DefaultFuture) _waitJudge(timeout time.Duration) (done bool) {
	if timeout == 0 {
		<-f.ch
		return true
	} else {
		select {
		case <-f.ch:
			return true
		case <-time.After(timeout):
			return false
		}
	}
}

func (f *DefaultFuture) Get() any {
	return f.GetTimeout(0)
}

func (f *DefaultFuture) GetTimeout(timeout time.Duration) any {
	if f.IsDone() {
		return f.obj
	}

	if f._waitJudge(timeout) {
		return f.obj
	}

	return nil
}

func (f *DefaultFuture) GetNow() any {
	return f.obj
}

func (f *DefaultFuture) Done() <-chan struct{} {
	return f.ch
}

func (f *DefaultFuture) Await() Future {
	f.Get()
	return f
}

func (f *DefaultFuture) AwaitTimeout(timeout time.Duration) Future {
	f.GetTimeout(timeout)
	return f
}

func (f *DefaultFuture) IsDone() bool {
	return atomic.LoadInt32(&f.state) != stateWait
}

func (f *DefaultFuture) IsSuccess() bool {
	return atomic.LoadInt32(&f.state) == stateSuccess
}

func (f *DefaultFuture) IsCancelled() bool {
	return atomic.LoadInt32(&f.state) == stateCancel
}

func (f *DefaultFuture) IsFail() bool {
	return atomic.LoadInt32(&f.state) == stateFail
}

func (f *DefaultFuture) Error() error {
	return f.err
}

func (f *DefaultFuture) AddListener(listener FutureListener) Future {
	if listener == nil {
		return f
	}

	f.acquire()
	defer f.release()
	if f.IsDone() {
		<-f.ch
		listener.OperationCompleted(f)
		return f
	}

	f.listeners.PushBack(listener)
	return f
}

func (f *DefaultFuture) Complete(obj any) bool {
	f.acquire()
	defer f.release()
	if atomic.CompareAndSwapInt32(&f.state, stateWait, stateSuccess) {
		if obj != nil {
			f.obj = obj
		}

		close(f.ch)
		f.callListener()
		return true
	}

	return false
}

func (f *DefaultFuture) Cancel() bool {
	f.acquire()
	defer f.release()
	if atomic.CompareAndSwapInt32(&f.state, stateWait, stateCancel) {
		close(f.ch)
		f.callListener()
		return true
	}

	return false
}

func (f *DefaultFuture) Fail(err error) bool {
	f.acquire()
	defer f.release()
	if atomic.CompareAndSwapInt32(&f.state, stateWait, stateFail) {
		f.err = err
		close(f.ch)
		f.callListener()
		return true
	}

	return false
}

func (f *DefaultFuture) Set(obj any) {
	f.obj = obj
}

func (f *DefaultFuture) callListener() {
	for v := f.listeners.Front(); v != nil; v = f.listeners.Front() {
		f.listeners.Remove(f.listeners.Front())
		listener := v.Value.(FutureListener)
		if ref := reflect.ValueOf(listener); ref.Kind() == reflect.Ptr && ref.IsNil() {
			continue
		}

		listener.OperationCompleted(f)
	}
}

func (f *DefaultFuture) Then(fn func(prev Future) any) (future ChainFuture) {
	cf := f
	future = NewChainFuture(cf)
	lfn := fn
	f.AddListener(NewFutureListener(func(f Future) {
		if cf.IsCancelled() {
			future.Completable().Cancel()
			return
		}

		if future.IsDone() {
			return
		}

		var rtn any
		defer func() {
			if e := recover(); e != nil {
				if err, ok := e.(error); ok {
					future.Completable().Fail(err)
				} else {
					future.Completable().Fail(fmt.Errorf("%v", e))
				}
			} else {
				future.Completable().Complete(rtn)
			}
		}()

		rtn = lfn(cf)
	}))

	return future
}

func (f *DefaultFuture) ThenAsync(fn func(prev Future) any) (future ChainFuture) {
	cf := f
	future = NewChainFuture(cf)
	lfn := fn
	f.AddListener(NewFutureListener(func(f Future) {
		if cf.IsCancelled() {
			future.Completable().Cancel()
			return
		}

		if future.IsDone() {
			return
		}

		go func(cf Future, future Future, lfn func(prev Future) any) {
			var rtn any
			defer func() {
				if e := recover(); e != nil {
					if err, ok := e.(error); ok {
						future.Completable().Fail(err)
					} else {
						future.Completable().Fail(fmt.Errorf("%v", e))
					}
				} else {
					future.Completable().Complete(rtn)
				}
			}()

			rtn = lfn(cf)
		}(cf, future, lfn)
	}))

	return future
}

type FutureListener interface {
	OperationCompleted(f Future)
}

type _FutureListener struct {
	f func(f Future)
}

func (l *_FutureListener) OperationCompleted(f Future) {
	l.f(f)
}

func NewFutureListener(f func(f Future)) FutureListener {
	if f == nil {
		return nil
	}

	return &_FutureListener{
		f: f,
	}
}

func Do(f func() any) (future Future) {
	future = NewFuture()
	var rtn any
	defer func() {
		if e := recover(); e != nil {
			if err, ok := e.(error); ok {
				future.Completable().Fail(err)
			} else {
				future.Completable().Fail(fmt.Errorf("%v", e))
			}
		} else {
			future.Completable().Complete(rtn)
		}
	}()

	rtn = f()
	return future
}

func DoAsync(f func() any) (future Future) {
	future = NewFuture()
	go func() {
		var rtn any
		defer func() {
			if e := recover(); e != nil {
				if err, ok := e.(error); ok {
					future.Completable().Fail(err)
				} else {
					future.Completable().Fail(fmt.Errorf("%v", e))
				}
			} else {
				future.Completable().Complete(rtn)
			}
		}()

		rtn = f()
	}()

	return future
}
