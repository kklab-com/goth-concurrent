package concurrent

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFuture(t *testing.T) {
	fs := NewFuture()
	for i := 0; i < 100; i++ {
		go func() {
			assert.EqualValues(t, 1, fs.Get())
			assert.EqualValues(t, true, fs.IsDone())
			assert.EqualValues(t, true, fs.IsSuccess())
		}()
	}

	ts := time.Now()
	go func() {
		time.Sleep(time.Millisecond)
		fs.Completable().Complete(1)
	}()

	assert.Nil(t, fs.GetNow())
	assert.EqualValues(t, false, fs.IsDone())
	fs.Await()
	assert.True(t, time.Now().Sub(ts) > time.Millisecond)
	assert.EqualValues(t, 1, fs.Get())
	assert.EqualValues(t, true, fs.IsDone())
	assert.EqualValues(t, true, fs.IsSuccess())
	assert.EqualValues(t, false, fs.IsCancelled())
	assert.EqualValues(t, false, fs.IsFail())

	f := NewFuture()
	go func() {
		time.Sleep(time.Microsecond)
		f.Completable().Complete(1)
	}()

	f.AddListener(NewFutureListener(func(f Future) {
		if !f.IsSuccess() {
			assert.Fail(t, "should not go into this scope")
		}
	}))

	f.Await()

	f = NewFuture()
	go func() {
		time.Sleep(time.Microsecond)
		f.Completable().Cancel()
	}()

	f.AddListener(NewFutureListener(func(f Future) {
		if !f.IsCancelled() {
			assert.Fail(t, "should not go into this scope")
		}
	}))

	f.Await()

	f = NewFuture()
	go func() {
		time.Sleep(time.Microsecond)
		f.Completable().Fail(fmt.Errorf("fail"))
	}()

	f.AddListener(NewFutureListener(func(f Future) {
		if !f.IsFail() {
			assert.Fail(t, "should not go into this scope")
		}
	}))

	assert.EqualValues(t, false, f.IsDone())
	f.Await()
	assert.EqualValues(t, "fail", f.Error().Error())

	f = NewCompletedFuture("s")
	assert.EqualValues(t, "s", f.GetNow())
	assert.EqualValues(t, "s", f.Get())
	assert.EqualValues(t, true, f.IsDone())
	assert.EqualValues(t, true, f.IsSuccess())
	assert.EqualValues(t, nil, f.Error())
	f.AddListener(NewFutureListener(func(f Future) {
		assert.True(t, f.IsSuccess())
	}))

	f = NewCancelledFuture()
	assert.EqualValues(t, nil, f.Get())
	assert.EqualValues(t, true, f.IsDone())
	assert.EqualValues(t, true, f.IsCancelled())
	assert.EqualValues(t, true, f.Error() == nil)
	f.AddListener(NewFutureListener(func(f Future) {
		assert.True(t, f.IsCancelled())
	}))

	f = NewFailedFuture(fmt.Errorf("err"))
	assert.EqualValues(t, nil, f.Get())
	assert.EqualValues(t, true, f.IsDone())
	assert.EqualValues(t, true, f.IsFail())
	assert.EqualValues(t, "err", f.Error().Error())
	f.AddListener(NewFutureListener(func(f Future) {
		assert.True(t, f.IsFail())
	}))

	time.Sleep(time.Millisecond)

	ffv := int32(0)
	f = NewFuture()
	listener := NewFutureListener(func(f Future) {
		ffvP := f.Get().(*int32)
		assert.EqualValues(t, 0, *ffvP)
		atomic.StoreInt32(ffvP, 1)
	})

	f.AddListener(listener)
	f.Completable().Complete(&ffv)
	assert.EqualValues(t, 1, ffv)

	f = NewFuture()
	st := time.Now()
	go func() {
		<-time.After(time.Second / 2)
		f.Completable().Complete("")
	}()

	f.Await()
	assert.True(t, st.Add(time.Second/2).Before(time.Now()))

	f = NewFuture()
	startTime := time.Now()
	f.AwaitTimeout(time.Millisecond)
	assert.True(t, time.Now().Sub(startTime) > time.Millisecond)
	assert.False(t, f.IsDone())
	startTime = time.Now()
	go func() {
		time.After(time.Microsecond)
		f.Completable().Complete(nil)
	}()

	f.GetTimeout(time.Millisecond)
	assert.True(t, time.Now().Sub(startTime) < time.Millisecond)
	assert.True(t, f.IsDone())
	assert.True(t, f.IsSuccess())

	assert.True(t, f.Immutable().IsDone())
	assert.True(t, f.Immutable().IsSuccess())

	f = NewFuture()
	go func() {
		<-time.After(time.Millisecond)
		f.Completable().Complete("1")
	}()

	assert.Equal(t, "3", f.Chainable().Then(func(parent Future) any {
		assert.Equal(t, "1", parent.Get())
		return "2"
	}).Then(func(parent Future) any {
		assert.Equal(t, "2", parent.Get())
		assert.Equal(t, "1", parent.Chainable().Prev().Get())
		assert.True(t, parent.IsSuccess())
		return "3"
	}).Get())

	f = NewFuture()
	ts = time.Now()
	go func() {
		<-time.After(time.Millisecond)
		f.Completable().Complete(nil)
	}()

	nf := f.Chainable().ThenAsync(func(parent Future) any {
		return "2"
	}).ThenAsync(func(parent Future) any {
		return "3"
	})

	assert.True(t, time.Now().Sub(ts) < time.Millisecond)
	nf.Await()
	assert.Equal(t, "3", nf.Get())
	assert.Equal(t, "2", nf.Chainable().Prev().Get())
	assert.Nil(t, nf.Chainable().Prev().Prev().Get())
	assert.True(t, time.Now().Sub(ts) > time.Millisecond)

	f = NewFuture()
	go func() {
		<-time.After(time.Millisecond)
		f.Completable().Cancel()
	}()

	assert.True(t, f.Chainable().Then(func(parent Future) any {
		return nil
	}).Await().IsCancelled())

	f = NewFuture()
	f.(Settable).Set("1")
	assert.True(t, !f.IsDone())
	assert.Equal(t, "1", f.GetNow())
	assert.True(t, f.Completable().Complete("2"))
	assert.Equal(t, "2", f.Get())
}

func TestFuture_Parallel(t *testing.T) {
	tc := 100000
	c := int32(0)
	wg := sync.WaitGroup{}
	for i := 0; i < tc; i++ {
		wg.Add(2)
		f := NewFuture()
		go func(f Future) {
			f.AddListener(NewFutureListener(func(f Future) {
				atomic.AddInt32(&c, 1)
			}))
			wg.Done()
		}(f)

		f.Chainable().Then(func(parent Future) any {
			time.Sleep(time.Microsecond)
			return atomic.AddInt32(&c, 1)
		})

		go func() {
			f.Completable().Complete(nil)
			wg.Done()
		}()
	}

	wg.Wait()
	assert.Equal(t, int32(tc)*2, atomic.LoadInt32(&c))
}

func TestFuture_NewCastFuture(t *testing.T) {
	MSG := "NEW"
	cf := NewCastFuture[string]()
	assert.Empty(t, cf.GetNow())
	cf.Completable().Complete(MSG)
	assert.Equal(t, MSG, cf.Get())
	assert.Equal(t, MSG, cf.GetNow())
	assert.Equal(t, MSG, cf.BaseFuture().Get())
	pcf := NewCastFuture[*string]()
	assert.Nil(t, pcf.GetNow())
	pcf.Completable().Complete(&MSG)
	assert.Equal(t, MSG, *pcf.Get())
	assert.Equal(t, MSG, *pcf.GetNow())
}

func TestFuture_WrapCastFuture(t *testing.T) {
	MSG := "NEW"
	f := NewFuture()
	cf := WrapCastFuture[string](f)
	assert.Empty(t, cf.GetNow())
	f.Completable().Complete(MSG)
	assert.Equal(t, MSG, f.Get())
	assert.Equal(t, MSG, cf.Get())
	assert.Equal(t, MSG, cf.GetNow())
	assert.Equal(t, MSG, cf.BaseFuture().Get())
}

func TestFuture_CastFutureAwait(t *testing.T) {
	MSG := "NEW"
	cf := NewCastFuture[string]()
	done1 := NewFuture()
	done2 := NewFuture()
	assert.Empty(t, cf.GetNow())
	cf.AddListener(NewFutureListener(func(f Future) {
		if f.Get() == MSG {
			done1.Completable().Complete(MSG)
		}
	}))

	done1.AddListener(NewFutureListener(func(f Future) {
		done2.Completable().Complete(nil)
	}))

	cf.Completable().Complete(MSG)
	assert.True(t, done1.AwaitTimeout(time.Second).IsSuccess())
	assert.True(t, done2.AwaitTimeout(time.Second).IsSuccess())
}
