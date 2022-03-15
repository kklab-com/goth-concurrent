package concurrent

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWaitGroup(t *testing.T) {
	ts := time.Now()
	b := &WaitGroup{}
	b.Add(100)
	go func() {
		time.Sleep(time.Millisecond)
		for i := 0; i < 100; i++ {
			go func() { b.Done() }()
		}

		b.Reset()
	}()

	b.Wait()
	assert.True(t, time.Now().Sub(ts) > time.Millisecond)
	for i := 0; i < 32000; i++ {
		b.Add(1)
		go func() {
			time.Sleep(time.Microsecond * time.Duration(rand.Int()%1000))
			b.Done()
		}()
	}

	b.Wait()
}
