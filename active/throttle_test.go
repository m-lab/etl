package active_test

import (
	"context"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m-lab/etl/active"
	"google.golang.org/api/iterator"
)

type source struct {
	count int
	c     *counter
}

type tRunnable struct {
	c *counter
}

// accessed with atomics.
var running int32
var maxRunning int32

func (tr *tRunnable) Run() error {
	now := atomic.AddInt32(&running, 1)
	log.Println(now)
	defer atomic.AddInt32(&running, -1)
	max := atomic.LoadInt32(&maxRunning)
	// This will try to update until some thread makes maxRunning > now.
	for now > max && !atomic.CompareAndSwapInt32(&maxRunning, max, now) {
		max = atomic.LoadInt32(&maxRunning)
	}
	time.Sleep(1 * time.Millisecond)

	tr.c.lock.Lock()
	tr.c.success++
	tr.c.lock.Unlock()
	return nil
}

func (tr *tRunnable) Info() string {
	return "info"
}

func (s *source) Next(ctx context.Context) (active.Runnable, error) {
	if s.count > 0 {
		s.count--
		return &tRunnable{s.c}, nil
	}
	return nil, iterator.Done
}
func (s *source) Label() string {
	return "label"
}

func TestThrottledSource(t *testing.T) {
	src := source{5, NewCounter(t)}
	// throttle to handle two at a time.
	ts := active.Throttle(&src, active.NewWSTokenSource(2))

	active.RunAll(context.Background(), ts)

	if src.c.success != 5 {
		t.Error("Should have been 5 runnables", src.c.success)
	}
	runningNow := atomic.LoadInt32(&running)
	if runningNow != 0 {
		t.Error("running should be 0:", runningNow)
	}
	max := atomic.LoadInt32(&maxRunning)
	if max != 2 {
		t.Error("Max running != 2", max)
	}
}
