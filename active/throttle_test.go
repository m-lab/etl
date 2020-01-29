package active_test

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/m-lab/etl/active"
	"google.golang.org/api/iterator"
)

// TODO maxTracker is a bad name.  It is unrelated to gardener tracker package.
type maxTracker struct {
	running   chan struct{}
	doneCount chan struct{}

	lock       sync.Mutex
	maxRunning int
}

func (mt *maxTracker) add() int {
	mt.running <- struct{}{} // Add to the count.
	now := len(mt.running)

	mt.lock.Lock()
	defer mt.lock.Unlock()
	if mt.maxRunning < now {
		mt.maxRunning = now
	}

	return now
}

func (mt *maxTracker) end() {
	// decrement inFlight, and increment doneCount.
	mt.doneCount <- <-mt.running
}

func (mt *maxTracker) done() int {
	return len(mt.doneCount)
}

func (mt *maxTracker) max() int {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	return mt.maxRunning
}

func newMaxTracker(n int) *maxTracker {
	return &maxTracker{
		running:   make(chan struct{}, n),
		doneCount: make(chan struct{}, n),
	}
}

type source struct {
	count   int
	tracker *maxTracker
}

type maxRunnable struct {
	tracker *maxTracker
}

func (s *source) Next(ctx context.Context) (active.Runnable, error) {
	if s.count > 0 {
		s.count--
		return &maxRunnable{s.tracker}, nil
	}
	return nil, iterator.Done
}
func (s *source) Label() string {
	return "label"
}

func (mr *maxRunnable) Run() error {
	now := mr.tracker.add()
	defer mr.tracker.end()

	log.Println(now, "running")

	time.Sleep(1 * time.Millisecond)

	return nil
}

func (tr *maxRunnable) Info() string {
	return "info"
}

func TestThrottledSource(t *testing.T) {
	src := source{count: 5, tracker: newMaxTracker(100)}
	// throttle to handle two at a time.
	ts := active.Throttle(&src, active.NewWSTokenSource(2))

	eg, err := runAll(context.Background(), ts)
	if err != iterator.Done {
		t.Fatal("Expected iterator.Done", err)
	}

	err = eg.Wait()
	if err != nil {
		t.Fatal(err)
	}

	if src.tracker.done() != 5 {
		t.Error("Should have been 5 runnables", src.tracker.done())
	}
	if len(src.tracker.running) != 0 {
		t.Error("running should be 0:", len(src.tracker.running))
	}
	if src.tracker.max() != 2 {
		t.Error("Max running != 2", src.tracker.max())
	}
}
