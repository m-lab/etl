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

type throttleStats struct {
	running   chan struct{}
	doneCount chan struct{}

	lock       sync.Mutex
	maxRunning int
}

func (mt *throttleStats) add() int {
	mt.running <- struct{}{} // Add to the count.
	now := len(mt.running)

	mt.lock.Lock()
	defer mt.lock.Unlock()
	if mt.maxRunning < now {
		mt.maxRunning = now
	}

	return now
}

func (mt *throttleStats) end() {
	// decrement inFlight, and increment doneCount.
	mt.doneCount <- <-mt.running
}

func (mt *throttleStats) done() int {
	return len(mt.doneCount)
}

func (mt *throttleStats) max() int {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	return mt.maxRunning
}

func newThrottleStats(n int) *throttleStats {
	return &throttleStats{
		running:   make(chan struct{}, n),
		doneCount: make(chan struct{}, n),
	}
}

type source struct {
	count int
	stats *throttleStats
}

type statsRunnable struct {
	stats *throttleStats
}

func (s *source) Next(ctx context.Context) (active.Runnable, error) {
	if s.count > 0 {
		s.count--
		return &statsRunnable{s.stats}, nil
	}
	return nil, iterator.Done
}
func (s *source) Label() string {
	return "label"
}
func (s *source) Datatype() string {
	return "datatype"
}

func (sr *statsRunnable) Run(ctx context.Context) error {
	now := sr.stats.add()
	defer sr.stats.end()

	log.Println(now, "running")

	time.Sleep(1 * time.Millisecond)

	return nil
}

func (sr *statsRunnable) Info() string {
	return "info"
}

func TestThrottledSource(t *testing.T) {
	src := source{count: 5, stats: newThrottleStats(100)}
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

	if src.stats.done() != 5 {
		t.Error("Should have been 5 runnables", src.stats.done())
	}
	if len(src.stats.running) != 0 {
		t.Error("running should be 0:", len(src.stats.running))
	}
	if src.stats.max() != 2 {
		t.Error("Max running != 2", src.stats.max())
	}
}
