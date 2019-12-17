package active_test

import (
	"context"
	"testing"

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

func (tr *tRunnable) Run() error {
	tr.c.lock.Lock()
	defer tr.c.lock.Unlock()
	tr.c.success++
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
}
