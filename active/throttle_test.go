package active_test

import (
	"fmt"
	"testing"

	"github.com/m-lab/etl/active"
)

type source struct {
	count int
	c     counter
}

func (s *source) Next(ctx context.Context) (active.Runnable, error) {
	if s.count < 5 {
		s.count++
		return c.toRunnable(), nil
	}
	return nil, iterator.Done
}

func (s *source) Next() (active.Runnable, error) {
	return fmt.Sprint(s.count)
}

func TestThrottledSource(t *testing.T) {
}
