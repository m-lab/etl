// Package active provides code for managing processing of an entire
// directory of task files.
package active

import (
	"context"

	"github.com/m-lab/etl/metrics"
	"golang.org/x/sync/errgroup"
)

// Runnable is just a function that does something and returns an error.
// A Runnable may return ErrShouldRetry if there was a non-persistent error.
// TODO - should this instead be and interface, with Run() and ShouldRetry()?
type Runnable interface {
	Run() error
	Info() string
}

// Source provides a Next function that returns Runnables.
type Source interface {
	// Next should return iterator.Done when there are no more Runnables.
	// It may block if there are no more runnables available right now,
	// (or if throttling is applied)
	Next(ctx context.Context) (Runnable, error)

	// Name returns a string for use in metrics and debug logs'
	Label() string
}

// RunAll will execute functions provided by Next() until there are no more,
// or the context is canceled.
func RunAll(ctx context.Context, rSrc Source) error {
	eg := errgroup.Group{}
	for {
		run, err := rSrc.Next(ctx)
		if err != nil {
			debug.Println(err)
			break
		}
		debug.Println("Starting func")
		f := func() error {
			metrics.ActiveTasks.WithLabelValues(rSrc.Label()).Inc()
			err := run.Run()
			metrics.ActiveTasks.WithLabelValues(rSrc.Label()).Dec()
			switch err {
			case nil:
				metrics.TestCount.WithLabelValues(rSrc.Label(), "", "ok").Inc()
			default:
				metrics.TestCount.WithLabelValues(rSrc.Label(), "", "error").Inc()
			}

			return err
		}
		eg.Go(f)
	}
	return eg.Wait()
}
