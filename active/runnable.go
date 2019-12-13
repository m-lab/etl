// Package active provides code for managing processing of an entire
// directory of task files.
package active

import (
	"context"
	"sync"
)

// Runnable is just a function that does something and returns an error.
// A Runnable may return ErrShouldRetry if there was a non-persistent error.
// TODO - should this instead be and interface, with Run() and ShouldRetry()?
type Runnable = func() error

// Source provides a Next function that returns Next functions.
type Source interface {
	// Next should return iterator.Done when there are no more Runnables.
	// It may block if there are no more runnables available right now,
	// (or if throttling is applied)
	Next(ctx context.Context) (Runnable, error)
}

// RunAll will execute functions provided by Next() until there are no more,
// or the context is canceled.
func RunAll(ctx context.Context, rSrc Source) {
	wg := sync.WaitGroup{}
	for {
		run, err := rSrc.Next(ctx)
		if err != nil {
			debug.Println(err)
			break
		}
		wg.Add(1)
		debug.Println("Starting func")
		go func() error {
			defer wg.Done()
			return run()
		}()
	}
	wg.Wait()
}
