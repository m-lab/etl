// Package active provides code for managing processing of an entire
// directory of task files.
package active

import (
	"context"
)

// Runnable is just a function that does something and returns an error.
// A Runnable may return ErrShouldRetry if there was a non-persistent error.
// TODO - should this instead be and interface, with Run() and ShouldRetry()?
type Runnable interface {
	Run(context.Context) error
	Info() string
}

// RunnableSource provides a Next function that returns Runnables.
type RunnableSource interface {
	// Next should return iterator.Done when there are no more Runnables.
	// It may block if there are no more runnables available right now,
	// (or if throttling is applied)
	Next(ctx context.Context) (Runnable, error)

	// Label returns a string for use in metrics and debug logs'
	Label() string

	// Datatype returns the datatype for use in metrics
	Datatype() string
}
