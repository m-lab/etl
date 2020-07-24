package active

import (
	"context"

	"golang.org/x/sync/semaphore"
)

// TokenSource specifies the interface for a source of tokens for throttling.
type TokenSource interface {
	Acquire(ctx context.Context) error
	Release()
}

// wsTokenSource is a simple token source for initial testing.
type wsTokenSource struct {
	sem *semaphore.Weighted
}

// Acquire acquires an admission token.
func (ts *wsTokenSource) Acquire(ctx context.Context) error {
	return ts.sem.Acquire(ctx, 1)
}

// Release releases an admission token.
func (ts *wsTokenSource) Release() {
	ts.sem.Release(1)
}

// NewWSTokenSource returns a TokenSource based on semaphore.Weighted.
func NewWSTokenSource(n int) TokenSource {
	return &wsTokenSource{semaphore.NewWeighted(int64(n))}
}

// throttedSource encapsulates a Source and a throttling mechanism.
type throttledSource struct {
	RunnableSource
	throttle TokenSource
}

// throttledRunnable encapsulates a Runnable and a token release function.
type throttledRunnable struct {
	Runnable
	release func()
}

// Run implements Source.Run
func (tr *throttledRunnable) Run(ctx context.Context) error {
	// The run function must release the token when it completes.
	defer tr.release()
	return tr.Runnable.Run(ctx)
}

// Next implements Source.Next
func (ts *throttledSource) Next(ctx context.Context) (Runnable, error) {
	// We want Next to block here until a throttle token is available.
	err := ts.throttle.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	next, err := ts.RunnableSource.Next(ctx)
	if err != nil {
		ts.throttle.Release()
		return nil, err
	}
	// The Run() function must eventually release the token, so
	// the throttle.Release function is saved here.
	return &throttledRunnable{
		Runnable: next,
		release:  ts.throttle.Release}, nil
}

// Throttle applies a provided TokenSource to throttle a Source.
// This returns an interface, which is discouraged by Go advocates, but
// seems like the right thing to do here, as there is no reason to export
// the concrete type.
func Throttle(src RunnableSource, tokens TokenSource) RunnableSource {
	return &throttledSource{
		RunnableSource: src,
		throttle:       tokens}
}
