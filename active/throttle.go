package active

import (
	"context"

	"golang.org/x/sync/semaphore"
)

type TokenSource interface {
	Acquire(ctx context.Context) error
	Release()
}

// TokenSource is a simple token source for initial testing.
type WSTokenSource struct {
	sem *semaphore.Weighted
}

// Acquire acquires an admission token.
func (ts *WSTokenSource) Acquire(ctx context.Context) error {
	return ts.sem.Acquire(ctx, 1)
}

// Release releases an admission token.
func (ts *WSTokenSource) Release() {
	ts.sem.Release(1)
}

func NewWSTokenSource(n int64) TokenSource {
	return &WSTokenSource{semaphore.NewWeighted(n)}
}

type ThrottledSource struct {
	Source
	throttle TokenSource
}

type throttledRunnable struct {
	Runnable
	release func()
}

func (tr *throttledRunnable) Run() error {
	defer tr.release()
	return tr.Runnable.Run()
}

func (ts *ThrottledSource) Next(ctx context.Context) (Runnable, error) {
	err := ts.throttle.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	next, err := ts.Source.Next(ctx)
	if err != nil {
		ts.throttle.Release()
		return nil, err
	}
	return &throttledRunnable{next, ts.throttle.Release}, nil
}

func Throttle(src Source, tokens TokenSource) Source {
	return &ThrottledSource{src, tokens}
}
