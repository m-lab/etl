// Package active provides code for managing processing of an entire
// directory of task files.
package active

// GCSSource iterates over a gs:// prefix to produce Runnables.
//
// TODO: Throttle combines a source of Runnables with a mechanism for blocking the Next() function

// TODO:
// A. Add metrics
// B. Add retry
// C. Recovery and monitoring using datastore.
import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/google-cloud-go-testing/storage/stiface"
	"google.golang.org/api/iterator"

	"cloud.google.com/go/storage"
	"github.com/m-lab/etl/cloud/gcs"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/go/logx"
)

var debug = logx.Debug

/* Discussion:
 What should happen if:
	storage context expires, or is canceled?
		Next() will return context error.
	storage errors?
		Next() will return any objects iterated prior to error.
		Next() will then return the error.

   streamToPending should run to completion unless the context expires,
   or there is a persistent gs error.
*/

// FileLister defines a function that returns a list of storage.ObjectAttrs.
type FileLister func(ctx context.Context) ([]*storage.ObjectAttrs, int64, error)

// FileListerFunc creates a function that returns a slice of *storage.ObjectAttrs.
// On certain GCS errors, it may return partial result and an error.
func FileListerFunc(sc stiface.Client, prefix string) FileLister {
	return func(ctx context.Context) ([]*storage.ObjectAttrs, int64, error) {
		return gcs.GetFilesSince(ctx, sc, prefix, time.Time{})
	}
}

// Context implements context.Context, but allows injection of an alternate Err().
type Context struct {
	context.Context
	cancel   func()
	lock     sync.Mutex
	otherErr error // Should access with atomics.
}

// Err returns nil, otherErr, or Context.Err()
func (c *Context) Err() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	select {
	case <-c.Done():
		if c.otherErr != nil {
			return c.otherErr
		}
		return c.Context.Err()
	default:
		return nil
	}
}

// Fail cancels the context, and sets the result of context.Err()
func (c *Context) Fail(err error) {
	debug.Output(2, "stopping")
	c.lock.Lock()
	defer c.lock.Unlock()
	c.otherErr = err
	c.cancel()
}

// WithFail wraps a context to allow specifying custom error with Fail()
func WithFail(ctx context.Context) *Context {
	ctx, cancel := context.WithCancel(ctx)
	return &Context{Context: ctx, cancel: cancel}
}

// GCSSource implements RunnableSource for a GCS bucket/prefix.
type GCSSource struct {
	ctx *Context
	// The fileLister produces the list of source files.
	fileLister FileLister
	// toRunnable creates a Runnable from ObjectAttrs.
	toRunnable func(*storage.ObjectAttrs) Runnable

	pendingChan chan Runnable
}

// NewGCSSource creates a new source for active processing.
func NewGCSSource(ctx context.Context, fl FileLister, toRunnable func(*storage.ObjectAttrs) Runnable) (*GCSSource, error) {
	fs := GCSSource{
		ctx:        WithFail(ctx),
		fileLister: fl,
		toRunnable: toRunnable,

		pendingChan: make(chan Runnable, 0),
	}

	go fs.streamToPending(ctx)

	return &fs, nil
}

// CancelStreaming terminates the streaming goroutine context.
func (fs *GCSSource) CancelStreaming() {
	fs.ctx.cancel()
}

// Next implements RunnableSource.  It returns
//    the next pending job to run, OR
//    iterator.Done OR
//    ctx.Err() OR
//    gcs error
func (fs *GCSSource) Next(ctx context.Context) (Runnable, error) {
	select {
	// This allows exit if pendingChan is blocking.
	case <-ctx.Done():
		return nil, ctx.Err()
	case next, ok := <-fs.pendingChan:
		// This ensures that context expirations and errors are respected.
		select {
		case <-ctx.Done():
			log.Println("Next timeout")
			metrics.ActiveErrors.WithLabelValues("next timeout").Inc()
			return nil, ctx.Err()
		case <-fs.ctx.Done():
			log.Println(fs.ctx.Err())
			return nil, fs.ctx.Err()
		default:
			if ok {
				return next, nil
			}
			debug.Println("iterator.Done")
			return nil, iterator.Done
		}
	}
}

// streamToPending feeds tasks to the pending queue until all files have been submitted.
// It fetches the list of files once, then converts files to Runnables until all files are
// handled, or the context is canceled or expires.
// The Runnables are pulled from the queue by Next().
func (fs *GCSSource) streamToPending(ctx context.Context) {
	// No matter what else happens, we eventually want to close the pendingChan.
	defer close(fs.pendingChan)

	files, _, err := fs.fileLister(ctx)
	if err != nil {
		log.Println("Error streaming", err)
		metrics.ActiveErrors.WithLabelValues("filelister").Inc()
		fs.ctx.Fail(err)
		return
	}

	for _, f := range files {
		debug.Println(f)
		if f == nil {
			log.Println("Nil file!!")
			metrics.ActiveErrors.WithLabelValues("nil file").Inc()
			continue
		}
		// We abandon remaining items if context expires.
		if ctx.Err() != nil {
			metrics.ActiveErrors.WithLabelValues("streamToPending").Inc()
			break
		}
		debug.Printf("Adding gs://%s/%s", f.Bucket, f.Name)
		// Blocks until consumer reads channel.
		fs.pendingChan <- fs.toRunnable(f)
	}
}
