// Package active provides code for managing processing of an entire
// directory of task files.
package active

// The "active" task manager supervises launching of workers to process all files
// in a gs:// prefix.  It processes the files in lexographical order, and maintains
// status info in datastore.

// Runnable is a func() error that can be run by a dispatching system.
//
// FileSource iterates over a gs:// prefix to produce Runnables.
//
// TODO: Throttle combines a source of Runnables with a mechanism for blocking the next() function

// TODO:
// A. Add metrics
// B. Add retry
// C. Recovery and monitoring using datastore.
import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/google-cloud-go-testing/storage/stiface"
	"google.golang.org/api/iterator"

	"cloud.google.com/go/storage"
	"github.com/m-lab/etl/cloud/gcs"
	"github.com/m-lab/go/logx"
)

var debug = logx.Debug

/* Discussion:
 Challenges:
	Since we are resubmitting tasks on the pending channel, we shouldn't close it until all tasks have completed.

 What should happen if:
	storage context expires?
		Next() will return context error.
		pending channel will be abandoned.
	storage errors?
		Next() will return any items already in pending.
		Next() will then return the error.
	we want to terminate RunAll before it completes?

   RunAll should run to completion unless there is a persistent gs error.
*/

var (
	// ErrShouldRetry should be returned by Runnable if a task should be retried.
	ErrShouldRetry = errors.New("Should retry")
)

// Runnable is just a function that does something and returns an error.
// A Runnable may return ErrShouldRetry if there was a non-persistent error.
// TODO - should this instead be and interface, with Run() and ShouldRetry()?
type Runnable = func() error

// RunnableSource provides a Next function that returns Next functions.
type RunnableSource interface {
	// Next should return iterator.Done when there are no more Runnables.
	// It may block if there are no more runnables available right now,
	// (or if throttling is applied)
	Next(ctx context.Context) (Runnable, error)
}

// Task maintains the status of a single file.
// These are NOT thread-safe, and should only be read and modified by
// a single goroutine.
type Task struct {
	path string               // Full path to object.
	obj  *storage.ObjectAttrs // Optional if completed or errored.
}

// FileLister defines a function that returns a list of storage.ObjectAttrs.
type FileLister func(ctx context.Context, since time.Time) ([]*storage.ObjectAttrs, int64, error)

// FileListerFunc creates a function that returns a slice of *storage.ObjectAttrs.
func FileListerFunc(sc stiface.Client, project string, prefix string) func(ctx context.Context, since time.Time) ([]*storage.ObjectAttrs, int64, error) {
	return func(ctx context.Context, since time.Time) ([]*storage.ObjectAttrs, int64, error) {
		return gcs.GetFilesSince(ctx, sc, project, prefix, since)
	}
}

// FileSource handles reading, caching, and updating a list of files,
// It implements RunnableSource
type FileSource struct {
	fileLister func(ctx context.Context, since time.Time) ([]*storage.ObjectAttrs, int64, error)
	runFunc    func(*Task) Runnable

	pendingChan chan Runnable

	cancel    func()        // Function to cancel the streaming feeder.
	done      chan struct{} // streaming func terminated
	doneState error         // streaming func final state.
}

// NewFileSource creates a new source for active processing.
func NewFileSource(ctx context.Context, fl FileLister, queueSize int, runFunc func(*Task) Runnable) (*FileSource, error) {
	ctx, cancel := context.WithCancel(ctx)
	fs := FileSource{
		fileLister: fl,
		runFunc:    runFunc,

		pendingChan: make(chan Runnable, queueSize),
		cancel:      cancel,
		done:        make(chan struct{}),
	}

	go fs.streamToPending(ctx)

	return &fs, nil
}

// CancelStreaming terminates the streaming goroutine context.
func (fs *FileSource) CancelStreaming() {
	fs.cancel()
}

func (fs *FileSource) stop(err error) {
	debug.Output(2, "stopping")
	select {
	case <-fs.done:
	default:
		fs.doneState = err
		close(fs.done)
	}
}

// streamToPending feeds tasks to the pending queue until all files have been submitted.
// It fetches the list of files repeatedly until there are no new files, or until the context is canceled or expires.
// The tasks are pulled from the queue by Next().
func (fs *FileSource) streamToPending(ctx context.Context) {
	// Files submitted to pending.  May not have been completed.
	submitted := make(map[string]struct{}, 100)
	lastUpdate := time.Time{} // Update time of last observed file.

	for {
		files, _, err := fs.fileLister(ctx, lastUpdate)
		if err != nil {
			debug.Println("Error streaming", err)
			if ctx.Err() != nil {
				fs.stop(ctx.Err())
				return
			}
			// TODO count errors and abort?
			continue // Retry.
		}

		if len(files) == 0 {
			close(fs.pendingChan)
			return
		}

		for _, f := range files {
			if ctx.Err() != nil {
				fs.stop(ctx.Err())
				return
			}
			if f.Prefix != "" {
				debug.Println("Skipping subdirectory:", f.Prefix)
				continue // skip directories
			}
			// Append any new files that haven't already been dispatched.
			if _, alreadySeen := submitted[f.Name]; !alreadySeen {
				// Find last update time, for use next time.
				if f.Updated.After(lastUpdate) {
					lastUpdate = f.Updated
				}

				tf := Task{path: "gs://" + f.Bucket + "/" + f.Name, obj: f}
				debug.Println("Adding", tf.path)
				submitted[f.Name] = struct{}{}
				fs.pendingChan <- fs.runFunc(&tf)
			}
		}
	}
}

// Next implements RunnableSource.  It returns
//    the next pending job to run, OR
//    iterator.Done OR
//    ctx.Err() OR
//    other error OR
func (fs *FileSource) Next(ctx context.Context) (Runnable, error) {
	select {
	// Check done states first.  No more than one value will sneak through.
	case <-ctx.Done():
		debug.Println("early", ctx.Err())
		return nil, ctx.Err()
	case <-fs.done:
		debug.Println("internally detected", fs.doneState)
		return nil, fs.doneState
	case next, ok := <-fs.pendingChan:
		if !ok {
			debug.Println("iterator.Done")
			return nil, iterator.Done
		}
		// Check again whether something expired
		select {
		case <-ctx.Done():
			debug.Println("late", ctx.Err())
			return nil, ctx.Err()
		case <-fs.done:
			debug.Println("internally detected", fs.doneState)
			return nil, fs.doneState
		default:
			debug.Println("normal")
			return next, nil
		}
	}
}

// RunAll will execute functions provided by Next() until there are no more,
// or the context is canceled.
// It will also retry the Runnable when indicated by ErrShouldRetry.
// It returns an errgroup.Group.
func (fs *FileSource) RunAll(ctx context.Context) {
	wg := sync.WaitGroup{}
	for {
		run, err := fs.Next(ctx)
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
