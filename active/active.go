// The active package provides code for managing processing of an entire
// directory of task files.
package active

// The "active" task manager supervises launching of workers to process all files
// in a gs:// prefix.  It processes the files in lexographical order, and maintains
// status info in datastore.

// Design:
//  1. a token channel is passed in to ProcessAll, and used to determine how many tasks may
//     be in flight.  It is returned to the caller when there are no more tasks to start,
//     but there may still be tasks running, and tokens that will be returned later.
//  2. a doneHandler waits for task completions, and updates the state.  It starts additional
//     tasks if there are any.  When there are no more tasks, it signals ProcessAll
//     that the token channel may be returned to the caller.

// TODO: Utilization based management:
// The manager start new tasks when either:
//   1. Two tasks have completed since the last task started.
//   2. The 10 second utilization of any single cpu falls below 80%.
//   3. The total 10 second utilization falls below 90%.

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-lab/etl/cloud/gcs"

	"github.com/GoogleCloudPlatform/google-cloud-go-testing/storage/stiface"

	"github.com/m-lab/etl/worker"
)

type DatastoreRecord struct {
	Name string // The prefix for the data source.

	// If the job has to be restarted, it will start with next file after this one.
	CompletedUpTo string

	// This is the next file to be started when there is a worker available.
	Next       string
	UpdateTime time.Time
}

// TaskFile maintains the status of a single file.
// These are NOT thread-safe, and should only be read and modified by
// a single goroutine.
type TaskFile struct {
	// TODO - is this needed?
	path string               // Full path to object.
	obj  *storage.ObjectAttrs // Optional if completed or errored.

	// Status - pending, processing, completed, errored.
	status string // For now - eventually this might be an enum.

	failures int
	lastErr  error
}

// FileSource handles reading, caching, and updating a list of files,
// and tracking the processing status of each file.
type FileSource struct {
	client     stiface.Client
	project    string
	prefix     string
	retryLimit int // number of retries for failed tasks.

	// Channel to handle cleanup when a task is completed.
	done chan *TaskFile

	// Remaining fields should only be accessed by the ProcessAll function.
	// while holding the mutex.
	status DatastoreRecord

	lastUpdate time.Time // Time of last call to UpdatePending
	allFiles   map[string]*TaskFile
	pending    []*TaskFile // Ordered list
	inFlight   map[string]*TaskFile
}

// NewFileSource creates a new source for active processing.
func NewFileSource(sc stiface.Client, project string, prefix string, retryLimit int) (*FileSource, error) {
	files, _, err := gcs.GetFilesSince(context.Background(), sc, project, prefix, time.Time{})
	if err != nil {
		// TODO metric
		return nil, err
	}

	if len(files) < 1 {
		return nil, ErrNoMoreFiles
	}

	fs := FileSource{
		client:     sc,
		project:    project,
		prefix:     prefix,
		status:     DatastoreRecord{Name: prefix},
		pending:    make([]*TaskFile, 0, len(files)),
		inFlight:   make(map[string]*TaskFile, 100),
		retryLimit: retryLimit,
		done:       make(chan *TaskFile, 0)}

	// Don't need to hold the lock yet, because we are sole owner.
	for _, f := range files {
		fs.pending = append(fs.pending, &TaskFile{path: f.Name, obj: f})
	}
	return &fs, nil
}

// updatePending should be called when there are no more pending tasks.
// Not thread-safe - should only be called by ProcessAll.
func (fs *FileSource) updatePending(ctx context.Context) error {
	updateTime := time.Now().Add(-time.Second)
	files, _, err := gcs.GetFilesSince(context.Background(), fs.client, fs.project, fs.prefix, fs.lastUpdate)
	if err != nil {
		return err
	}
	fs.lastUpdate = updateTime

	for _, f := range files {
		if f.Prefix != "" {
			log.Println("Skipping subdirectory:", f.Prefix)
			continue // skip directories
		}
		// Append any new files that aren't found in existing Taskfiles.
		if _, exists := fs.allFiles[f.Name]; !exists {
			tf := TaskFile{path: f.Name, obj: f}
			fs.allFiles[f.Name] = &tf
			fs.pending = append(fs.pending, &tf)
		}
	}

	return nil
}

// next returns the next pending TaskFile.  It runs Update if there
// are initially none available, and reprocesses tasks from the
// errored list if the there are still none pending.
// Caller should have already obtained a semaphore.
func (fs *FileSource) next(ctx context.Context) (*TaskFile, error) {
	if len(fs.pending) == 0 {
		err := fs.updatePending(ctx)
		if err != nil {
			return nil, err
		}
	}
	if len(fs.pending) > 0 {
		tf := fs.pending[0]
		fs.pending = fs.pending[1:]
		return tf, nil
	}
	return nil, nil
}

// complete updates the FileSource to reflect the completion of
// a processing attempt.
// If the processing ends in an error, the task will be moved to
// the end of the pending list, unless the task has already been retried fs.retry times.
func (fs *FileSource) complete(tf *TaskFile) error {
	_, exists := fs.inFlight[tf.path]
	if !exists {
		return errors.New("task not in flight")
	}

	delete(fs.inFlight, tf.path)
	if tf.lastErr != nil {
		if tf.failures < fs.retryLimit {
			fs.pending = append(fs.pending, tf)
			tf.failures++
		}
	}
	return nil
}

var ErrNoMoreFiles = errors.New("No more files in file source")

// process starts the next task.
func (fs *FileSource) process(tf *TaskFile) {
	_, tf.lastErr = worker.ProcessTask(tf.path)
	fs.done <- tf
}

// doneHandler handles all the done channel returns.  For every
// completed task, it will attempt to start two tasks, to utilize
// any additional tokens that become available.
// When there are no further pending items, it decrements the wait group
// to indicate to ProcessAll that no more tokens are needed.
func (fs *FileSource) doneHandler(ctx context.Context, tokens chan struct{}, wg *sync.WaitGroup) {
DRAIN:
	for {
		select {
		case t := <-fs.done:
			fs.complete(t)
			<-tokens // return the token
		case tokens <- struct{}{}:
			tf, err := fs.next(ctx)
			if err != nil {
				log.Println(err)
			}
			if tf != nil {
				fs.process(tf)
			} else {
				// return the token
				<-tokens
				break DRAIN
			}
		}
	}

	wg.Done()
	for t := range fs.done {
		fs.complete(t)
		<-tokens // return the token
		if len(fs.inFlight) == 0 {
			close(fs.done)
		}
	}
}

// ProcessAll iterates through all the TaskFiles, processing each one.
// At the end, it will also make one attempt to reprocess any that failed
// the first time.
func (fs *FileSource) ProcessAll(ctx context.Context, tokens chan struct{}) error {
	if len(fs.pending) == 0 {
		return nil
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	// Handle completed tasks, and start more.
	go fs.doneHandler(ctx, tokens, &wg)

	// Wait until no more pending.
	wg.Wait()

	// Return the semaphore queue for some other FileSource to use.
	return nil

}
