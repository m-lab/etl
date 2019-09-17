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

// TODO:
// A. Add metrics
//
// B. Recovery and monitoring using datastore.
//
// C. Utilization based management:
//    The manager starts new tasks when either:
//   1. Two tasks have completed since the last task started.
//   2. The 10 second utilization of any single cpu falls below 80%.
//   3. The total 10 second utilization falls below 90%.

import (
	"context"
	"errors"
	"fmt"
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

	failures int
	lastErr  error
}

func (tf TaskFile) String() string {
	return fmt.Sprintf("%s: %d failures, %v", tf.path, tf.failures, tf.lastErr)
}

// Path returns the full path to the file.
func (tf TaskFile) Path() string {
	return tf.path
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
	status DatastoreRecord

	lastUpdate time.Time // Time of last call to UpdatePending
	allFiles   map[string]*TaskFile
	pending    []*TaskFile // Ordered list - TODO make this a channel?
	inFlight   map[string]*TaskFile

	// The function that processes each task.
	process func(*TaskFile) error

	err []*TaskFile
}

// NewFileSource creates a new source for active processing.
func NewFileSource(sc stiface.Client, project string, prefix string, retryLimit int, processFunc func(*TaskFile) error) (*FileSource, error) {
	fs := FileSource{
		client:     sc,
		project:    project,
		prefix:     prefix,
		status:     DatastoreRecord{Name: prefix},
		pending:    make([]*TaskFile, 0, 1),
		allFiles:   make(map[string]*TaskFile, 100),
		inFlight:   make(map[string]*TaskFile, 100),
		retryLimit: retryLimit,
		done:       make(chan *TaskFile, 0),
		process:    processFunc,
		err:        make([]*TaskFile, 0, 10),
	}

	return &fs, nil
}

// Errors returns a list of all TaskFile objects that ended with error.
func (fs *FileSource) Errors() []*TaskFile {
	return fs.err
}

// updatePending should be called when there are no more pending tasks.
// Not thread-safe - should only be called by ProcessAll.
func (fs *FileSource) updatePending(ctx context.Context) error {
	// Allow for a little clock skew.
	updateTime := time.Now().Add(-time.Second)
	files, _, err := gcs.GetFilesSince(context.Background(), fs.client, fs.project, fs.prefix, fs.lastUpdate)
	if err != nil {
		return err
	}
	fs.lastUpdate = updateTime

	if len(fs.pending) == 0 && cap(fs.pending) < len(files) {
		fs.pending = make([]*TaskFile, 0, len(files))
	}
	for _, f := range files {
		if f.Prefix != "" {
			log.Println("Skipping subdirectory:", f.Prefix)
			continue // skip directories
		}
		// Append any new files that aren't found in existing Taskfiles.
		if _, exists := fs.allFiles[f.Name]; !exists {
			log.Println("Adding", "gs://"+f.Bucket+"/"+f.Name)
			tf := TaskFile{path: "gs://" + f.Bucket + "/" + f.Name, obj: f}
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
// Returns an error iff updatePending errored.
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

// ErrTaskNotFound is returned if an inflight task is not found in inFlight.
var ErrTaskNotFound = errors.New("task not found")

// NOT THREAD-SAFE.  Should be called only by ProcessAll (through doneHandler)
// updateState updates the FileSource to reflect the completion of
// a processing attempt.
// If the processing ends in an error, the task will be moved to
// the end of the pending list, unless the task has already been retried fs.retry times.
func (fs *FileSource) updateState(tf *TaskFile) error {
	_, exists := fs.inFlight[tf.path]
	if !exists {
		log.Println("Did not find", tf.path)
		return ErrTaskNotFound
	}

	delete(fs.inFlight, tf.path)
	if tf.lastErr != nil {
		if tf.failures < fs.retryLimit {
			fs.pending = append(fs.pending, tf)
			tf.failures++
		} else {
			fs.err = append(fs.err, tf)
		}
	}

	return tf.lastErr
}

func processTask(tf *TaskFile) error {
	_, err := worker.ProcessTask(tf.path)
	return err
}

// doneHandler handles all the done channel returns.
// It returns when there are no more pending tasks,
// When all items have been processed, it closes the done channel.
func (fs *FileSource) doneHandler(ctx context.Context, tokens chan struct{}, wg *sync.WaitGroup) {
PROCESS:
	for {
		select {
		case tf := <-fs.done:
			err := fs.updateState(tf) // This removes the task from inFlight.
			if err != nil {
				log.Println(tf.Path(), err)
			}
			<-tokens // return the token
		case tokens <- struct{}{}:
			tf, err := fs.next(ctx)
			if err != nil {
				log.Println(err)
			}
			if tf == nil {
				// return the token
				<-tokens
				break PROCESS
			}

			// This adds the task to inFlight.
			fs.inFlight[tf.path] = tf
			go func() {
				tf.lastErr = fs.process(tf)
				fs.done <- tf
			}()
		}
	}

	if wg != nil {
		wg.Done() // Signal that there are no more pending tasks.
	}

	// Now drain any remaining tasks in flight.
	if len(fs.inFlight) > 0 {
		for t := range fs.done {
			fs.updateState(t) // This removes the task from inFlight.
			<-tokens          // return the token
			if len(fs.inFlight) == 0 {
				break
			}
		}
	}
	close(fs.done)
}

// ProcessAll iterates through all the TaskFiles, processing each one.
// It may also retry any that failed the first time.
func (fs *FileSource) ProcessAll(ctx context.Context, tokens chan struct{}, wg *sync.WaitGroup) error {
	err := fs.updatePending(ctx)
	if err != nil {
		return err
	}
	// Handle tasks in parallel.
	// When this returns, there are still tasks in flight, but no more
	// will be started.
	go fs.doneHandler(ctx, tokens, wg)

	return nil
}
