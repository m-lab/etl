// The active package provides code for managing processing of an entire
// directory of task files.
package active

// The "active" task manager supervises launching of workers to process all files
// in a gs:// prefix.  It processes the files in lexographical order, and maintains
// status info in datastore.

// TODO: Utilization based management:
// The manager start new tasks when either:
//   1. Two tasks have completed since the last task started.
//   2. The 10 second utilization of any single cpu falls below 80%.
//   3. The total 10 second utilization falls below 90%.

import (
	"context"
	"errors"
	"sync"
	"time"
	"log"

	"github.com/GoogleCloudPlatform/google-cloud-go-testing/storage/stiface"
	"github.com/m-lab/etl/cloud/gcs"

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
	path string

	// Status - pending, processing, completed, errored.
	status string // For now - eventually this might be an enum.

	failures int
	lastErr  error
}

// FileSource handles reading, caching, and updating a list of files,
// and tracking the processing status of each file.
type FileSource struct {
	mutex  sync.Mutex
	project string
	prefix string

	pending   []*TaskFile // Ordered list
	inFlight  map[string]*TaskFile
	completed []*TaskFile // Ordered list.
	retry     []*TaskFile

	retries int // number of retries for failed tasks.

	// Semaphore used to control number of active tasks.
	sem chan struct{}
}

func NewFileSource(sem chan struct{}, sc stiface.Client, project string, prefix string) *FileSource {
	pending, _, err := gcs.GetAllFiles(context.Background(), sc, project, prefix)
	if err != nil {
		// TODO metric
		log.Println(err)
		return nil
	}

	return &FileSource{status: DatastoreRecord{}, 
	pending: pending, inFlight: map[string]*TaskFile, len(sem),
	sem: sem}
}

// UpdatePending should be called when there are no more pending tasks.
func (fs *FileSource) UpdatePending(ctx context.Context) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	// Can we use mtime to streamline update?
	// TODO

	return nil
}

// Next returns the next pending TaskFile.  It runs Update if there
// are initially none available, and reprocesses tasks from the
// errored list if the there are still none pending.
// Caller should have already obtained a semaphore.
func (fs *FileSource) Next() (*TaskFile, error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()
	if len(fs.pending) < 1 {
		err := fs.UpdatePending(context.Background())
		if err != nil {
			return nil, err
		}
	}
	if len(fs.pending) > 0 {
		tf := fs.pending[0]
		fs.pending = fs.pending[1:]
		return tf, nil
	}
	if len(fs.retry) > 0 {
		tf := fs.retry[0]
		fs.retry = fs.retry[1:]
		return tf, nil
	}
	return nil, nil
}

// CompleteTask updates the FileSource to reflect the completion of
// a processing attempt.
// If the processing ends in an error, the task will be moved to
// the retry list, unless the task has already been retried fs.retry times.
func (fs *FileSource) CompleteTask(tf *TaskFile) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()
	defer func() {
		fs.sem <- struct{}{}
	}()

	_, exists := fs.inFlight[tf.path]
	if !exists {
		return errors.New("task not in flight")
	}

	delete(fs.inFlight, tf.path)
	if tf.lastErr != nil {
		if tf.failures < 1 {
			fs.retry = append(fs.retry, tf)
			tf.failures++
		}
	} else {
		fs.completed = append(fs.completed, tf)
	}
	return nil
}

var ErrNoMoreFiles = errors.New("No more files in file source")

// Process starts the next task.  For concurrent processing, use go StartNext().
// This function must NOT hold the mutex, and should call other thread-safe functions
// for all accesses to FileSource fields.
func (fs *FileSource) ProcessOne(tf *TaskFile) error {
	// next is now owned.
	_, tf.lastErr = worker.ProcessTask(tf.path)

	return fs.CompleteTask(tf) // This also returns the semaphore.
}

// ProcessAll iterates through all the TaskFiles, processing each one.
// At the end, it will also make one attempt to reprocess any that failed
// the first time.
func (fs *FileSource) ProcessAll() (chan struct{}, error) {

	for {
		next, err := fs.Next()
		if err != nil {
			return fs.sem, err
		}
		if next == nil {
			break
		}
		// Asynchronously process this task.
		// The semaphore will be returned when task completes.
		go fs.ProcessOne(next)
	}

	// Return the semaphore queue for some other FileSource to use.
	return fs.sem, nil
}
