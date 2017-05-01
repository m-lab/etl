// Package task provides the tracking of state for a single task pushed by the
// external task queue.
//
// The Task type ...
// TODO(dev) Improve comments and header before merging to dev.
package task

import (
	"io"
	"log"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/storage"
)

// TODO(dev) Add unit tests for meta data.
type Task struct {
	*storage.ETLSource                           // Source from which to read tests.
	etl.Parser                                   // Parser to parse the tests.
	etl.Inserter                                 // provides InsertRows(...)
	meta               map[string]bigquery.Value // Metadata about this task.
}

// NewTask constructs a task, injecting the source and the parser.
func NewTask(filename string, src *storage.ETLSource, prsr etl.Parser, inserter etl.Inserter) *Task {
	// TODO - should the meta data be a nested type?
	meta := make(map[string]bigquery.Value, 3)
	meta["filename"] = filename
	meta["parse_time"] = time.Now()
	meta["attempt"] = 1
	t := Task{src, prsr, inserter, meta}
	return &t
}

// ProcessAllTests loops through all the tests in a tar file, calls the
// injected parser to parse them, and inserts them into bigquery (not yet implemented).
func (tt *Task) ProcessAllTests() error {
	metrics.WorkerState.WithLabelValues("process").Inc()
	defer metrics.WorkerState.WithLabelValues("process").Dec()
	files := 0
	nilData := 0
	// Read each file from the tar
	for testname, data, err := tt.NextTest(); err != io.EOF; testname, data, err = tt.NextTest() {
		files += 1
		if err != nil {
			if err == io.EOF {
				break
			}
			// TODO(dev) Handle this error properly!
			log.Printf("filename:%s files:%d duration:%v err:%v",
				tt.meta["filename"], files, time.Since(tt.meta["parse_time"].(time.Time)), err)
			break
		}
		if data == nil {
			// TODO(dev) Handle directories (expected) and other
			// things separately.
			nilData += 1
			// If verbose, log the filename that is skipped.
			continue
		}

		err := tt.Parser.ParseAndInsert(tt.meta, testname, data)
		if err != nil {
			log.Printf("%v", err)
			// TODO(dev) Handle this error properly!
			return err
		}
	}

	// Flush any rows cached in the inserter.
	err := tt.Flush()

	if err != nil {
		log.Printf("%v", err)
	}
	// TODO - make this debug or remove
	log.Printf("%d files, %d nil data, %d rows", files, nilData, tt.Count())
	return err
}
