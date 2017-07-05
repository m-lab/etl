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

// Impose 200MB max size for a single file.  Larger than this risks an OOM if there are
// multiple large files at on multiple tasks.
const MAX_FILE_SIZE = 200000000

// TODO(dev) Add unit tests for meta data.
type Task struct {
	// ETLSource and Parser are both embedded, so their interfaces are delegated
	// to the component structs.
	*storage.ETLSource // Source from which to read tests.
	etl.Parser         // Parser to parse the tests.

	meta map[string]bigquery.Value // Metadata about this task.
}

// NewTask constructs a task, injecting the source and the parser.
func NewTask(filename string, src *storage.ETLSource, prsr etl.Parser) *Task {
	// TODO - should the meta data be a nested type?
	meta := make(map[string]bigquery.Value, 3)
	meta["filename"] = filename
	meta["parse_time"] = time.Now()
	meta["attempt"] = 1
	t := Task{src, prsr, meta}
	return &t
}

// ProcessAllTests loops through all the tests in a tar file, calls the
// injected parser to parse them, and inserts them into bigquery. Returns the
// number of files processed.
func (tt *Task) ProcessAllTests() (int, error) {
	metrics.WorkerState.WithLabelValues("task").Inc()
	defer metrics.WorkerState.WithLabelValues("task").Dec()
	files := 0
	nilData := 0
	var testname string
	var data []byte
	var err error
	// Read each file from the tar

	for testname, data, err = tt.NextTest(MAX_FILE_SIZE); err != io.EOF; testname, data, err = tt.NextTest(MAX_FILE_SIZE) {
		files++
		if err != nil {
			if err == io.EOF {
				break
			}
			// We are seeing several of these per hour, a little more than
			// one in one thousand files.  duration varies from 10 seconds up to several
			// minutes.
			// Example:
			// filename:gs://m-lab-sandbox/ndt/2016/04/10/20160410T000000Z-mlab1-ord02-ndt-0002.tgz
			// files:666 duration:1m47.571825351s
			// err:stream error: stream ID 801; INTERNAL_ERROR
			// Because of the break, this error is passed up, and counted at the Task level.
			log.Printf("filename:%s testname:%s files:%d, duration:%v err:%v",
				tt.meta["filename"], testname, files,
				time.Since(tt.meta["parse_time"].(time.Time)), err)

			metrics.TestCount.WithLabelValues(
				tt.Parser.TableName(), "unknown", "unrecovered").Inc()
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
		// Shouldn't have any of these, as they should be handled in ParseAndInsert.
		if err != nil {
			metrics.TaskCount.WithLabelValues(
				"Task", "ParseAndInsertError").Inc()
			log.Printf("%v", err)
			// TODO(dev) Handle this error properly!
			continue
		}
	}

	// Flush any rows cached in the inserter.
	flushErr := tt.Flush()

	if flushErr != nil {
		log.Printf("%v", flushErr)
	}
	// TODO - make this debug or remove
	log.Printf("Processed %d files, %d nil data, %d rows committed, %d failed, from %s into %s",
		files, nilData, tt.Parser.Committed(), tt.Parser.Failed(),
		tt.meta["filename"], tt.Parser.FullTableName())
	// Return the file count, and the terminal error, if other than EOF.
	if err != io.EOF {
		return files, err
	}
	return files, nil
}
