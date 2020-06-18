// Package task provides the tracking of state for a single task pushed by the
// external task queue.
//
// The Task type ...
// TODO(dev) Improve comments and header before merging to dev.
package task

import (
	"context"
	"io"
	"log"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/storage"
)

// Factory provides Get() which always returns a new, complete Task.
// TODO for the defs that stay in factory package, remove ...Factory.
type Factory interface {
	Get(context.Context, etl.DataPath) (*Task, etl.ProcessingError)
}

// DefaultMaxFileSize is the default value for the file size limit for calls to NextTest.
// Larger than this risks an OOM if there are
// multiple large files at on multiple tasks.
// This can be overridden with SetMaxFileSize()
const DefaultMaxFileSize = 200 * 1024 * 1024

// Task contains the state required to process a single task tar file.
// TODO(dev) Add unit tests for meta data.
type Task struct {
	// TestSource and Parser are both embedded, so their interfaces are delegated
	// to the component structs.
	etl.TestSource // Source from which to read tests.
	etl.Parser     // Parser to parse the tests.

	meta        map[string]bigquery.Value // Metadata about this task.
	maxFileSize int64                     // Max file size to avoid OOM.
}

// NewTask constructs a task, injecting the source and the parser.
func NewTask(filename string, src etl.TestSource, prsr etl.Parser) *Task {
	// TODO - should the meta data be a nested type?
	meta := make(map[string]bigquery.Value, 3)
	meta["filename"] = filename
	meta["parse_time"] = time.Now()
	meta["attempt"] = 1
	meta["date"] = src.Date()
	t := Task{
		TestSource:  src,
		Parser:      prsr,
		meta:        meta,
		maxFileSize: DefaultMaxFileSize,
	}
	return &t
}

// Close closes the source and sink.
func (tt *Task) Close() error {
	pErr := tt.Parser.Close()
	tErr := tt.TestSource.Close()
	if pErr != nil {
		return pErr
	}
	return tErr
}

// SetMaxFileSize overrides the default maxFileSize.
func (tt *Task) SetMaxFileSize(max int64) {
	tt.maxFileSize = max
}

// ProcessAllTests loops through all the tests in a tar file, calls the
// injected parser to parse them, and inserts them into bigquery. Returns the
// number of files processed.
// TODO pass in the datatype label.
func (tt *Task) ProcessAllTests() (int, error) {
	if tt.Parser == nil {
		panic("Parser is nil")
	}
	metrics.WorkerState.WithLabelValues(tt.Type(), "task").Inc()
	defer metrics.WorkerState.WithLabelValues(tt.Type(), "task").Dec()
	files := 0
	nilData := 0
	var testname string
	var data []byte
	var err error
	// Read each file from the tar

OUTER:
	for testname, data, err = tt.NextTest(tt.maxFileSize); err != io.EOF; testname, data, err = tt.NextTest(tt.maxFileSize) {
		files++
		if err != nil {
			switch {
			case err == io.EOF:
				break OUTER
			case err == storage.ErrOversizeFile:
				log.Printf("filename:%s testname:%s files:%d, duration:%v err:%v",
					tt.meta["filename"], testname, files,
					time.Since(tt.meta["parse_time"].(time.Time)), err)
				metrics.TestCount.WithLabelValues(
					tt.Type(), "unknown", "oversize file").Inc()
				continue OUTER
			default:
				// We are seeing several of these per hour, a little more than
				// one in one thousand files.  duration varies from 10 seconds
				// up to several minutes.
				// Example:
				// filename:
				// gs://m-lab-sandbox/ndt/2016/04/10/20160410T000000Z-mlab1-ord02-ndt-0002.tgz
				// files:666 duration:1m47.571825351s
				// err:stream error: stream ID 801; INTERNAL_ERROR
				// Because of the break, this error is passed up, and counted at
				// the Task level.
				log.Printf("filename:%s testname:%s files:%d, duration:%v err:%v",
					tt.meta["filename"], testname, files,
					time.Since(tt.meta["parse_time"].(time.Time)), err)

				metrics.TestCount.WithLabelValues(
					tt.Type(), "unknown", "unrecovered").Inc()
				// Since we don't understand these errors, safest thing to do is
				// stop processing the tar file (and task).
				break OUTER
			}
		}
		if data == nil {
			// TODO(dev) Handle directories (expected) and other
			// things separately.
			nilData++
			// If verbose, log the filename that is skipped.
			continue
		}
		kind, parsable := tt.Parser.IsParsable(testname, data)
		if !parsable {
			metrics.FileSizeHistogram.WithLabelValues(
				tt.Type(), kind, "ignored").Observe(float64(len(data)))
			// Don't bother calling ParseAndInsert since this is unparsable.
			continue
		} else {
			metrics.FileSizeHistogram.WithLabelValues(
				tt.Type(), kind, "parsed").Observe(float64(len(data)))
		}
		err = tt.Parser.ParseAndInsert(tt.meta, testname, data)
		// Shouldn't have any of these, as they should be handled in ParseAndInsert.
		if err != nil {
			metrics.TaskCount.WithLabelValues(
				tt.Type(), "ParseAndInsertError").Inc()
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

	// Check if the overall task is OK, or should be rejected.
	if tt.Parser.TaskError() != nil {
		return files, tt.Parser.TaskError()
	}
	return files, flushErr
}
