package worker

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/storage"
	"github.com/m-lab/etl/task"
)

var (
	ErrBadDataType = errors.New("unknown data type")
)

func makeEtlSource(fn string, tableBase string) (*storage.ETLSource, error) {
	client, err := storage.GetStorageClient(false)
	// TODO - add a timer for reading the file.
	tr, err := storage.NewETLSource(client, fn)
	if err != nil {
		return nil, err
		// TODO - anything better we could do here?
	}
	// Label storage metrics with the expected table name.
	tr.TableBase = tableBase
	return tr, nil
}

// ProcessTask interprets a filename to create a Task, Parser, and Inserter,
// and processes the file content.
// Returns an http status code and an error if the task did not complete successfully.
// This was previously a private function in etl_worker.go.
// TODO - add comprehensive unit test??
// TODO - refactor into a Worker struct containing a StorageClient and other similar things, to allow use of fake implementations for testing.
func ProcessTask(fn string) (int, error) {
	path, err := etl.ValidateTestPath(fn)
	if err != nil {
		log.Printf("Invalid filename: %v\n", err)
		return http.StatusBadRequest, err
	}
	tableBase := path.TableBase()

	// Move this into Validate function
	dataType := path.GetDataType()
	if dataType == etl.INVALID {
		metrics.TaskCount.WithLabelValues(tableBase, "worker", "BadRequest").Inc()
		log.Printf("Invalid filename: %s\n", fn)
		return http.StatusBadRequest, ErrBadDataType
	}

	dateFormat := "20060102"
	date, _ := time.Parse(dateFormat, path.PackedDate)
	ins, err := bq.NewInserter(dataType, date)
	if err != nil {
		metrics.TaskCount.WithLabelValues(tableBase, string(dataType), "NewInserterError").Inc()
		log.Printf("Error creating BQ Inserter:  %v", err)
		return http.StatusInternalServerError, err
		// TODO - anything better we could do here?
	}

	tr, err := makeEtlSource(fn, tableBase)
	if err != nil {
		metrics.TaskCount.WithLabelValues(tableBase, "worker", "ServiceUnavailable").Inc()
		log.Printf("Error getting storage client: %v\n", err)
		return http.StatusServiceUnavailable, err
	}
	defer tr.Close()

	return ProcessSource(fn, *path, dataType, tr, ins)
}

// ProcessTaskWithInserter interprets a filename to create a Task, Parser, and Inserter,
// and processes the file content.
// Returns an http status code and an error if the task did not complete successfully.
// TODO - add comprehensive unit test??
func ProcessTaskWithInserter(fn string, ins etl.Inserter) (int, error) {
	path, err := etl.ValidateTestPath(fn)
	if err != nil {
		log.Printf("Invalid filename: %v\n", err)
		return http.StatusBadRequest, err
	}
	tableBase := path.TableBase()

	// Move this into Validate function
	dataType := path.GetDataType()
	if dataType == etl.INVALID {
		metrics.TaskCount.WithLabelValues(tableBase, "worker", "BadRequest").Inc()
		log.Printf("Invalid filename: %s\n", fn)
		return http.StatusBadRequest, ErrBadDataType
	}

	src, err := makeEtlSource(fn, tableBase)
	if err != nil {
		metrics.TaskCount.WithLabelValues(tableBase, "worker", "ServiceUnavailable").Inc()
		log.Printf("Error getting storage client: %v\n", err)
		return http.StatusServiceUnavailable, err
	}
	defer src.Close()

	return ProcessSource(fn, *path, dataType, src, ins)
}

// ProcessSource allows injection of arbitrary etlSource and inserter.
// TODO - add test with fake source and inserter.
func ProcessSource(fn string, path etl.DataPath, dt etl.DataType, tr *storage.ETLSource, ins etl.Inserter) (int, error) {
	tableBase := path.TableBase()
	// Count number of workers operating on each table.
	metrics.WorkerCount.WithLabelValues(tableBase).Inc()
	defer metrics.WorkerCount.WithLabelValues(tableBase).Dec()

	// These keep track of the (nested) state of the worker.
	metrics.WorkerState.WithLabelValues(tableBase, "worker").Inc()
	defer metrics.WorkerState.WithLabelValues(tableBase, "worker").Dec()

	dateFormat := "20060102"
	date, _ := time.Parse(dateFormat, path.PackedDate)

	// Create parser, injecting Inserter
	p := parser.NewParser(dt, ins)
	if p == nil {
		metrics.TaskCount.WithLabelValues(tableBase, string(dt), "NewInserterError").Inc()
		log.Printf("Error creating parser for %s", dt)
		return http.StatusInternalServerError, fmt.Errorf("problem creating parser for %s", dt)
	}
	tsk := task.NewTask(fn, tr, p)

	files, err := tsk.ProcessAllTests()

	// Count the files processed per-host-module per-weekday.
	// TODO(soltesz): evaluate separating hosts and pods as separate metrics.
	metrics.FileCount.WithLabelValues(
		path.Host+"-"+path.Site+"-"+path.Experiment,
		date.Weekday().String()).Add(float64(files))

	metrics.WorkerState.WithLabelValues(path.TableBase(), "finish").Inc()
	defer metrics.WorkerState.WithLabelValues(path.TableBase(), "finish").Dec()
	if err != nil {
		metrics.TaskCount.WithLabelValues(path.TableBase(), string(dt), "TaskError").Inc()
		log.Printf("Error Processing Tests:  %v", err)
		// NOTE: This may cause indefinite retries, and stalled task queue.  Task will eventually
		// expire, but it might be better to have a different mechanism for retries, particularly
		// for gardener, which waits for empty task queue.
		return http.StatusInternalServerError, err
		// TODO - anything better we could do here?
	}

	metrics.TaskCount.WithLabelValues(tableBase, string(dt), "OK").Inc()
	return http.StatusOK, nil
}
