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

// ProcessTask interprets a filename to create a Task, Parser, and Inserter,
// and processes the file content.
// Returns an http status code and an error if the task did not complete successfully.
// This was previously a private function in etl_worker.go.
// TODO - add comprehensive unit test??
func ProcessTask(fn string) (int, error) {
	data, err := etl.ValidateTestPath(fn)
	if err != nil {
		log.Printf("Invalid filename: %v\n", err)
		return http.StatusBadRequest, err
	}

	// Count number of workers operating on each table.
	metrics.WorkerCount.WithLabelValues(data.TableBase()).Inc()
	defer metrics.WorkerCount.WithLabelValues(data.TableBase()).Dec()

	// These keep track of the (nested) state of the worker.
	metrics.WorkerState.WithLabelValues(data.TableBase(), "worker").Inc()
	defer metrics.WorkerState.WithLabelValues(data.TableBase(), "worker").Dec()

	// Move this into Validate function
	dataType := data.GetDataType()
	if dataType == etl.INVALID {
		metrics.TaskCount.WithLabelValues(data.TableBase(), "worker", "BadRequest").Inc()
		log.Printf("Invalid filename: %s\n", fn)
		return http.StatusBadRequest, ErrBadDataType
	}

	client, err := storage.GetStorageClient(false)
	if err != nil {
		metrics.TaskCount.WithLabelValues(data.TableBase(), "worker", "ServiceUnavailable").Inc()
		log.Printf("Error getting storage client: %v\n", err)
		return http.StatusServiceUnavailable, err
	}

	// TODO - add a timer for reading the file.
	tr, err := storage.NewETLSource(client, fn)
	if err != nil {
		metrics.TaskCount.WithLabelValues(data.TableBase(), string(dataType), "ETLSourceError").Inc()
		log.Printf("Error opening gcs file: %v", err)
		return http.StatusInternalServerError, err
		// TODO - anything better we could do here?
	}
	defer tr.Close()
	// Label storage metrics with the expected table name.
	tr.TableBase = data.TableBase()

	dateFormat := "20060102"
	date, err := time.Parse(dateFormat, data.PackedDate)

	ins, err := bq.NewInserter(dataType, date)
	if err != nil {
		metrics.TaskCount.WithLabelValues(data.TableBase(), string(dataType), "NewInserterError").Inc()
		log.Printf("Error creating BQ Inserter:  %v", err)
		return http.StatusInternalServerError, err
		// TODO - anything better we could do here?
	}

	// Create parser, injecting Inserter
	p := parser.NewParser(dataType, ins)
	if p == nil {
		metrics.TaskCount.WithLabelValues(data.TableBase(), string(dataType), "NewInserterError").Inc()
		log.Printf("Error creating parser for %s", dataType)
		return http.StatusInternalServerError, fmt.Errorf("problem creating parser for %s", dataType)
	}
	tsk := task.NewTask(fn, tr, p)

	files, err := tsk.ProcessAllTests()

	// Count the files processed per-host-module per-weekday.
	// TODO(soltesz): evaluate separating hosts and pods as separate metrics.
	metrics.FileCount.WithLabelValues(
		data.Host+"-"+data.Site+"-"+data.Experiment,
		date.Weekday().String()).Add(float64(files))

	metrics.WorkerState.WithLabelValues(data.TableBase(), "finish").Inc()
	defer metrics.WorkerState.WithLabelValues(data.TableBase(), "finish").Dec()
	if err != nil {
		metrics.TaskCount.WithLabelValues(data.TableBase(), string(dataType), "TaskError").Inc()
		log.Printf("Error Processing Tests:  %v", err)
		// NOTE: This may cause indefinite retries, and stalled task queue.  Task will eventually
		// expire, but it might be better to have a different mechanism for retries, particularly
		// for gardener, which waits for empty task queue.
		return http.StatusInternalServerError, err
		// TODO - anything better we could do here?
	}

	metrics.TaskCount.WithLabelValues(data.TableBase(), string(dataType), "OK").Inc()
	return http.StatusOK, nil
}
