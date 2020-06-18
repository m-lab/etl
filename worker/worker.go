package worker

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	gcs "cloud.google.com/go/storage"

	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/factory"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/storage"
	"github.com/m-lab/etl/task"
)

// GetSource gets the TestSource for the filename.
// fn is a gs:// GCS uri.
func GetSource(client *gcs.Client, uri string) (etl.TestSource, etl.DataPath, int, error) {
	path, err := etl.ValidateTestPath(uri)
	label := path.TableBase() // On error, this will be "invalid", so not all that useful.
	if err != nil {
		metrics.TaskCount.WithLabelValues(label, "InvalidFilename").Inc()
		log.Printf("Invalid filename: %v\n", err)
		return nil, etl.DataPath{}, http.StatusBadRequest, err
	}

	dataType := path.GetDataType()
	// Can this be merged with error case above?
	if dataType == etl.INVALID {
		metrics.TaskCount.WithLabelValues(string(dataType), "SourcePathError").Inc()
		log.Printf("Invalid datatype: %s", path)
		return nil, etl.DataPath{}, http.StatusInternalServerError, err
	}
	tr, err := storage.NewTestSource(client, path, label)
	if err != nil {
		metrics.TaskCount.WithLabelValues(string(dataType), "ETLSourceError").Inc()
		log.Printf("Error opening gcs file: %v", err)
		return nil, etl.DataPath{}, http.StatusInternalServerError, err
		// TODO - anything better we could do here?
	}
	return tr, path, http.StatusOK, nil
}

// ProcessTask interprets a filename to create a Task, Parser, and Inserter,
// and processes the file content.  Storage client is implicitly obtained
// from GetStorageClient.
// Returns an http status code and an error if the task did not complete
// successfully.
// DEPRECATED - should migrate to ProcessGKETask.
func ProcessTask(fn string) (int, error) {
	client, err := storage.GetStorageClient(false)
	if err != nil {
		path, _ := etl.ValidateTestPath(fn)
		metrics.TaskCount.WithLabelValues(path.DataType, "ServiceUnavailable").Inc()
		log.Printf("Error getting storage client: %v\n", err)
		return http.StatusServiceUnavailable, err
	}
	return ProcessTaskWithClient(client, fn)
}

// ProcessTaskWithClient handles processing with an injected client.
func ProcessTaskWithClient(client *gcs.Client, fn string) (int, error) {
	tr, path, status, err := GetSource(client, fn)
	if err != nil {
		return status, err
	}
	defer tr.Close()

	return ProcessTestSource(tr, path)
}

// ProcessTestSource handles processing of all TestSource contents.
func ProcessTestSource(src etl.TestSource, path etl.DataPath) (int, error) {
	label := path.TableBase() // This works even on error?

	// Count number of workers operating on each table.
	metrics.WorkerCount.WithLabelValues(label).Inc()
	defer metrics.WorkerCount.WithLabelValues(label).Dec()

	// These keep track of the (nested) state of the worker.
	metrics.WorkerState.WithLabelValues(label, "worker").Inc()
	defer metrics.WorkerState.WithLabelValues(label, "worker").Dec()

	dataType := path.GetDataType()

	dateFormat := "20060102"
	date, err := time.Parse(dateFormat, path.PackedDate)

	ins, err := bq.NewInserter(dataType, date)
	if err != nil {
		metrics.TaskCount.WithLabelValues(label, string(dataType), "NewInserterError").Inc()
		log.Printf("Error creating BQ Inserter:  %v", err)
		return http.StatusInternalServerError, err
		// TODO - anything better we could do here?
	}

	// Create parser, injecting Inserter
	p := parser.NewParser(dataType, ins)
	if p == nil {
		metrics.TaskCount.WithLabelValues(string(dataType), "NewInserterError").Inc()
		log.Printf("Error creating parser for %s", dataType)
		return http.StatusInternalServerError, fmt.Errorf("problem creating parser for %s", dataType)
	}
	tsk := task.NewTask(src.Detail(), src, p)
	defer tsk.Close()

	files, err := tsk.ProcessAllTests()

	// Count the files processed per-host-module per-weekday.
	// TODO(soltesz): evaluate separating hosts and pods as separate metrics.
	metrics.FileCount.WithLabelValues(
		path.Host+"-"+path.Site+"-"+path.Experiment,
		date.Weekday().String()).Add(float64(files))

	metrics.WorkerState.WithLabelValues(label, "finish").Inc()
	defer metrics.WorkerState.WithLabelValues(label, "finish").Dec()
	if err != nil {
		metrics.TaskCount.WithLabelValues(string(dataType), "TaskError").Inc()
		log.Printf("Error Processing Tests:  %v", err)
		// NOTE: This may cause indefinite retries, and stalled task queue.
		//  Task will eventually expire, but it might be better to have a
		// different mechanism for retries, particularly for gardener, which
		// waits for empty task queue.
		return http.StatusInternalServerError, err
		// TODO - anything better we could do here?
	}

	metrics.TaskCount.WithLabelValues(string(dataType), "OK").Inc()
	return http.StatusOK, nil
}

// StandardTaskFactory implements task.Factory
type StandardTaskFactory struct {
	Sink      factory.SinkFactory
	Source    factory.SourceFactory
	Annotator factory.AnnotatorFactory
}

// Get implements task.Factory.Get
func (tf *StandardTaskFactory) Get(ctx context.Context, dp etl.DataPath) (*task.Task, etl.ProcessingError) {
	sink, err := tf.Sink.Get(ctx, dp)
	if err != nil {
		e := fmt.Errorf("%v creating sink for %s", err, dp.GetDataType())
		log.Println(e, dp.URI)
		return nil, err
	}

	ann, err := tf.Annotator.Get(ctx, dp)
	if err != nil {
		e := fmt.Errorf("%v creating annotator for %s", err, dp.GetDataType())
		log.Println(e, dp.URI)
		return nil, err
	}
	src, err := tf.Source.Get(ctx, dp)
	if err != nil {
		e := fmt.Errorf("%v creating source for %s", err, dp.GetDataType())
		log.Println(e, dp.URI)
		return nil, err
	}

	p := parser.NewSinkParser(dp.GetDataType(), sink, src.Type(), ann)
	if p == nil {
		e := fmt.Errorf("%v creating parser for %s", err, dp.GetDataType())
		log.Println(e, dp.URI)
		return nil, err
	}

	tsk := task.NewTask(dp.URI, src, p)
	return tsk, nil
}

// ProcessGKETask interprets a filename to create a Task, Parser, and Inserter,
// and processes the file content.
// Used default BQ Sink, and GCS Source.
// Returns an http status code and an error if the task did not complete
// successfully.
func ProcessGKETask(path etl.DataPath, tf task.Factory) etl.ProcessingError {
	// Count number of workers operating on each table.
	metrics.WorkerCount.WithLabelValues(path.DataType).Inc()
	defer metrics.WorkerCount.WithLabelValues(path.DataType).Dec()

	// These keep track of the (nested) state of the worker.
	metrics.WorkerState.WithLabelValues(path.DataType, "worker").Inc()
	defer metrics.WorkerState.WithLabelValues(path.DataType, "worker").Dec()

	tsk, err := tf.Get(nil, path)
	if err != nil {
		metrics.TaskCount.WithLabelValues(err.DataType(), err.Detail()).Inc()
		log.Printf("TaskFactory error: %v", err)
		return err // http.StatusBadRequest, err
	}

	defer tsk.Close()
	return DoGKETask(tsk, path)
}

// DoGKETask creates task, processes all tests and handle metrics
func DoGKETask(tsk *task.Task, path etl.DataPath) etl.ProcessingError {
	files, err := tsk.ProcessAllTests()

	dateFormat := "20060102"
	date, dateErr := time.Parse(dateFormat, path.PackedDate)
	if dateErr != nil {
		metrics.TaskCount.WithLabelValues(path.DataType, "Bad Date").Inc()
		log.Printf("Error parsing path.PackedDate: %v", err)
		return factory.NewError(
			path.DataType, "PackedDate", http.StatusBadRequest, dateErr)
	}

	// Count the files processed per-host-module per-weekday.
	// TODO(soltesz): evaluate separating hosts and pods as separate metrics.
	metrics.FileCount.WithLabelValues(
		path.Host+"-"+path.Site+"-"+path.Experiment,
		date.Weekday().String()).Add(float64(files))

	if err != nil {
		metrics.TaskCount.WithLabelValues(path.DataType, "TaskError").Inc()
		log.Printf("Error Processing Tests:  %v", err)
		return factory.NewError(
			path.DataType, "TaskError", http.StatusInternalServerError, err)
		// TODO - anything better we could do here?
	}

	// NOTE: In the k8s parsers, there are huge spikes in the task rate.
	//  For ndt5, this is likely just because the tasks are very small.
	// In tcpinfo, there are many many small tasks at the end of a date,
	// because long running connections cause pusher to make small archives
	// for subsequent days, and these are lexicographically after all the
	// large files. We are starting to think this is a bug, and tcpinfo
	// should instead place the small files from long running connections
	// in future date directories, instead of the date that the connection
	// originated.  The parser should then also put these small connection
	// snippets into the partition corresponding to the time of the traffic,
	// rather than the time of the original connection.  We are unclear
	// about how to handle short connections that span midnight UTC, but
	// suspect they should be placed in the date of the original connection
	// time.
	metrics.TaskCount.WithLabelValues(path.DataType, "OK").Inc()
	return nil
}
