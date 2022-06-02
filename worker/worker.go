package worker

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/factory"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/task"
)

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

	tsk := task.NewTask(dp.URI, src, p, sink)
	return tsk, nil
}

// ProcessGKETask interprets a filename to create a Task, Parser, and Inserter,
// and processes the file content.
// Used default BQ Sink, and GCS Source.
// Returns an http status code and an error if the task did not complete
// successfully.
func ProcessGKETask(ctx context.Context, path etl.DataPath, tf task.Factory) etl.ProcessingError {
	// Count number of workers operating on each table.
	metrics.WorkerCount.WithLabelValues(path.DataType).Inc()
	defer metrics.WorkerCount.WithLabelValues(path.DataType).Dec()

	// These keep track of the (nested) state of the worker.
	metrics.WorkerState.WithLabelValues(path.DataType, "worker").Inc()
	defer metrics.WorkerState.WithLabelValues(path.DataType, "worker").Dec()

	tsk, err := tf.Get(ctx, path)
	if err != nil {
		metrics.TaskTotal.WithLabelValues(err.DataType(), err.Detail()).Inc()
		log.Printf("TaskFactory error: %v", err)
		return err
	}

	defer tsk.Close()
	return DoGKETask(tsk, path)
}

// DoGKETask creates task, processes all tests and handle metrics
func DoGKETask(tsk *task.Task, path etl.DataPath) etl.ProcessingError {
	files, err := tsk.ProcessAllTests(true) // fail fast on parsing errors.

	dateFormat := "20060102"
	date, dateErr := time.Parse(dateFormat, path.PackedDate)
	if dateErr != nil {
		metrics.TaskTotal.WithLabelValues(path.DataType, "Bad Date").Inc()
		log.Printf("Error parsing path.PackedDate: %v", err)
		return factory.NewError(
			path.DataType, "PackedDate", http.StatusBadRequest, dateErr)
	}

	// Count the files processed per-host-module per-weekday.
	// TODO(soltesz): evaluate separating hosts and pods as separate metrics.
	metrics.FileCount.WithLabelValues(
		path.Experiment,
		date.Weekday().String()).Add(float64(files))

	if err != nil {
		metrics.TaskTotal.WithLabelValues(path.DataType, "TaskError").Inc()
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
	metrics.TaskTotal.WithLabelValues(path.DataType, "OK").Inc()
	return nil
}
