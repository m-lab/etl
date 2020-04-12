package worker

import (
	"fmt"
	"log"
	"net/http"
	"time"

	gcs "cloud.google.com/go/storage"

	"github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/go/bqx"

	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/row"
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
	tr, err := storage.NewTestSource(client, uri, label)
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
// Returns an http status code and an error if the task did not complete successfully.
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
		// NOTE: This may cause indefinite retries, and stalled task queue.  Task will eventually
		// expire, but it might be better to have a different mechanism for retries, particularly
		// for gardener, which waits for empty task queue.
		return http.StatusInternalServerError, err
		// TODO - anything better we could do here?
	}

	metrics.TaskCount.WithLabelValues(string(dataType), "OK").Inc()
	return http.StatusOK, nil
}

// ProcessGKETask interprets a filename to create a Task, Parser, and Inserter,
// and processes the file content.
// Used default BQ Sink, and GCS Source.
// Returns an http status code and an error if the task did not complete successfully.
func ProcessGKETask(fn string, path etl.DataPath, ann api.Annotator) (int, error) {
	dataType := path.GetDataType()
	pdt := bqx.PDT{Project: dataType.BigqueryProject(), Dataset: dataType.Dataset(), Table: dataType.Table()}

	bqClient, err := bq.GetClient(pdt.Project)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	up := bqClient.Dataset(pdt.Dataset).Table(pdt.Table).Uploader()
	// This avoids problems when a single row of the insert has invalid
	// data.  We then have to carefully parse the returned error object.
	up.SkipInvalidRows = true
	return ProcessGKETaskWithUploader(fn, pdt, up, ann)
}

// ProcessGKETaskWithUploader writes to the provided uploader.
func ProcessGKETaskWithUploader(fn string, pdt bqx.PDT, uploader etl.Uploader, ann api.Annotator) (int, error) {
	client, err := storage.GetStorageClient(false)
	if err != nil {
		path, _ := etl.ValidateTestPath(fn)
		metrics.TaskCount.WithLabelValues(path.TableBase(), "worker", "ServiceUnavailable").Inc()
		log.Printf("Error getting storage client: %v\n", err)
		return http.StatusServiceUnavailable, err
	}
	return ProcessGKETaskWithClient(fn, pdt, client, uploader, ann)
}

// ProcessGKETaskWithClient uses the provided GCS client to source the file.
func ProcessGKETaskWithClient(fn string, pdt bqx.PDT, client *gcs.Client, uploader etl.Uploader, ann api.Annotator) (int, error) {
	src, path, status, err := GetSource(client, fn)
	if err != nil {
		return status, err
	}
	defer src.Close()

	label := src.Type()

	// Count number of workers operating on each table.
	metrics.WorkerCount.WithLabelValues(label).Inc()
	defer metrics.WorkerCount.WithLabelValues(label).Dec()

	// These keep track of the (nested) state of the worker.
	metrics.WorkerState.WithLabelValues(label, "worker").Inc()
	defer metrics.WorkerState.WithLabelValues(label, "worker").Dec()

	dataType := path.GetDataType()

	ins, err := bq.NewColumnPartitionedInserterWithUploader(pdt, uploader)
	if err != nil {
		metrics.TaskCount.WithLabelValues(label, string(dataType), "NewInserterError").Inc()
		log.Printf("Error creating BQ Inserter:  %v", err)
		return http.StatusInternalServerError, err
		// TODO - anything better we could do here?
	}

	return ProcessGKESourceSink(fn, path, src, ins, ann)
}

// ProcessGKESourceSink processes files in a TestSource to a row.Sink.
// TODO remove DataPath arg?
func ProcessGKESourceSink(fn string, path etl.DataPath, src etl.TestSource, sink row.Sink, ann api.Annotator) (int, error) {
	// Create parser, injecting Inserter
	p := parser.NewSinkParser(path.GetDataType(), sink, src.Type(), ann)
	if p == nil {
		metrics.TaskCount.WithLabelValues(src.Type(), "NewInserterError").Inc()
		log.Printf("Error creating parser for %s", path.GetDataType())
		return http.StatusInternalServerError, fmt.Errorf("problem creating parser for %s", path.GetDataType())
	}

	return DoGKETask(fn, path, src, p)
}

// DoGKETask creates task, processes all tests and handle metrics
func DoGKETask(fn string, path etl.DataPath, src etl.TestSource, parser etl.Parser) (int, error) {
	tsk := task.NewTask(fn, src, parser)
	files, err := tsk.ProcessAllTests()

	dateFormat := "20060102"
	date, err := time.Parse(dateFormat, path.PackedDate)
	if err != nil {
		metrics.TaskCount.WithLabelValues(src.Type(), "Bad Date").Inc()
		log.Printf("Error parsing path.PackedDate: %v", err)
		return http.StatusBadRequest, err
	}

	// Count the files processed per-host-module per-weekday.
	// TODO(soltesz): evaluate separating hosts and pods as separate metrics.
	metrics.FileCount.WithLabelValues(
		path.Host+"-"+path.Site+"-"+path.Experiment,
		date.Weekday().String()).Add(float64(files))

	if err != nil {
		metrics.TaskCount.WithLabelValues(src.Type(), "TaskError").Inc()
		log.Printf("Error Processing Tests:  %v", err)
		// NOTE: This may cause indefinite retries, and stalled task queue.  Task will eventually
		// expire, but it might be better to have a different mechanism for retries, particularly
		// for gardener, which waits for empty task queue.
		return http.StatusInternalServerError, err
		// TODO - anything better we could do here?
	}

	// NOTE: In the k8s parsers, there are huge spikes in the task rate.  For ndt5, this is likely just
	// because the tasks are very small.  In tcpinfo, there are many many small tasks at the end of
	// a date, because long running connections cause pusher to make small archives for subsequent
	// days, and these are lexicographically after all the large files.
	// We are starting to think this is a bug, and tcpinfo should instead place the small files
	// from long running connections in future date directories, instead of the date that the
	// connection originated.  The parser should then also put these small connection snippets into
	// the partition corresponding to the time of the traffic, rather than the time of the original
	// connection.  We are unclear about how to handle short connections that span midnight UTC, but
	// suspect they should be placed in the date of the original connection time.
	metrics.TaskCount.WithLabelValues(src.Type(), "OK").Inc()
	return http.StatusOK, nil
}
