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
	"github.com/m-lab/etl/storage"
	"github.com/m-lab/etl/task"
)

// GetSource gets the TestSource for the filename.
// fn is a gs:// GCS uri.
func GetSource(client *gcs.Client, uri string) (etl.TestSource, etl.DataPath, int, error) {
	path, err := etl.ValidateTestPath(uri)
	label := path.TableBase() // This works even on error?
	if err != nil {
		metrics.TaskCount.WithLabelValues(label, "worker", "InvalidFilename").Inc()
		log.Printf("Invalid filename: %v\n", err)
		return nil, etl.DataPath{}, http.StatusBadRequest, err
	}

	dataType := path.GetDataType()
	if dataType == etl.INVALID {
		metrics.TaskCount.WithLabelValues(label, "invalid", "SourcePathError").Inc()
		log.Printf("Invalid datatype: %s", path)
		return nil, etl.DataPath{}, http.StatusInternalServerError, err
	}
	tr, err := storage.NewTestSource(client, uri, label)
	if err != nil {
		metrics.TaskCount.WithLabelValues(label, string(dataType), "ETLSourceError").Inc()
		log.Printf("Error opening gcs file: %v", err)
		return nil, etl.DataPath{}, http.StatusInternalServerError, err
		// TODO - anything better we could do here?
	}
	return tr, path, http.StatusOK, nil
}

// ProcessTask interprets a filename to create a Task, Parser, and Inserter,
// and processes the file content.
// storage.Client may be injected for testing.
// Returns an http status code and an error if the task did not complete successfully.
// DEPRECATED - should migrate to ProcessGKETask.
func ProcessTask(fn string) (int, error) {
	client, err := storage.GetStorageClient(false)
	if err != nil {
		path, _ := etl.ValidateTestPath(fn)
		metrics.TaskCount.WithLabelValues(path.TableBase(), "worker", "ServiceUnavailable").Inc()
		log.Printf("Error getting storage client: %v\n", err)
		return http.StatusServiceUnavailable, err
	}
	return ProcessTaskWithClient(client, fn)
}

func ProcessTaskWithClient(client *gcs.Client, fn string) (int, error) {
	tr, _, status, err := GetSource(client, fn)
	if err != nil {
		return status, err
	}
	defer tr.Close()

	return ProcessTestSource(tr, fn)
}

func ProcessTestSource(src etl.TestSource, fn string) (int, error) {
	path, err := etl.ValidateTestPath(fn)
	label := path.TableBase() // This works even on error?
	if err != nil {
		metrics.TaskCount.WithLabelValues(label, "worker", "InvalidFilename").Inc()
		log.Printf("Invalid filename: %v\n", err)
		return http.StatusBadRequest, err
	}

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
		metrics.TaskCount.WithLabelValues(label, string(dataType), "NewInserterError").Inc()
		log.Printf("Error creating parser for %s", dataType)
		return http.StatusInternalServerError, fmt.Errorf("problem creating parser for %s", dataType)
	}
	tsk := task.NewTask(fn, src, p)

	files, err := tsk.ProcessAllTests()

	// Count the files processed per-host-module per-weekday.
	// TODO(soltesz): evaluate separating hosts and pods as separate metrics.
	metrics.FileCount.WithLabelValues(
		path.Host+"-"+path.Site+"-"+path.Experiment,
		date.Weekday().String()).Add(float64(files))

	metrics.WorkerState.WithLabelValues(label, "finish").Inc()
	defer metrics.WorkerState.WithLabelValues(label, "finish").Dec()
	if err != nil {
		metrics.TaskCount.WithLabelValues(label, string(dataType), "TaskError").Inc()
		log.Printf("Error Processing Tests:  %v", err)
		// NOTE: This may cause indefinite retries, and stalled task queue.  Task will eventually
		// expire, but it might be better to have a different mechanism for retries, particularly
		// for gardener, which waits for empty task queue.
		return http.StatusInternalServerError, err
		// TODO - anything better we could do here?
	}

	metrics.TaskCount.WithLabelValues(label, string(dataType), "OK").Inc()
	return http.StatusOK, nil
}

// ProcessGKETask interprets a filename to create a Task, Parser, and Inserter,
// and processes the file content.  The inserter is customized to write to column partitioned tables.
// It is currently used in the GKE parser instances, but will eventually replace ProcessTask for
// all parser/task types.
// Returns an http status code and an error if the task did not complete successfully.
// TODO pass in the configured Sink object, instead of creating based on datatype.
func ProcessGKETask(fn string, pdt bqx.PDT, ann api.Annotator) (int, error) {
	gcsClient, err := storage.GetStorageClient(false)
	if err != nil {
		path, _ := etl.ValidateTestPath(fn)
		metrics.TaskCount.WithLabelValues(path.TableBase(), "worker", "ServiceUnavailable").Inc()
		log.Printf("Error getting storage client: %v\n", err)
		return http.StatusServiceUnavailable, err
	}
	client, err := bq.GetClient(pdt.Project)
	if err != nil {
		return 0, err
	}

	uploader := client.Dataset(pdt.Dataset).Table(pdt.Table).Uploader()
	// This avoids problems when a single row of the insert has invalid
	// data.  We then have to carefully parse the returned error object.
	uploader.SkipInvalidRows = true

	return ProcessGKETaskWithClient(fn, gcsClient, uploader, ann)
}

// ProcessGKETaskWithClient uses the provided GCS client to source the file.
func ProcessGKETaskWithClient(fn string, client *gcs.Client, uploader etl.Uploader, ann api.Annotator) (int, error) {
	tr, path, status, err := GetSource(client, fn)
	if err != nil {
		return status, err
	}
	defer tr.Close()

	label := path.TableBase()

	// Count number of workers operating on each table.
	metrics.WorkerCount.WithLabelValues(label).Inc()
	defer metrics.WorkerCount.WithLabelValues(label).Dec()

	// These keep track of the (nested) state of the worker.
	metrics.WorkerState.WithLabelValues(label, "worker").Inc()
	defer metrics.WorkerState.WithLabelValues(label, "worker").Dec()

	dataType := path.GetDataType()
	pdt := bqx.PDT{Project: dataType.BigqueryProject(), Dataset: dataType.Dataset(), Table: dataType.Table()}

	ins, err := bq.NewColumnPartitionedInserterWithUploader(pdt, uploader)
	if err != nil {
		metrics.TaskCount.WithLabelValues(label, string(dataType), "NewInserterError").Inc()
		log.Printf("Error creating BQ Inserter:  %v", err)
		return http.StatusInternalServerError, err
		// TODO - anything better we could do here?
	}

	// Create parser, injecting Inserter
	p := parser.NewSinkParser(dataType, ins, label, ann)
	if p == nil {
		metrics.TaskCount.WithLabelValues(label, string(dataType), "NewInserterError").Inc()
		log.Printf("Error creating parser for %s", dataType)
		return http.StatusInternalServerError, fmt.Errorf("problem creating parser for %s", dataType)
	}
	tsk := task.NewTask(fn, tr, p)

	files, err := tsk.ProcessAllTests()

	dateFormat := "20060102"
	date, err := time.Parse(dateFormat, path.PackedDate)
	if err != nil {
		metrics.TaskCount.WithLabelValues(label, string(dataType), "Bad Date").Inc()
		log.Printf("Error parsing path.PackedDate: %v", err)
		return http.StatusBadRequest, err
	}

	// Count the files processed per-host-module per-weekday.
	// TODO(soltesz): evaluate separating hosts and pods as separate metrics.
	metrics.FileCount.WithLabelValues(
		path.Host+"-"+path.Site+"-"+path.Experiment,
		date.Weekday().String()).Add(float64(files))

	metrics.WorkerState.WithLabelValues(label, "finish").Inc()
	defer metrics.WorkerState.WithLabelValues(label, "finish").Dec()
	if err != nil {
		metrics.TaskCount.WithLabelValues(label, string(dataType), "TaskError").Inc()
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
	metrics.TaskCount.WithLabelValues(label, string(dataType), "OK").Inc()
	return http.StatusOK, nil
}
