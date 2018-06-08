// Sample
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/storage"
	"github.com/m-lab/etl/task"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	// Enable profiling. For more background and usage information, see:
	//   https://blog.golang.org/profiling-go-programs
	"net/http/pprof"
	// Enable exported debug vars.  See https://golang.org/pkg/expvar/
	_ "expvar"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// Task Queue can always submit to an admin restricted URL.
//   login: admin
// Return 200 status code.
// Track reqeusts that last longer than 24 hrs.
// Is task handling idempotent?

// Useful headers added by AppEngine when sending Tasks via Push.
//   X-AppEngine-QueueName
//   X-AppEngine-TaskETA
//   X-AppEngine-TaskName
//   X-AppEngine-TaskRetryCount
//   X-AppEngine-TaskExecutionCount

// TODO(gfr) Add either a black list or a white list for the environment
// variables, so we can hide sensitive vars. https://github.com/m-lab/etl/issues/384
func Status(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "<html><body>\n")
	fmt.Fprintf(w, "<p>NOTE: This is just one of potentially many instances.</p>\n")
	commit := os.Getenv("COMMIT_HASH")
	if len(commit) >= 8 {
		fmt.Fprintf(w, "Release: %s <br>  Commit: <a href=\"https://github.com/m-lab/etl/tree/%s\">%s</a><br>\n",
			os.Getenv("RELEASE_TAG"), os.Getenv("COMMIT_HASH"), os.Getenv("COMMIT_HASH")[0:7])
	} else {
		fmt.Fprintf(w, "Release: %s   Commit: unknown\n", os.Getenv("RELEASE_TAG"))
	}

	fmt.Fprintf(w, "<p>Workers: %d / %d</p>\n", atomic.LoadInt32(&inFlight), maxInFlight)
	env := os.Environ()
	for i := range env {
		fmt.Fprintf(w, "%s</br>\n", env[i])
	}
	fmt.Fprintf(w, "</body></html>\n")
}

// Basic throttling to restrict the number of tasks in flight.
const defaultMaxInFlight = 20

var maxInFlight int32 // Max number of concurrent workers (and tasks in flight).
var inFlight int32    // Current number of tasks in flight.

// Returns true if request should be rejected.
// If the max concurrency (MC) exceeds (or matches) the instances*workers, then
// most requests will be rejected, until the median number of workers is
// less than the throttle.
// ** So we should set max instances (MI) * max workers (MW) > max concurrency.
//
// We also want max_concurrency high enough that most instances have several
// jobs.  With MI=20, MW=25, MC=100, the average workers/instance is only 4, and
// we end up with many instances starved, so AppEngine was removing instances even
// though the queue throughput was poor.
// ** So we probably want MC/MI > MW/2, to prevent starvation.
//
// For now, assuming:
//    MC: 180,  MI: 20, MW: 10
//
// TODO - replace the atomic with a channel based semaphore and non-blocking
// select.
func shouldThrottle() bool {
	if atomic.AddInt32(&inFlight, 1) > maxInFlight {
		atomic.AddInt32(&inFlight, -1)
		return true
	}
	return false
}

func decrementInFlight() {
	atomic.AddInt32(&inFlight, -1)
}

// TODO(gfr) unify counting for http and pubsub paths?
func worker(rwr http.ResponseWriter, rq *http.Request) {
	// This will add metric count and log message from any panic.
	// The panic will still propagate, and http will report it.
	defer func() {
		etl.CountPanics(recover(), "worker")
	}()

	// Throttle by grabbing a semaphore from channel.
	if shouldThrottle() {
		metrics.TaskCount.WithLabelValues("unknown", "worker", "TooManyRequests").Inc()
		rwr.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprintf(rwr, `{"message": "Too many tasks."}`)
		return
	}

	// Decrement counter when worker finishes.
	defer decrementInFlight()

	var err error
	retryCountStr := rq.Header.Get("X-AppEngine-TaskRetryCount")
	retryCount := 0
	if retryCountStr != "" {
		retryCount, err = strconv.Atoi(retryCountStr)
		if err != nil {
			log.Printf("Invalid retries string: %s\n", retryCountStr)
		}
	}
	executionCountStr := rq.Header.Get("X-AppEngine-TaskExecutionCount")
	executionCount := 0
	if executionCountStr != "" {
		executionCount, err = strconv.Atoi(executionCountStr)
		if err != nil {
			log.Printf("Invalid execution count string: %s\n", executionCountStr)
		}
	}

	rq.ParseForm()
	// Log request data.
	for key, value := range rq.Form {
		log.Printf("Form:   %q == %q\n", key, value)
	}

	rawFileName := rq.FormValue("filename")
	status, msg := subworker(rawFileName, executionCount, retryCount)
	rwr.WriteHeader(status)
	fmt.Fprintf(rwr, msg)
}

func subworker(rawFileName string, executionCount, retryCount int) (status int, msg string) {
	// TODO(dev) Check how many times a request has already been attempted.

	var err error
	// This handles base64 encoding, and requires a gs:// prefix.
	fn, err := storage.GetFilename(rawFileName)
	if err != nil {
		metrics.TaskCount.WithLabelValues("unknown", "worker", "BadRequest").Inc()
		log.Printf("Invalid filename: %s\n", fn)
		return http.StatusBadRequest, `{"message": "Invalid filename."}`
	}

	// TODO(dev): log the originating task queue name from headers.
	log.Printf("Received filename: %q  Retries: %d, Executions: %d\n",
		fn, retryCount, executionCount)

	data, err := etl.ValidateTestPath(fn)
	if err != nil {
		log.Printf("Invalid filename: %v\n", err)
		return http.StatusBadRequest, `{"message": "Invalid filename."}`
	}
	dataType := data.GetDataType()

	// Count number of workers operating on each table.
	metrics.WorkerCount.WithLabelValues(data.TableBase()).Inc()
	defer metrics.WorkerCount.WithLabelValues(data.TableBase()).Dec()

	// These keep track of the (nested) state of the worker.
	metrics.WorkerState.WithLabelValues(data.TableBase(), "worker").Inc()
	defer metrics.WorkerState.WithLabelValues(data.TableBase(), "worker").Dec()

	// Move this into Validate function
	if dataType == etl.INVALID {
		metrics.TaskCount.WithLabelValues(data.TableBase(), "worker", "BadRequest").Inc()
		log.Printf("Invalid filename: %s\n", fn)
		return http.StatusBadRequest, `{"message": "Invalid filename."}`
	}

	client, err := storage.GetStorageClient(false)
	if err != nil {
		metrics.TaskCount.WithLabelValues(data.TableBase(), "worker", "ServiceUnavailable").Inc()
		log.Printf("Error getting storage client: %v\n", err)
		return http.StatusServiceUnavailable, `{"message": "Could not create client."}`
	}

	// TODO - add a timer for reading the file.
	tr, err := storage.NewETLSource(client, fn)
	if err != nil {
		metrics.TaskCount.WithLabelValues(data.TableBase(), string(dataType), "ETLSourceError").Inc()
		log.Printf("Error opening gcs file: %v", err)
		return http.StatusInternalServerError, `{"message": "Problem opening gcs file."}`
		// TODO - anything better we could do here?
	}
	defer tr.Close()
	// Label storage metrics with the expected table name.
	tr.TableBase = data.TableBase()

	dateFormat := "20060102"
	date, err := time.Parse(dateFormat, data.PackedDate)

	dataset := etl.DataTypeToDataset(dataType)
	ins, err := bq.NewInserter(dataset, dataType, date)
	if err != nil {
		metrics.TaskCount.WithLabelValues(data.TableBase(), string(dataType), "NewInserterError").Inc()
		log.Printf("Error creating BQ Inserter:  %v", err)
		return http.StatusInternalServerError, `{"message": "Problem creating BQ inserter."}`
		// TODO - anything better we could do here?
	}

	// Wrap inserter to give insertion time metrics.
	ins = bq.DurationWrapper{ins}

	// Create parser, injecting Inserter
	p := parser.NewParser(dataType, ins)
	tsk := task.NewTask(fn, tr, p)

	files, err := tsk.ProcessAllTests()

	// Count the files processed per-host-module per-weekday.
	// TODO(soltesz): evaluate separating hosts and pods as separate metrics.
	metrics.FileCount.WithLabelValues(
		data.Host+"-"+data.Pod+"-"+data.Experiment,
		date.Weekday().String()).Add(float64(files))

	metrics.WorkerState.WithLabelValues(data.TableBase(), "finish").Inc()
	defer metrics.WorkerState.WithLabelValues(data.TableBase(), "finish").Dec()
	if err != nil {
		metrics.TaskCount.WithLabelValues(data.TableBase(), string(dataType), "TaskError").Inc()
		log.Printf("Error Processing Tests:  %v", err)
		return http.StatusInternalServerError, `{"message": "Error in ProcessAllTests"}`
		// TODO - anything better we could do here?
	}

	metrics.TaskCount.WithLabelValues(data.TableBase(), string(dataType), "OK").Inc()
	return http.StatusOK, `{"message": "Success"}`
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	// TODO(soltesz): provide a real health check.
	fmt.Fprint(w, "ok")
}

func setMaxInFlight() {
	maxInFlightString, ok := os.LookupEnv("MAX_WORKERS")
	if ok {
		maxInFlightInt, err := strconv.Atoi(maxInFlightString)
		if err == nil {
			maxInFlight = int32(maxInFlightInt)
		} else {
			log.Println("MAX_WORKERS not configured.  Using 20.")
			maxInFlight = defaultMaxInFlight
		}
	} else {
		log.Println("MAX_WORKERS not configured.  Using 20.")
		maxInFlight = defaultMaxInFlight
	}
}

func main() {
	// Define a custom serve mux for prometheus to listen on a separate port.
	// We listen on a separate port so we can forward this port on the host VM.
	// We cannot forward port 8080 because it is used by AppEngine.
	mux := http.NewServeMux()
	// Assign the default prometheus handler to the standard exporter path.
	mux.Handle("/metrics", promhttp.Handler())
	// Assign the pprof handling paths to the external port to access individual
	// instances.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/", Status)
	mux.HandleFunc("/status", Status)
	go http.ListenAndServe(":9090", mux)

	http.HandleFunc("/", Status)
	http.HandleFunc("/status", Status)
	http.HandleFunc("/worker", metrics.DurationHandler("generic", worker))
	http.HandleFunc("/_ah/health", healthCheckHandler)

	// Enable block profiling
	runtime.SetBlockProfileRate(1000000) // One event per msec.

	setMaxInFlight()

	// We also setup another prometheus handler on a non-standard path. This
	// path name will be accessible through the AppEngine service address,
	// however it will be served by a random instance.
	http.Handle("/random-metrics", promhttp.Handler())
	http.ListenAndServe(":8080", nil)
}
