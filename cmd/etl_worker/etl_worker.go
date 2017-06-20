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

func handler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	fmt.Fprint(w, "Hello world!")
}

// Basic throttling to restrict the number of tasks in flight.
var inFlight int32

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
func shouldThrottle() bool {
	if atomic.AddInt32(&inFlight, 1) > 20 {
		atomic.AddInt32(&inFlight, -1)
		return true
	}
	return false
}

func decrementInFlight() {
	atomic.AddInt32(&inFlight, -1)
}

func worker(w http.ResponseWriter, r *http.Request) {
	// TODO(dev) Check how many times a request has already been attempted.

	// These keep track of the (nested) state of the worker.
	metrics.WorkerState.WithLabelValues("worker").Inc()
	defer metrics.WorkerState.WithLabelValues("worker").Dec()

	// Throttle by grabbing a semaphore from channel.
	if shouldThrottle() {
		metrics.TaskCount.WithLabelValues("unknown", "TooManyRequests").Inc()
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprintf(w, `{"message": "Too many tasks."}`)
		return
	}
	// Decrement counter when worker finishes.
	defer decrementInFlight()

	metrics.WorkerCount.Inc()
	defer metrics.WorkerCount.Dec()

	var err error
	retryCountStr := r.Header.Get("X-AppEngine-TaskRetryCount")
	retryCount := 0
	if retryCountStr != "" {
		retryCount, err = strconv.Atoi(retryCountStr)
		if err != nil {
			log.Printf("Invalid retries string: %s\n", retryCountStr)
		}
	}
	executionCountStr := r.Header.Get("X-AppEngine-TaskExecutionCount")
	executionCount := 0
	if executionCountStr != "" {
		executionCount, err = strconv.Atoi(executionCountStr)
		if err != nil {
			log.Printf("Invalid execution count string: %s\n", executionCountStr)
		}
	}

	r.ParseForm()
	// Log request data.
	for key, value := range r.Form {
		log.Printf("Form:   %q == %q\n", key, value)
	}

	// This handles base64 encoding, and requires a gs:// prefix.
	fn, err := storage.GetFilename(r.FormValue("filename"))
	if err != nil {
		metrics.TaskCount.WithLabelValues("unknown", "BadRequest").Inc()
		log.Printf("Invalid filename: %s\n", fn)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"message": "Invalid filename."}`)
		return
	}

	// TODO(dev): log the originating task queue name from headers.
	log.Printf("Received filename: %q  Retries: %d, Executions: %d\n",
		fn, retryCount, executionCount)

	data, err := etl.ValidateTestPath(fn)
	if err != nil {
		log.Printf("Invalid filename: %v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"message": "Invalid filename."}`)
		return
	}
	dataType := data.GetDataType()

	// Move this into Validate function
	if dataType == etl.INVALID {
		metrics.TaskCount.WithLabelValues("unknown", "BadRequest").Inc()
		log.Printf("Invalid filename: %s\n", fn)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"message": "Invalid filename."}`)
		return
	}

	client, err := storage.GetStorageClient(false)
	if err != nil {
		metrics.TaskCount.WithLabelValues("unknown", "ServiceUnavailable").Inc()
		log.Printf("Error getting storage client: %v\n", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, `{"message": "Could not create client."}`)
		return
	}

	// TODO - add a timer for reading the file.
	tr, err := storage.NewETLSource(client, fn)
	if err != nil {
		metrics.TaskCount.WithLabelValues(string(dataType), "ETLSourceError").Inc()
		log.Printf("Error opening gcs file: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"message": "Problem opening gcs file."}`)
		return
		// TODO - anything better we could do here?
	}
	defer tr.Close()

	dateFormat := "20060102"
	date, err := time.Parse(dateFormat, data.PackedDate)

	dataset, ok := os.LookupEnv("BIGQUERY_DATASET")
	if !ok {
		dataset = "mlab_sandbox"
	}
	ins, err := bq.NewInserter(dataset, dataType, date)
	if err != nil {
		metrics.TaskCount.WithLabelValues(string(dataType), "NewInserterError").Inc()
		log.Printf("Error creating BQ Inserter:  %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"message": "Problem creating BQ inserter."}`)
		return
		// TODO - anything better we could do here?
	}

	// Wrap inserter to give insertion time metrics.
	ins = bq.DurationWrapper{ins}

	// Create parser, injecting Inserter
	p := parser.NewParser(dataType, ins)
	tsk := task.NewTask(fn, tr, p)

	err = tsk.ProcessAllTests()

	metrics.WorkerState.WithLabelValues("finish").Inc()
	defer metrics.WorkerState.WithLabelValues("finish").Dec()
	if err != nil {
		metrics.TaskCount.WithLabelValues(string(dataType), "TaskError").Inc()
		log.Printf("Error Processing Tests:  %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"message": "Error in ProcessAllTests"}`)
		return
		// TODO - anything better we could do here?
	}

	// TODO - if there are any errors, consider sending back a meaningful response
	// for web browser and queue-pusher debugging.
	fmt.Fprintf(w, `{"message": "Success"}`)

	metrics.TaskCount.WithLabelValues(string(dataType), "OK").Inc()
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	// TODO(soltesz): provide a real health check.
	fmt.Fprint(w, "ok")
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
	go http.ListenAndServe(":9090", mux)

	http.HandleFunc("/", handler)
	http.HandleFunc("/worker", metrics.DurationHandler("generic", worker))
	http.HandleFunc("/_ah/health", healthCheckHandler)

	// Enable block profiling
	runtime.SetBlockProfileRate(1000000) // One event per msec.

	// We also setup another prometheus handler on a non-standard path. This
	// path name will be accessible through the AppEngine service address,
	// however it will be served by a random instance.
	http.Handle("/random-metrics", promhttp.Handler())
	http.ListenAndServe(":8080", nil)
}
