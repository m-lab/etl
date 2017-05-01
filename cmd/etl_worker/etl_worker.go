// Sample
package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"strings"
	"sync/atomic"

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

// TODO(dev) Add unit test
func getFilename(filename string) (string, error) {
	if strings.HasPrefix(filename, "gs://") {
		return filename, nil
	}

	decode, err := base64.StdEncoding.DecodeString(filename)
	if err != nil {
		return "", errors.New("invalid file path: " + filename)
	}
	fn := string(decode[:])
	if strings.HasPrefix(fn, "gs://") {
		return fn, nil
	}

	return "", errors.New("invalid base64 encoded file path: " + fn)
}

func getDataType(fn string) etl.DataType {
	fields := etl.TaskPattern.FindStringSubmatch(fn)
	if fields == nil {
		return etl.INVALID
	}
	dt, ok := etl.DirToDataType[fields[2]]
	if !ok {
		return etl.INVALID
	}
	return dt
}

// Basic throttling to restrict the number of tasks in flight.
var inFlight int32

// Returns true if request should be rejected.
func shouldThrottle() bool {
	if atomic.AddInt32(&inFlight, 1) > 25 {
		atomic.AddInt32(&inFlight, -1)
		return true
	}
	return false
}

func decrementInFlight() {
	atomic.AddInt32(&inFlight, -1)
}

func worker(w http.ResponseWriter, r *http.Request) {
	// These keep track of the (nested) state of the worker.
	metrics.WorkerState.WithLabelValues("top").Inc()
	defer metrics.WorkerState.WithLabelValues("top").Dec()

	// Throttle by grabbing a semaphore from channel.
	if shouldThrottle() {
		metrics.TaskCount.WithLabelValues("unknown", "TooManyRequests").Inc()
		fmt.Fprintf(w, `{"message": "Too many tasks."}`)
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}
	// Decrement counter when worker finishes.
	defer decrementInFlight()

	metrics.WorkerCount.Inc()
	defer metrics.WorkerCount.Dec()

	r.ParseForm()
	// Log request data.
	for key, value := range r.Form {
		log.Printf("Form:   %q == %q\n", key, value)
	}

	// This handles base64 encoding, and requires a gs:// prefix.
	fn, err := getFilename(r.FormValue("filename"))
	if err != nil {
		metrics.TaskCount.WithLabelValues("unknown", "BadRequest").Inc()
		fmt.Fprintf(w, `{"message": "Invalid filename."}`)
		w.WriteHeader(http.StatusBadRequest)
		log.Printf("Invalid filename: %s\n", fn)
		return
	}

	// TODO(dev): log the originating task queue name from headers.
	log.Printf("Received filename: %q\n", fn)

	dataType := getDataType(fn)
	if dataType == etl.INVALID {
		metrics.TaskCount.WithLabelValues("unknown", "BadRequest").Inc()
		fmt.Fprintf(w, `{"message": "Invalid filename."}`)
		w.WriteHeader(http.StatusBadRequest)
		log.Printf("Invalid filename: %s\n", fn)
		return
	}

	client, err := storage.GetStorageClient(false)
	if err != nil {
		metrics.TaskCount.WithLabelValues("unknown", "ServiceUnavailable").Inc()
		log.Printf("Error getting storage client: %v\n", err)
		fmt.Fprintf(w, `{"message": "Could not create client."}`)
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	// TODO - add a timer for reading the file.
	tr, err := storage.NewETLSource(client, fn)
	if err != nil {
		metrics.TaskCount.WithLabelValues(string(dataType), "InternalServerError").Inc()
		log.Printf("Error downloading file: %v", err)
		fmt.Fprintf(w, `{"message": "Problem downloading file."}`)
		w.WriteHeader(http.StatusInternalServerError)
		return
		// TODO - anything better we could do here?
	}
	defer tr.Close()

	ins, err := bq.NewInserter("mlab_sandbox", dataType)
	if err != nil {
		metrics.TaskCount.WithLabelValues(string(dataType), "InternalServerError").Inc()
		log.Printf("Error creating BQ Inserter:  %v", err)
		fmt.Fprintf(w, `{"message": "Problem creating BQ inserter."}`)
		w.WriteHeader(http.StatusInternalServerError)
		return
		// TODO - anything better we could do here?
	}

	// Wrap inserter to give insertion time metrics.
	ins = bq.DurationWrapper{ins}

	// Create parser, injecting Inserter
	p := parser.NewParser(dataType, ins)
	tsk := task.NewTask(fn, tr, p, ins)

	err = tsk.ProcessAllTests()

	metrics.WorkerState.WithLabelValues("finish").Inc()
	defer metrics.WorkerState.WithLabelValues("finish").Dec()
	if err != nil {
		metrics.TaskCount.WithLabelValues(string(dataType), "InternalServerError").Inc()
		log.Printf("Error Processing Tests:  %v", err)
		fmt.Fprintf(w, `{"message": "Error in ProcessAllTests"}`)
		w.WriteHeader(http.StatusInternalServerError)
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
