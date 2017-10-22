// Sample
package main

import (
	"encoding/json"
	"fmt"
	"html"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"google.golang.org/api/option"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"

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
	// Throttle by grabbing a semaphore from channel.
	if shouldThrottle() {
		metrics.TaskCount.WithLabelValues("unknown", "TooManyRequests").Inc()
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

	// These keep track of the (nested) state of the worker.
	metrics.WorkerState.WithLabelValues("worker").Inc()
	defer metrics.WorkerState.WithLabelValues("worker").Dec()

	metrics.WorkerCount.Inc()
	defer metrics.WorkerCount.Dec()

	var err error
	// This handles base64 encoding, and requires a gs:// prefix.
	fn, err := storage.GetFilename(rawFileName)
	if err != nil {
		metrics.TaskCount.WithLabelValues("unknown", "BadRequest").Inc()
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

	// Move this into Validate function
	if dataType == etl.INVALID {
		metrics.TaskCount.WithLabelValues("unknown", "BadRequest").Inc()
		log.Printf("Invalid filename: %s\n", fn)
		return http.StatusBadRequest, `{"message": "Invalid filename."}`
	}

	client, err := storage.GetStorageClient(false)
	if err != nil {
		metrics.TaskCount.WithLabelValues("unknown", "ServiceUnavailable").Inc()
		log.Printf("Error getting storage client: %v\n", err)
		return http.StatusServiceUnavailable, `{"message": "Could not create client."}`
	}

	// TODO - add a timer for reading the file.
	tr, err := storage.NewETLSource(client, fn)
	if err != nil {
		metrics.TaskCount.WithLabelValues(string(dataType), "ETLSourceError").Inc()
		log.Printf("Error opening gcs file: %v", err)
		return http.StatusInternalServerError, `{"message": "Problem opening gcs file."}`
		// TODO - anything better we could do here?
	}
	defer tr.Close()

	dateFormat := "20060102"
	date, err := time.Parse(dateFormat, data.PackedDate)

	dataset, ok := os.LookupEnv("BIGQUERY_DATASET")
	if !ok {
		// TODO - make this fatal.
		dataset = "mlab_sandbox"
	}
	ins, err := bq.NewInserter(dataset, dataType, date)
	if err != nil {
		metrics.TaskCount.WithLabelValues(string(dataType), "NewInserterError").Inc()
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

	metrics.WorkerState.WithLabelValues("finish").Inc()
	defer metrics.WorkerState.WithLabelValues("finish").Dec()
	if err != nil {
		metrics.TaskCount.WithLabelValues(string(dataType), "TaskError").Inc()
		log.Printf("Error Processing Tests:  %v", err)
		return http.StatusInternalServerError, `{"message": "Error in ProcessAllTests"}`
		// TODO - anything better we could do here?
	}

	metrics.TaskCount.WithLabelValues(string(dataType), "OK").Inc()
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

func runPubSubHandler() error {
	ctx := context.Background()
	opt := option.WithServiceAccountFile(os.Getenv("SUBSCRIPTION_KEY"))
	// Must use the project where the subscription resides, or else
	// we can't find it.
	proj := os.Getenv("SUBSCRIPTION_PROJECT")
	subscription := os.Getenv("SUBSCRIPTION_NAME")
	client, err := pubsub.NewClient(ctx, proj, opt)

	if err != nil {
		return err
	}

	sub := client.Subscription(subscription)

	// This definitely limits the number of messages being
	// concurrently processed, but NOT the number of messages
	// in some kind of queue.
	sub.ReceiveSettings.MaxOutstandingMessages = int(maxInFlight)

	// This seems to have no impact on the number of concurrent
	// messages, and each concurrently processed message is
	//  handled in its own (new) goroutine.
	// sub.ReceiveSettings.NumGoroutines = 1

	err = sub.Receive(ctx, func(cctx context.Context, msg *pubsub.Message) {
		jdata := html.UnescapeString(string(msg.Data))
		data := make(map[string]string)
		json.Unmarshal([]byte(jdata), &data)
		bucket, ok := data["bucket"]
		if !ok {
			// "id" may not be informative either.  8-(
			log.Printf("Request missing bucket name: %s\n", data["id"])
			msg.Ack() // No point in trying again.
		}
		filename, ok := data["name"]
		if !ok {
			// "id" may not be informative either.  8-(
			log.Printf("Request missing file name: %s\n", data["id"])
		}
		fullname := "gs://" + bucket + "/" + filename

		status, outcome := subworker(fullname, 0, 0)
		if status != http.StatusOK {
			log.Println(outcome)
		}
	})
	return err
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

	setMaxInFlight()

	// We also setup another prometheus handler on a non-standard path. This
	// path name will be accessible through the AppEngine service address,
	// however it will be served by a random instance.
	http.Handle("/random-metrics", promhttp.Handler())
	go http.ListenAndServe(":8080", nil)

	subscription := os.Getenv("SUBSCRIPTION_NAME")
	if subscription != "" {
		err := runPubSubHandler()
		if err != nil {
			panic(err)
		}
	}
}
