// Sample
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/m-lab/go/prometheusx"
	"github.com/m-lab/go/rtx"

	"github.com/m-lab/etl/active"
	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/factory"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/storage"
	"github.com/m-lab/etl/task"
	"github.com/m-lab/etl/worker"

	// Enable profiling. For more background and usage information, see:
	//   https://blog.golang.org/profiling-go-programs
	_ "net/http/pprof"

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

// Status writes a status summary to a ResponseWriter, and can be used as a Handler.
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

// This limits the number of workers available for externally requested single task files.
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
func handleRequest(rwr http.ResponseWriter, rq *http.Request) {
	// This will add metric count and log message from any panic.
	// The panic will still propagate, and http will report it.
	defer func() {
		metrics.CountPanics(recover(), "handleRequest")
	}()

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
	etaUnixStr := rq.Header.Get("X-AppEngine-TaskETA")
	etaUnixSeconds := float64(0)
	if etaUnixStr != "" {
		etaUnixSeconds, err = strconv.ParseFloat(etaUnixStr, 64)
		if err != nil {
			log.Printf("Invalid eta string: %s\n", etaUnixStr)
		}
	}
	etaTime := time.Unix(int64(etaUnixSeconds), 0) // second granularity is sufficient.
	age := time.Since(etaTime)

	rq.ParseForm()
	// Log request data.
	for key, value := range rq.Form {
		log.Printf("Form:   %q == %q\n", key, value)
	}

	rawFileName := rq.FormValue("filename")
	status, msg := subworker(rawFileName, executionCount, retryCount, age)
	rwr.WriteHeader(status)
	fmt.Fprintf(rwr, msg)
}

func subworker(rawFileName string, executionCount, retryCount int, age time.Duration) (status int, msg string) {
	// TODO(dev) Check how many times a request has already been attempted.

	var err error
	// This handles base64 encoding, and requires a gs:// prefix.
	fn, err := etl.GetFilename(rawFileName)
	if err != nil {
		metrics.TaskCount.WithLabelValues("unknown", "BadRequest").Inc()
		log.Printf("Invalid filename: %s\n", fn)
		return http.StatusBadRequest, `{"message": "Invalid filename."}`
	}

	// TODO(dev): log the originating task queue name from headers.
	log.Printf("Received filename: %q  Retries: %d, Executions: %d, Age: %5.2f hours\n",
		fn, retryCount, executionCount, age.Hours())

	status, err = worker.ProcessTask(fn)
	if err == nil {
		msg = `{"message": "Success"}`
	} else {
		msg = fmt.Sprintf(`{"message": "%s"}`, err.Error())
	}
	return
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

type runnable struct {
	tf task.Factory
	gcs.ObjectAttrs
}

func (r *runnable) Run(ctx context.Context) error {
	path := fmt.Sprintf("gs://%s/%s", r.Bucket, r.Name)
	dp, err := etl.ValidateTestPath(path)
	if err != nil {
		log.Printf("Invalid filename: %v\n", err)
		return err
	}

	start := time.Now()
	log.Println("Processing", path)

	statusCode := http.StatusOK
	pErr := worker.ProcessGKETask(ctx, dp, r.tf)
	if pErr != nil {
		statusCode = pErr.Code()
	}
	metrics.DurationHistogram.WithLabelValues(
		dp.DataType, http.StatusText(statusCode)).Observe(
		time.Since(start).Seconds())
	return err
}

func (r *runnable) Info() string {
	// Should truncate this to exclude the date, maybe include the year?
	return r.Name
}

func toRunnable(obj *gcs.ObjectAttrs) active.Runnable {
	c, err := storage.GetStorageClient(false)
	if err != nil {
		return nil // TODO add an error?
	}
	taskFactory := worker.StandardTaskFactory{
		Annotator: factory.DefaultAnnotatorFactory(),
		Sink:      bq.NewSinkFactory(),
		Source:    storage.GCSSourceFactory(c),
	}
	return &runnable{&taskFactory, *obj}
}

func mustGardenerAPI(ctx context.Context, jobServer string) *active.GardenerAPI {
	rawBase := fmt.Sprintf("http://%s:8080", jobServer)
	base, err := url.Parse(rawBase)
	rtx.Must(err, "Invalid jobServer: "+rawBase)

	return active.NewGardenerAPI(*base, active.MustStorageClient(ctx))
}

var mainCtx, mainCancel = context.WithCancel(context.Background())

func main() {
	defer mainCancel()
	flag.Parse()

	// Expose prometheus and pprof metrics on a separate port.
	prometheusx.MustStartPrometheus(":9090")

	http.HandleFunc("/", Status)
	http.HandleFunc("/status", Status)
	http.HandleFunc("/worker", metrics.DurationHandler("generic", handleRequest))
	http.HandleFunc("/_ah/health", healthCheckHandler)

	// Enable block profiling
	runtime.SetBlockProfileRate(1000000) // One event per msec.

	setMaxInFlight()

	// We also setup another prometheus handler on a non-standard path. This
	// path name will be accessible through the AppEngine service address,
	// however it will be served by a random instance.
	http.Handle("/random-metrics", promhttp.Handler())

	gardener := os.Getenv("GARDENER_HOST")
	if len(gardener) > 0 {
		log.Println("Using", gardener)
		maxWorkers := 120
		minPollingInterval := 10 * time.Second
		gapi := mustGardenerAPI(mainCtx, gardener)
		// Note that this does not currently track duration metric.
		go gapi.Poll(mainCtx, toRunnable, maxWorkers, minPollingInterval)
	} else {
		log.Println("GARDENER_HOST not specified or empty")
	}
	rtx.Must(http.ListenAndServe(":8080", nil), "failed to listen")
}
