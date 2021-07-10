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
	"golang.org/x/sync/errgroup"

	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/httpx"
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

	"github.com/m-lab/annotation-service/site"

	// Enable profiling. For more background and usage information, see:
	//   https://blog.golang.org/profiling-go-programs
	_ "net/http/pprof"

	// Enable exported debug vars.  See https://golang.org/pkg/expvar/
	_ "expvar"
)

// Basic throttling to restrict the number of tasks in flight.
const defaultMaxInFlight = 20

var (
	// This limits the number of workers available for externally requested single task files.
	maxInFlight int32 // Max number of concurrent workers (and tasks in flight).
	inFlight    int32 // Current number of tasks in flight.
)

// Flags.
var (
	outputType = flagx.Enum{
		Options: []string{"gcs", "bigquery", "local"},
		Value:   "bigquery",
	}

	maxActiveTasks = flag.Int64("max_active", 1, "Maximum number of active tasks")
	gardenerHost   = flag.String("gardener_host", "", "Gardener host for jobs")

	servicePort     = flag.String("service_port", ":8080", "The main (private) service port")
	shutdownTimeout = flag.Duration("shutdown_timeout", 1*time.Minute, "Graceful shutdown time allowance")
	gcloudProject   = flag.String("gcloud_project", "", "GCP Project id")
	maxWorkers      = flag.Int("max_workers", defaultMaxInFlight, "Maximum number of workers")
	isBatch         = flag.Bool("batch_service", false, "Whether to run the parser in batch mode")
	omitDeltas      = flag.Bool("ndt_omit_deltas", false, "Whether to skip ndt.web100 snapshot deltas")
	bigqueryProject = flag.String("bigquery_project", "", "Override GCLOUD_PROJECT for BigQuery operations")
	bigqueryDataset = flag.String("bigquery_dataset", "", "Override the BigQuery dataset for output tables")
	outputDir       = flag.String("output_dir", "", "If output type is 'local', write output to this directory")
	annotatorURL    = flagx.URL{}
)

// Other global values.
var (
	mainCtx, mainCancel = context.WithCancel(context.Background())

	// In active polling mode, this holds the GardenerAPI.
	// TODO: eliminate this global by making the Status handler a receiver.
	gardenerAPI *active.GardenerAPI
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	flag.Var(&outputType, "output", "Output to bigquery or gcs.")
	flag.Var(&annotatorURL, "annotator_url", "Base URL for the annotation service.")
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
	commit := etl.GitCommit
	if len(commit) >= 8 {
		fmt.Fprintf(w, "Release: %s <br>  Commit: <a href=\"https://github.com/m-lab/etl/tree/%s\">%s</a><br>\n",
			etl.Version, etl.GitCommit, etl.GitCommit[0:7])
	} else {
		fmt.Fprintf(w, "Release: %s   Commit: %s\n", etl.Version, etl.GitCommit)
	}

	if gardenerAPI != nil {
		gardenerAPI.Status(w)
	}
	switch outputType.Value {
	case "bigquery":
		fmt.Fprintf(w, "Writing output to BigQuery\n")
	case "gcs":
		fmt.Fprintf(w, "Writing output to %s\n", outputBucket())
	}
	fmt.Fprintf(w, "<p>Workers: %d / %d</p>\n", atomic.LoadInt32(&inFlight), maxInFlight)
	env := os.Environ()
	for i := range env {
		fmt.Fprintf(w, "%s</br>\n", env[i])
	}
	fmt.Fprintf(w, "</body></html>\n")
}

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

func handleLocalRequest(rw http.ResponseWriter, req *http.Request) {
	fn, err := etl.GetFilename(req.FormValue("filename"))
	if err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(rw, "failed to get valid filename= parameter from request")
		return
	}

	dp, err := etl.ValidateTestPath(fn)
	if err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(rw, "failed to validate test path: %q", fn)
		return
	}

	c, err := storage.GetStorageClient(false)
	if err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(rw, "failed to get storage client")
		return
	}

	ctx := context.Background()
	obj, err := c.Bucket(dp.Bucket).Object(dp.Path).Attrs(ctx)
	if err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(rw, "failed to get object attrs for %s / %s", dp.Bucket, dp.Path)
		return
	}

	r := toRunnable(obj)
	err = r.Run(ctx)
	if err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(rw, "runnable failed to run on %s / %s", dp.Bucket, dp.Path)
		return
	}

	fmt.Fprintf(rw, "no observed errors")
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
	fmt.Fprint(rwr, msg)
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

type runnable struct {
	tf task.Factory
	gcs.ObjectAttrs
}

func (r *runnable) Run(ctx context.Context) error {
	path := fmt.Sprintf("gs://%s/%s", r.Bucket, r.Name)
	dp, err := etl.ValidateTestPath(path)
	if err != nil {
		log.Printf("Invalid filename: %v\n", err)
		metrics.TaskCount.WithLabelValues(string(etl.INVALID), "BadRequest").Inc()
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

func outputBucket() string {
	return "etl-" + *gcloudProject
}

func toRunnable(obj *gcs.ObjectAttrs) active.Runnable {
	c, err := storage.GetStorageClient(false)
	if err != nil {
		return nil // TODO add an error?
	}

	var sink factory.SinkFactory
	switch outputType.Value {
	case "bigquery":
		sink = bq.NewSinkFactory()
	case "gcs":
		sink = storage.NewSinkFactory(c, outputBucket())
	case "local":
		sink = storage.NewLocalFactory(*outputDir)
	}

	taskFactory := worker.StandardTaskFactory{
		Annotator: factory.DefaultAnnotatorFactory(),
		Sink:      sink,
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

// Used for testing.
var mainServerAddr = make(chan string, 1)

// startServers does not return until context is cancelled.
func startServers(ctx context.Context, mux http.Handler) *errgroup.Group {
	// Expose prometheus and pprof metrics on a separate port.
	promServer := prometheusx.MustServeMetrics()
	defer promServer.Close() // Only relevant if ListenAndServeAsync fails below.

	// Start up the main job and update server.
	server := &http.Server{
		Addr:    *servicePort, // This used to be :8080
		Handler: mux,
	}

	rtx.Must(httpx.ListenAndServeAsync(server), "Could not start main server")
	// This publishes the service port for use in unit tests.
	mainServerAddr <- server.Addr

	select {
	case <-ctx.Done():
		// This currently only executes when the context is cancelled
		// by unit tests.  It does not yet execute in production.
		log.Println("Shutting down servers")
		ctx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
		defer cancel()
		start := time.Now()
		eg := errgroup.Group{}
		eg.Go(func() error {
			return server.Shutdown(ctx)
		})
		eg.Go(func() error {
			return promServer.Shutdown(ctx)
		})
		eg.Wait()
		log.Println("Shutdown took", time.Since(start))
		return &eg
	}
}

func main() {
	defer mainCancel()
	fmt.Println("Version:", etl.Version, "GitCommit:", etl.GitCommit)

	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Could not get args from env")

	go site.MustReload(mainCtx)

	// Enable block profiling
	runtime.SetBlockProfileRate(1000000) // One event per msec.

	maxInFlight = (int32)(*maxWorkers)
	// TODO: eliminate global variables in favor of config/env object.
	etl.IsBatch = *isBatch
	etl.OmitDeltas = *omitDeltas
	etl.GCloudProject = *gcloudProject
	etl.BigqueryProject = *bigqueryProject
	etl.BigqueryDataset = *bigqueryDataset
	etl.BatchAnnotatorURL = annotatorURL.String() + "/batch_annotate"

	if len(*gardenerHost) > 0 {
		log.Println("Using", *gardenerHost)
		minPollingInterval := 10 * time.Second
		gardenerAPI = mustGardenerAPI(mainCtx, *gardenerHost)
		// Note that this does not currently track duration metric.
		go gardenerAPI.Poll(mainCtx, toRunnable, (int)(*maxActiveTasks), minPollingInterval)
	} else {
		log.Println("GARDENER_HOST not specified or empty.  Running in passive mode.")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", Status)
	mux.HandleFunc("/status", Status)
	mux.HandleFunc("/worker", metrics.DurationHandler("generic", handleRequest))
	mux.HandleFunc("/_ah/health", healthCheckHandler) // legacy
	mux.HandleFunc("/alive", healthCheckHandler)
	mux.HandleFunc("/ready", healthCheckHandler)

	// Registers handler for v2 datatypes. Works with "local" output for local development.
	mux.HandleFunc("/v2/worker", handleLocalRequest)

	_ = startServers(mainCtx, mux)
}
