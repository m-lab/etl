package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"time"

	gcs "cloud.google.com/go/storage"

	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/rtx"

	"github.com/m-lab/etl/active"
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
		Options: []string{"local"},
		Value:   "local",
	}

	gcsPath        = flag.String("gcsPath", "", "GCS path to archive to process")
	maxActiveTasks = flag.Int64("max_active", 1, "Maximum number of active tasks")

	servicePort     = flag.String("service_port", ":8080", "The main (private) service port")
	shutdownTimeout = flag.Duration("shutdown_timeout", 1*time.Minute, "Graceful shutdown time allowance")
	gcloudProject   = flag.String("gcloud_project", "", "GCP Project id")
	maxWorkers      = flag.Int("max_workers", defaultMaxInFlight, "Maximum number of workers")
	omitDeltas      = flag.Bool("ndt_omit_deltas", false, "Whether to skip ndt.web100 snapshot deltas")
	outputDir       = flag.String("output_dir", "./output", "If output type is 'local', write output to this directory")
	annotatorURL    = flagx.MustNewURL("https://annotator-dot-mlab-sandbox.appspot.com")
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

func processGCSArchive(fn string) error {
	dp, err := etl.ValidateTestPath(fn)
	if err != nil {
		return err
	}

	c, err := storage.GetStorageClient(false)
	if err != nil {
		return err
	}

	ctx := context.Background()
	obj, err := c.Bucket(dp.Bucket).Object(dp.Path).Attrs(ctx)
	if err != nil {
		log.Println(err)
		return err
	}

	r, err := toRunnable(obj)
	if err != nil {
		return err
	}
	err = r.Run(ctx)
	if err != nil {
		return err
	}

	return nil
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

func toRunnable(obj *gcs.ObjectAttrs) (active.Runnable, error) {
	c, err := storage.GetStorageClient(false)
	if err != nil {
		return nil, err
	}

	var sink factory.SinkFactory
	switch outputType.Value {
	case "local":
		sink = storage.NewLocalFactory(*outputDir)
	}

	taskFactory := worker.StandardTaskFactory{
		Annotator: factory.DefaultAnnotatorFactory(),
		Sink:      sink,
		Source:    storage.GCSSourceFactory(c),
	}
	return &runnable{&taskFactory, *obj}, nil
}

func main() {
	defer mainCancel()
	fmt.Println("Version:", etl.Version, "GitCommit:", etl.GitCommit)

	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Could not get args from env")

	// Enable block profiling
	runtime.SetBlockProfileRate(1000000) // One event per msec.

	maxInFlight = (int32)(*maxWorkers)
	// TODO: eliminate global variables in favor of config/env object.
	etl.IsBatch = false
	etl.OmitDeltas = *omitDeltas
	etl.GCloudProject = *gcloudProject
	etl.BatchAnnotatorURL = annotatorURL.String() + "/batch_annotate"

	err := processGCSArchive(*gcsPath)
	log.Println(err)
}
