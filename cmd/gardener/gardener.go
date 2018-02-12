// Package main defines a service for handling various post-processing
// and house-keeping tasks associated with the pipelines.
// Most tasks will be run periodically, but some may be triggered
// by URL requests from authorized sources.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/m-lab/etl/batch"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ###############################################################################
// All DataStore related code and variables
// ###############################################################################
const (
	dsNamespace   = "gardener"
	dsKind        = "gardener"
	batchStateKey = "batch-state"
)

var dsClient *datastore.Client

func getDSClient() (*datastore.Client, error) {
	var err error
	var client *datastore.Client
	if dsClient == nil {
		project, ok := os.LookupEnv("PROJECT")
		if !ok {
			return dsClient, errors.New("PROJECT env var not set")
		}
		client, err = datastore.NewClient(context.Background(), project)
		if err == nil {
			dsClient = client
		}
	}
	return dsClient, err
}

// Load retrieves an arbitrary record from datastore.
func Load(name string, obj interface{}) error {
	var client *datastore.Client
	client, err := getDSClient()
	if err != nil {
		return err
	}
	k := datastore.NameKey(dsKind, name, nil)
	k.Namespace = dsNamespace
	return client.Get(context.Background(), k, obj)
}

// Save stores an arbitrary object to kind/key in the default namespace.
// If a record already exists, then it is overwritten.
// TODO(gfr) Make an upsert version of this:
// https://cloud.google.com/datastore/docs/concepts/entities
func Save(key string, obj interface{}) error {
	client, err := getDSClient()
	if err != nil {
		return err
	}
	k := datastore.NameKey(dsKind, key, nil)
	k.Namespace = dsNamespace
	_, err = client.Put(context.Background(), k, obj)
	return err
}

// ###############################################################################
//  Batch processing task scheduling and support code
// ###############################################################################

// Persistent Queuer for use in handlers and gardener tasks.
var batchQueuer batch.Queuer

// QueueState holds the state information for each batch queue.
type QueueState struct {
	// Per queue, indicate which day is being processed in
	// that queue.  No need to process more than one day at a time.
	// The other queues will take up the slack while we add more
	// tasks when one queue is emptied.

	QueueName        string // Name of the batch queue.
	NextTask         string // Name of task file currently being enqueued.
	PendingPartition string // FQ Name of next table partition to be added.
}

// BatchState holds the entire batch processing state.
// It holds the state locally, and also is stored in DataStore for
// recovery when the instance is restarted, e.g. for weekly platform
// updates.
// At any given time, we restrict ourselves to a 14 day reprocessing window,
// and finish the beginning of that window before reprocessing any dates beyond it.
// When we determine that the first date in the window has been submitted for
// processing, we advance the window up to the next pending date.
type BatchState struct {
	Hostname       string       // Hostname of the gardener that saved this.
	InstanceID     string       // instance ID of the gardener that saved this.
	WindowStart    time.Time    // Start of two week window we are working on.
	QueueBase      string       // base name for queues.
	QStates        []QueueState // States for each queue.
	LastUpdateTime time.Time    // Time of last update.  (Is this in DS metadata?)
}

// MaybeScheduleMoreTasks will look for an empty task queue, and if it finds one, will look
// for corresponding days to add to the queue.
// Alternatively, it may look first for the N oldest days to be reprocessed, and will then
// check whether any of the task queues for those oldest days is empty, and conditionally add tasks.
func MaybeScheduleMoreTasks(queuer *batch.Queuer) {
	// GetTaskQueueDepth returns the number of pending items in a task queue.
	stats, err := queuer.GetTaskqueueStats()
	if err != nil {
		log.Println(err)
	} else {
		for k, v := range stats {
			if len(v) > 0 && v[0].Tasks == 0 && v[0].InFlight == 0 {
				log.Printf("Ready: %s: %v\n", k, v[0])
				// Should add more tasks now.
			}
		}
	}
}

// queuerFromEnv creates a Queuer struct initialized from environment variables.
// It uses TASKFILE_BUCKET, PROJECT, QUEUE_BASE, and NUM_QUEUES.
func queuerFromEnv() (batch.Queuer, error) {
	bucketName, ok := os.LookupEnv("TASKFILE_BUCKET")
	if !ok {
		return batch.Queuer{}, errors.New("TASKFILE_BUCKET not set")
	}
	project, ok := os.LookupEnv("PROJECT")
	if !ok {
		return batch.Queuer{}, errors.New("PROJECT not set")
	}
	queueBase, ok := os.LookupEnv("QUEUE_BASE")
	if !ok {
		return batch.Queuer{}, errors.New("QUEUE_BASE not set")
	}
	numQueues, err := strconv.Atoi(os.Getenv("NUM_QUEUES"))
	if err != nil {
		log.Println(err)
		return batch.Queuer{}, errors.New("Parse error on NUM_QUEUES")
	}

	return batch.CreateQueuer(http.DefaultClient, nil, queueBase, numQueues, project, bucketName, false)
}

// StartDateRFC3339 is the date at which reprocessing will start when it catches
// up to present.  For now, we are making this the beginning of the ETL timeframe,
// until we get annotation fixed to use the actual data date instead of NOW.
const StartDateRFC3339 = "2017-05-01T00:00:00Z00:00"

// startupBatch determines whether some other instance has control, and
// assumes control if not.
func startupBatch(base string, numQueues int) (BatchState, error) {
	hostname := os.Getenv("HOSTNAME")
	instance := os.Getenv("GAE_INSTANCE")
	queues := make([]QueueState, numQueues)
	var bs BatchState
	err := Load(batchStateKey, &bs)
	if err != nil {
		startDate, err := time.Parse(time.RFC3339, StartDateRFC3339)
		if err != nil {
			log.Println("Could not parse start time.  Not starting batch.")
			return bs, err
		}
		bs = BatchState{hostname, instance, startDate, base, queues, time.Now()}

	} else {
		// TODO - should check whether we should take over, or leave alone.
	}

	err = Save(batchStateKey, &bs)
	return bs, err
}

// ###############################################################################
//  Top level service control code.
// ###############################################################################

// periodic will run approximately every 5 minutes.
func periodic() {
	_, err := startupBatch(batchQueuer.QueueBase, batchQueuer.NumQueues)
	if err != nil {
		log.Fatal(err)
	}
	for {
		log.Println("Periodic is running")

		MaybeScheduleMoreTasks(&batchQueuer)

		// There is no need for randomness, since this is a singleton handler.
		time.Sleep(300 * time.Second)
	}
}

func setupPrometheus() {
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
}

// Status provides basic information about the service.  For now, it is just
// configuration and version info.  In future it will likely include more
// dynamic information.
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

	env := os.Environ()
	for i := range env {
		fmt.Fprintf(w, "%s</br>\n", env[i])
	}
	fmt.Fprintf(w, "</body></html>\n")
}

var healthy = false

// healthCheck, for now, used for both /ready and /alive.
func healthCheck(w http.ResponseWriter, r *http.Request) {
	if !healthy {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"message": "Internal server error."}`)
	}
	fmt.Fprint(w, "ok")
}

// runService starts a service handler and runs forever.
// The configuration info comes from environment variables.
func runService() {
	// Enable block profiling
	runtime.SetBlockProfileRate(1000000) // One event per msec.

	setupPrometheus()
	// We also setup another prometheus handler on a non-standard path. This
	// path name will be accessible through the AppEngine service address,
	// however it will be served by a random instance.
	http.Handle("/random-metrics", promhttp.Handler())
	http.HandleFunc("/", Status)
	http.HandleFunc("/status", Status)

	http.HandleFunc("/alive", healthCheck)
	http.HandleFunc("/ready", healthCheck)

	var err error
	batchQueuer, err = queuerFromEnv()
	if err == nil {
		healthy = true
		log.Println("Running as a service.")

		// Run the background "periodic" function.
		go periodic()
	} else {
		// Leaving healthy == false
		// This will cause app-engine to roll back.
		log.Println(err)
		log.Println("Required environment variables are missing or invalid.")
	}

	// ListenAndServe, and terminate when it returns.
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// ###############################################################################
//  Top level command line code.
// ###############################################################################

// These are used only for command line.  For service, environment variables are used
// for general parameters, and request parameter for month.
var (
	fProject = flag.String("project", "", "Project containing queues.")
	fQueue   = flag.String("queue", "etl-ndt-batch-", "Base of queue name.")
	// TODO implement listing queues to determine number of queue, and change this to 0
	fNumQueues = flag.Int("num_queues", 8, "Number of queues.  Normally determined by listing queues.")
	// Gardener will only read from this bucket, so its ok to use production bucket as default.
	fBucket = flag.String("bucket", "archive-mlab-oti", "Source bucket.")
	fExper  = flag.String("experiment", "ndt", "Experiment prefix, without trailing slash.")
	fMonth  = flag.String("month", "", "Single month spec, as YYYY/MM")
	fDay    = flag.String("day", "", "Single day spec, as YYYY/MM/DD")
	fDryRun = flag.Bool("dry_run", false, "Prevents all output to queue_pusher.")
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	// Check if invoked as a service.
	isService, _ := strconv.ParseBool(os.Getenv("GARDENER_SERVICE"))
	if isService {
		runService()
		return
	}

	// Otherwise this is a command line invocation...
	flag.Parse()
	// Check that either project or dry-run is set.
	// If dry-run, it is ok for the project to be unset, as the URLs
	// only are seen by a fake http client.
	if *fProject == "" && !*fDryRun {
		log.Println("Must specify project (or --dry_run)")
		flag.PrintDefaults()
		return
	}

	q, err := batch.CreateQueuer(http.DefaultClient, nil, *fQueue, *fNumQueues, *fProject, *fBucket, *fDryRun)
	if err != nil {
		// In command line mode, good to just fast fail.
		log.Fatal(err)
	}
	if *fMonth != "" {
		q.PostMonth(*fExper + "/" + *fMonth + "/")
	} else if *fDay != "" {
		q.PostDay(nil, *fExper+"/"+*fDay+"/")
	}
}
