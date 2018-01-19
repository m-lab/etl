// Package main defines a service for submitting date ranges for
// reprocessing.  This will generally be triggered by an internal
// timer, but specific ranges can also be reprocessed by submitting
// URL requests from authorized sources.

package main

/*
Strategies...
  1. Work from a month prefix, but explicitly iterate over days.
      maybe use a separate goroutine for each date? (DONE)
  2. Work from a prefix, or range of prefixes.
  3. Work from a date range

*/

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"regexp"
	"runtime"
	"strconv"

	"github.com/m-lab/etl/batch"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

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

// QueuerFromEnv creates a Queuer struct initialized from environment variables.
// It uses TASKFILE_BUCKET, PROJECT, QUEUE_BASE, and NUM_QUEUES.
func QueuerFromEnv() (batch.Queuer, error) {
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

var (
	monthRegex = regexp.MustCompile(`(?P<exp>[^/]*)/(?P<yyyymm>\d{4}[/-][01]\d/)`)
)

// Month handles month format requests.
// example http://localhost:8080/reproc/month?prefix=ndt/2017/09/
// TODO - use flusher, ok := w.(http.Flusher) to send partial result updates.
func Month(rwr http.ResponseWriter, rq *http.Request) {
	rq.ParseForm()
	// Log request data.
	for key, value := range rq.Form {
		log.Printf("Form:   %q == %q\n", key, value)
	}

	rawPrefix := rq.FormValue("prefix")
	fields := monthRegex.FindStringSubmatch(rawPrefix)
	log.Printf("%+v\n", fields)
	if fields == nil {
		fmt.Fprintf(rwr, "Invalid prefix %s\n", rawPrefix)
		return
	}

	cronQueuer.PostMonth(rawPrefix)
	// rwr.WriteHeader("ok")
	fmt.Fprintf(rwr, "Processed %s\n", rawPrefix)
	log.Println("Done")
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	// TODO(soltesz): provide a real health check.
	fmt.Fprint(w, "ok")
}

// Persistent Queuer for use in handlers.
var cronQueuer batch.Queuer

// For service, the configuration info comes from environment variables.
func runService() {
	// Enable block profiling
	runtime.SetBlockProfileRate(1000000) // One event per msec.

	setupPrometheus()

	var err error
	cronQueuer, err = QueuerFromEnv()
	if err != nil {
		log.Println(err)
		log.Fatal("Required environment variables are missing or invalid.")
	}
	log.Println("Running as a service.")
	http.HandleFunc("/_ah/health", healthCheckHandler)
	http.HandleFunc("/", Status)
	http.HandleFunc("/status", Status)
	http.HandleFunc("/reproc/month", Month)

	// We also setup another prometheus handler on a non-standard path. This
	// path name will be accessible through the AppEngine service address,
	// however it will be served by a random instance.
	http.Handle("/random-metrics", promhttp.Handler())
	http.ListenAndServe(":8080", nil)
}

// These are used for command line.
var (
	fProject = flag.String("project", "", "Project containing queues.")
	fQueue   = flag.String("queue", "etl-ndt-batch-", "Base of queue name.")
	// TODO implement listing queues to determine number of queue, and change this to 0
	fNumQueues = flag.Int("num_queues", 8, "Number of queues.  Normally determined by listing queues.")
	fBucket    = flag.String("bucket", "archive-mlab-oti", "Source bucket.")
	fExper     = flag.String("experiment", "ndt", "Experiment prefix, without trailing slash.")
	fMonth     = flag.String("month", "", "Single month spec, as YYYY/MM")
	fDay       = flag.String("day", "", "Single day spec, as YYYY/MM/DD")
	fDryRun    = flag.Bool("dry_run", false, "Prevents all output to queue_pusher.")
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	// Check if invoked as a service.
	cron, _ := strconv.ParseBool(os.Getenv("CRON_SERVICE"))
	if cron {
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
		log.Fatal(err)
	}
	if *fMonth != "" {
		q.PostMonth(*fExper + "/" + *fMonth + "/")
	} else if *fDay != "" {
		q.PostDay(nil, *fExper+"/"+*fDay+"/")
	}
}
