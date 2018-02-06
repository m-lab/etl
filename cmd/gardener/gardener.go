// Package main defines a command line tool for submitting date
// ranges for reprocessing
package main

import (
	"flag"
	"log"
	"net/http"
	"net/http/pprof"

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

// These are used for command line.
var (
	fProject = flag.String("project", "", "Project containing queues.")
	fQueue   = flag.String("queue", "etl-ndt-batch-", "Base of queue name.")
	// TODO implement listing queues to determine number of queue, and change this to 0
	fNumQueues = flag.Int("num_queues", 8, "Number of queues.  Normally determined by listing queues.")
	fBucket    = flag.String("bucket", "", "Source bucket.")
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
