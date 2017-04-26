// Sample
package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/storage"
	"github.com/m-lab/etl/task"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	// Enable profiling. For more background and usage information, see:
	//   https://blog.golang.org/profiling-go-programs
	"net/http/pprof"
	// Enable exported debug vars.  See https://golang.org/pkg/expvar/
	_ "expvar"
)

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
		return etl.InvalidData
	}
	switch fields[2] {
	case "ndt":
		return etl.NDTData
	case "sidestream":
		return etl.SSData
	case "paris-traceroute":
		return etl.PTData
	case "switch":
		return etl.SWData
	default:
		return etl.InvalidData
	}
}

func getInserter(dt etl.DataType, fake bool) (etl.Inserter, error) {
	return bq.NewInserter(
		etl.InserterParams{"mlab_sandbox", etl.TableNames[dt], 10 * time.Second, 100}, nil)
}

func getParser(dt etl.DataType, ins etl.Inserter) etl.Parser {
	switch dt {
	case etl.NDTData:
		// TODO - substitute appropriate parsers here and below.
		return parser.NewTestParser(ins)
	case etl.SSData:
		return parser.NewTestParser(ins)
	case etl.PTData:
		return parser.NewTestParser(ins)
	case etl.SWData:
		return parser.NewTestParser(ins)
	default:
		return nil
	}
}

func worker(w http.ResponseWriter, r *http.Request) {
	workerCount.Inc()
	defer workerCount.Dec()

	r.ParseForm()
	// Log request data.
	for key, value := range r.Form {
		log.Printf("Form:   %q == %q\n", key, value)
	}

	// This handles base64 encoding, and requires a gs:// prefix.
	fn, err := getFilename(r.FormValue("filename"))
	if err != nil {
		fmt.Fprintf(w, `{"message": "Invalid filename."}`)
		w.WriteHeader(http.StatusBadRequest)
		log.Printf("Invalid filename: %s\n", fn)
		return
	}

	dataType := getDataType(fn)
	if dataType == etl.InvalidData {
		fmt.Fprintf(w, `{"message": "Invalid filename."}`)
		w.WriteHeader(http.StatusBadRequest)
		log.Printf("Invalid filename: %s\n", fn)
		return
	}

	// TODO(dev): log the originating task queue name from headers.
	log.Printf("Received filename: %q\n", fn)

	client, err := storage.GetStorageClient(false)
	if err != nil {
		fmt.Fprintf(w, `{"message": "Could not create client."}`)
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	tr, err := storage.NewETLSource(client, fn)
	if err != nil {
		fmt.Fprintf(w, `{"message": "Problem downloading file."}`)
		w.WriteHeader(http.StatusInternalServerError)
		log.Printf("%v", err)
		return
		// TODO - anything better we could do here?
	}
	defer tr.Close()

	// TODO(dev) Use a more thoughtful setting for buffer size.
	// For now, 10K per row times 100 results is 1MB, which is an order of
	// magnitude below our 10MB max, so 100 might not be such a bad
	// default.
	ins, err := getInserter(dataType, false)
	if err != nil {
		log.Printf("%v", err)
		fmt.Fprintf(w, `{"message": "Problem creating BQ inserter."}`)
		w.WriteHeader(http.StatusInternalServerError)
		return
		// TODO - anything better we could do here?
	}
	// Create parser, injecting Inserter
	p := getParser(dataType, ins)
	tsk := task.NewTask(fn, tr, p, ins, "with_meta")

	tsk.ProcessAllTests()

	// TODO - if there are any errors, consider sending back a meaningful response
	// for web browser and queue-pusher debugging.
	fmt.Fprintf(w, `{"message": "Success"}`)
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

	// We also setup another prometheus handler on a non-standard path. This
	// path name will be accessible through the AppEngine service address,
	// however it will be served by a random instance.
	http.Handle("/random-metrics", promhttp.Handler())
	http.ListenAndServe(":8080", nil)
}

//=====================================================================================
//                       Prometheus Monitoring
//=====================================================================================

var (
	workerCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "etl_parser_worker_count",
		Help: "Number of active workers.",
	})
)

func init() {
	prometheus.MustRegister(workerCount)
}
