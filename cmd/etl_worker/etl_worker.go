// Sample
package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/storage"
	"github.com/m-lab/etl/task"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	// Enable profiling. For more background and usage information, see:
	//   https://blog.golang.org/profiling-go-programs
	_ "net/http/pprof"
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

func worker(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	// Log request data.
	for key, value := range r.Form {
		log.Printf("Form:   %q == %q\n", key, value)
	}

	// TODO(dev): log the originating task queue name from headers.
	log.Printf("Received filename: %q\n", r.FormValue("filename"))

	client, err := storage.GetStorageClient(false)
	if err != nil {
		fmt.Fprintf(w, `{"message": "Could not create client."}`)
		w.WriteHeader(503) // Service Unavailable
		return
	}

	// TODO(dev) Create reusable Client.
	tr, err := storage.NewGCSTarReader(client, r.FormValue("filename"))
	if err != nil {
		log.Printf("%v", err)
		log.Printf("Bailing out")
		fmt.Fprintf(w, `{"message": "Bailing out"}`)
		return
		// TODO - something better.
	}
	parser := new(parser.TestParser)
	ins, err := bq.NewInserter("mlab-sandbox", "mlab_sandbox", "test3")
	if err != nil {
		log.Printf("%v", err)
		log.Printf("Bailing out")
		fmt.Fprintf(w, `{"message": "Bailing out"}`)
		return
		// TODO - something better.
	}
	tsk := task.NewTask(tr, parser, ins, "test3")

	tsk.ProcessAllTests()
	tr.Close()

	// TODO - if there are any errors, consider sending back a meaningful response
	// for web browser and queue-pusher debugging.
	fmt.Fprintf(w, `{"message": "Success"}`)
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	// TODO(soltesz): provide a real health check.
	fmt.Fprint(w, "ok")
}

func main() {
	http.HandleFunc("/", handler)
	http.HandleFunc("/worker", metrics.DurationHandler("generic", worker))
	http.HandleFunc("/_ah/health", healthCheckHandler)

	// Assign the default prometheus handler to the standard exporter path.
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":8080", nil)
}
