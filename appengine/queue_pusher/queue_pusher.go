// A microservice that accepts HTTP requests, creates a Task from given
// parameters, and adds the Task to a Push TaskQueue.
package pushqueue

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"google.golang.org/appengine"
	"google.golang.org/appengine/taskqueue"
)

const defaultMessage = "<html><body>This is not the app you're looking for.</body></html>"

// Requests can only add tasks to one of these whitelisted queue names.
var queueWhitelist = map[string]bool{
	"etl-parser-queue":           true,
	"etl-ndt-queue":              true,
	"etl-sidestream-queue":       true,
	"etl-paris-traceroute-queue": true,
}

func init() {
	http.HandleFunc("/", defaultHandler)
	http.HandleFunc("/receiver", receiver)
	http.HandleFunc("/stats", queueStats)
}

// A default handler for root path.
func defaultHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	fmt.Fprintf(w, defaultMessage)
}

// queueStats provides statistics for a given queue.
func queueStats(w http.ResponseWriter, r *http.Request) {
	queuename := r.FormValue("queuename")

	if queuename == "" {
		http.Error(w, `{"message": "Bad request parameters"}`, http.StatusBadRequest)
		return
	}

	if _, ok := queueWhitelist[queuename]; !ok {
		// TODO(dev): return the queueWhitelist to client.
		http.Error(w, `{"message": "Given queue name is not acceptable"}`, http.StatusNotAcceptable)
		return
	}

	// Get stats.
	ctx := appengine.NewContext(r)
	stats, err := taskqueue.QueueStats(ctx, []string{queuename})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Return stats to client.
	b, err := json.Marshal(stats)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, string(b))
}

// receiver accepts a GET request, and transforms the given parameters into a TaskQueue Task.
func receiver(w http.ResponseWriter, r *http.Request) {
	// TODO(dev): require a POST instead of a GET.
	if r.Method != http.MethodGet {
		http.Error(w, `{"message": "Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	filename := r.FormValue("filename")

	// TODO(dev): determine correct queue based on file type.
	queuename := "etl-parser-queue"

	if filename == "" {
		http.Error(w, `{"message": "Bad request parameters"}`, http.StatusBadRequest)
		return
	}

	ctx := appengine.NewContext(r)
	params := url.Values{"filename": []string{filename}}
	t := taskqueue.NewPOSTTask("/worker", params)
	if _, err := taskqueue.Add(ctx, t, queuename); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
