// A microservice that accepts HTTP requests, creates a Task from given
// parameters, and adds the Task to a Push TaskQueue.
package pushqueue

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/storage"
	"google.golang.org/appengine"
	"google.golang.org/appengine/taskqueue"
)

const defaultMessage = "<html><body>This is not the app you're looking for.</body></html>"

// Requests can only add tasks to one of these whitelisted queue names.
var queueForType = map[etl.DataType]string {
	etl.NDTData: "etl-ndt-queue",
	etl.SSData: "etl-sidestream-queue",
	etl.PTData: "etl-traceroute-queue",
	etl.SWData: "etl-disco-queue",
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

	validQueue := false
	for _, queue := range queueForType {
		validQueue = validQueue || (queuename == queue)
	}
	if !validQueue {
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

// receiver accepts a GET or POST request, and transforms the given parameters into a TaskQueue Task.
func receiver(w http.ResponseWriter, r *http.Request) {
	// TODO(dev): require a POST instead of working with both POST and GET
	// after we update the Cloud Function to use POST.
	filename := r.FormValue("filename")
	if filename == "" {
		http.Error(w, `{"message": "No filename provided"}`, http.StatusBadRequest)
		return
	}

	decoded_filename, err := storage.GetFilename(filename)
	if err != nil {
		http.Error(w, `{"message": "Could not base64decode filename"}`, http.StatusBadRequest)
		return
	}

	// determine correct queue based on file name.
	queuename, ok := queueForType[storage.GetDataType(decoded_filename)]

	// Lots of files will be archived that should not be enqueued. Pass
	// over those files without comment.
	// TODO(dev) count how many names we skip over using prometheus
	if ok {
		ctx := appengine.NewContext(r)
		params := url.Values{"filename": []string{filename}}
		t := taskqueue.NewPOSTTask("/worker", params)
		if _, err := taskqueue.Add(ctx, t, queuename); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// determine correct queue based on file name.
	queuename := queueForFile(decoded_filename)

	// Lots of files will be archived that should not be enqueued. Pass
	// over those files without comment.
	if queuename != "" {
		ctx := appengine.NewContext(r)
		params := url.Values{"filename": []string{filename}}
		t := taskqueue.NewPOSTTask("/worker", params)
		if _, err := taskqueue.Add(ctx, t, queuename); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}
