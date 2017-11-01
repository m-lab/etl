// A microservice that accepts HTTP requests, creates a Task from given
// parameters, and adds the Task to a Push TaskQueue.
package pushqueue

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/storage"
	"google.golang.org/appengine"
	"google.golang.org/appengine/taskqueue"
)

const defaultMessage = "<html><body>This is not the app you're looking for.</body></html>"

// The following queues should not be directly addressed.
var queueForType = map[etl.DataType]string{
	etl.NDT: "etl-ndt-queue",
	etl.SS:  "etl-sidestream-queue",
	etl.PT:  "etl-traceroute-queue",
	etl.SW:  "etl-disco-queue",
}

// Disallow any queue name that is an automatic queue target.
func isDirectQueueNameOK(name string) bool {
	for _, value := range queueForType {
		if value == name {
			return false
		}
	}
	return true
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

	// TODO(dev): maybe this should be made more efficient?
	validQueue := false
	for _, queue := range queueForType {
		validQueue = validQueue || (queuename == queue)
	}
	if !validQueue {
		// TODO(dev): return a list of valid queues
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

func receiver(w http.ResponseWriter, r *http.Request) {
	receiverWithTestBypass(false, w, r)
}

// receiver accepts a GET request, and transforms the given parameters into a TaskQueue Task.
func receiverWithTestBypass(bypass bool, w http.ResponseWriter, r *http.Request) {
	// TODO(dev): require a POST instead of working with both POST and GET
	// after we update the Cloud Function to use POST.
	filename := r.FormValue("filename")
	if filename == "" {
		http.Error(w, `{"message": "No filename provided"}`, http.StatusBadRequest)
		return
	}

	decodedFilename, err := storage.GetFilename(filename)
	if err != nil {
		http.Error(w, `{"message": "Could not base64decode filename"}`, http.StatusBadRequest)
		return
	}

	// Validate filename.
	fnData, err := etl.ValidateTestPath(decodedFilename)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"message": "Invalid filename."}`)
		return
	}

	// determine correct queue based on parameter or file name.
	var ok bool
	queuename := r.FormValue("queue")
	if queuename != "" {
		ok = isDirectQueueNameOK(queuename)
	} else {
		queuename, ok = queueForType[fnData.GetDataType()]
	}

	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"message": "Invalid queuename."}`)
		return
	}

	// Skip queue if bypass for test.
	if bypass {
		fmt.Fprintf(w, `{"message": "StatusOK", "queue": %s, "filename": "filename"}`, queuename)
		return
	}

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
}
