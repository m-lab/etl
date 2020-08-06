// Package pushqueue provides a microservice that accepts HTTP requests, creates
// a Task from given parameters, and adds the Task to a TaskQueue.
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/m-lab/etl/etl"

	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
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

// A default handler for root path.
func defaultHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		// TODO - this is actually returning StatusOK.  Weird!
		http.Error(w, `{"message": "Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	fmt.Fprintf(w, defaultMessage)
}

// queueStats provides statistics for a given queue.
func queueStats(w http.ResponseWriter, r *http.Request) {
	queuename := r.FormValue("queuename")
	test := r.FormValue("test-bypass")

	if queuename == "" {
		http.Error(w, `{"message": "Bad request parameters"}`, http.StatusBadRequest)
		ctx := appengine.NewContext(r)
		log.Errorf(ctx, "%+v\n", w)
		return
	}

	// Bypass action if test mode.
	if test != "" {
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
	// TODO(dev): require a POST instead of working with both POST and GET
	// after we update the Cloud Function to use POST.
	filename := r.FormValue("filename")
	if filename == "" {
		http.Error(w, `{"message": "No filename provided"}`, http.StatusBadRequest)
		return
	}

	decodedFilename, err := etl.GetFilename(filename)
	if err != nil {
		http.Error(w, `{"message": "Could not base64decode filename"}`, http.StatusBadRequest)
		return
	}

	// Validate filename.
	fnData, err := etl.ValidateTestPath(decodedFilename)
	if err != nil {
		ctx := appengine.NewContext(r)
		log.Errorf(ctx, "%v\n", err)
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

	// Lots of files will be archived that should not be enqueued. Pass
	// over those files without comment.
	// TODO(dev) count how many names we skip over using prometheus
	if ok {
		params := url.Values{"filename": []string{filename}}
		t := taskqueue.NewPOSTTask("/worker", params)
		test := r.FormValue("test-bypass")
		if test == "" {
			// Skip queuing if bypass for test.
			ctx := appengine.NewContext(r)
			if _, err := taskqueue.Add(ctx, t, queuename); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

		}
	}
}

func main() {
	http.HandleFunc("/", defaultHandler)
	http.HandleFunc("/receiver", receiver)
	http.HandleFunc("/stats", queueStats)
	appengine.Main()
}
