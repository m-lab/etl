// Sample
package main

import (
	"fmt"
	"net/http"
	//	"google.golang.org/appengine"
	//	"google.golang.org/appengine/datastore"
	//	"google.golang.org/appengine/log"
	//	"google.golang.org/appengine/taskqueue"
)

// Task Queue can always submit to an admin restricted URL.
//   login: admin
// Return 200 status code.
// Track reqeusts that last longer than 24 hrs.
// Is task handling idempotent?

// Useful headers added by AppEngine when sending Tasks.
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
	fmt.Fprintf(w, `{"message": "Hello world!"}`)
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "ok")
}

func main() {
	http.HandleFunc("/", handler)
	http.HandleFunc("/worker", worker)
	http.HandleFunc("/_ah/health", healthCheckHandler)
	http.ListenAndServe("localhost:8080", nil)
}
