package main

import (
	"bytes"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"testing"

	"google.golang.org/api/option"
)

func ClientOpts() []option.ClientOption {
	opts := []option.ClientOption{}
	if os.Getenv("TRAVIS") != "" {
		authOpt := option.WithCredentialsFile("../../travis-testing.key")
		opts = append(opts, authOpt)
	}
	return opts
}

// ResetFlags restores the command line flags to their default
// values.  This does NOT reset the result of Parsed().
func ResetFlags() {
	flag.VisitAll(func(f *flag.Flag) {
		f.Value.Set(f.DefValue)
	})
}

type countingHTTP struct {
	count   int
	lastURL string
}

func (h *countingHTTP) Get(url string) (resp *http.Response, err error) {
	h.count++
	h.lastURL = url
	resp = &http.Response{}
	resp.Body = ioutil.NopCloser(bytes.NewReader([]byte{}))
	resp.Status = "200 OK"
	resp.StatusCode = 200
	return
}

// NOTE: This test depends on the contents of the test-archive-mlab-sandbox
// bucket.  If it fails, check whether that bucket has been modified.
func xExample_day() {
	log.SetFlags(0)
	flag.CommandLine.Parse([]string{
		"-queue", "base-", "-dry_run", "-bucket=test-archive-mlab-sandbox",
		"-day=2017/09/24"})
	defer ResetFlags()

	log.SetOutput(os.Stdout) // Redirect to stdout so test framework sees it.
	log.SetOutput(os.Stderr) // restore
	// Output:
	// Adding  ndt/2017/09/24/  to  base-1
	// Added  76  tasks to  base-1
}
