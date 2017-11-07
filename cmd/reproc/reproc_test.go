package main

import (
	"bytes"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"testing"
	"time"
)

// ResetFlags restores the command line flags to their default
// values.  This does NOT reset the result of Parsed().
func ResetFlags() {
	flag.VisitAll(func(f *flag.Flag) {
		f.Value.Set(f.DefValue)
	})
}

func Test_queueFor(t *testing.T) {
	// Set the flag values?
	flag.CommandLine.Parse([]string{"-queue", "base-", "-dry_run"})
	defer ResetFlags()

	tme, _ := time.Parse("2006/01/02", "2017/09/01")
	queue := queueFor(tme)
	if queue != "base-2" {
		t.Error("bad queue: ", queue)
	}
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
func Test_month(t *testing.T) {
	flag.CommandLine.Parse([]string{
		"-queue", "base-", "-dry_run", "-bucket=test-archive-mlab-sandbox",
		"-month=2017/10"})
	defer ResetFlags()

	fake := countingHTTP{}
	setup(&fake)
	run()
	if fake.count != 100 {
		t.Error("Should have made 100 http requests.")
	}
}

// NOTE: This test depends on the contents of the test-archive-mlab-sandbox
// bucket.  If it fails, check whether that bucket has been modified.
func Example_month() {
	log.SetFlags(0)
	flag.CommandLine.Parse([]string{
		"-queue", "base-", "-dry_run", "-bucket=test-archive-mlab-sandbox",
		"-month=2017/10"})
	defer ResetFlags()

	log.SetOutput(os.Stdout) // Redirect to stdout so test framework sees it.
	setup(nil)
	run()
	log.SetOutput(os.Stderr) // restore
	// Unordered output:
	// Adding  ndt/2017/10/28/  to  base-3
	// Adding  ndt/2017/10/29/  to  base-4
	// Added  10  tasks to  base-3
	// Added  90  tasks to  base-4
}

// NOTE: This test depends on the contents of the test-archive-mlab-sandbox
// bucket.  If it fails, check whether that bucket has been modified.
func Example_day() {
	log.SetFlags(0)
	flag.CommandLine.Parse([]string{
		"-queue", "base-", "-dry_run", "-bucket=test-archive-mlab-sandbox",
		"-day=2017/09/24"})
	defer ResetFlags()

	log.SetOutput(os.Stdout) // Redirect to stdout so test framework sees it.
	setup(nil)
	run()
	log.SetOutput(os.Stderr) // restore
	// Output:
	// Adding  ndt/2017/09/24/  to  base-1
	// Added  76  tasks to  base-1
}
