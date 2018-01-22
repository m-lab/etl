// +build integration

package batch_test

// These tests get lists of files from archive-mlab-test bucket,
// but use a fake http client to avoid posting to task queues.
//
// When run on travis, these tests require the cloud-test service
// account, or some other service account with permission to list
// storage bucket test-archive-mlab-sandbox.

import (
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/m-lab/etl/batch"
	"google.golang.org/api/option"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// ResetFlags restores the command line flags to their default
// values.  This does NOT reset the result of Parsed().
func ResetFlags() {
	flag.VisitAll(func(f *flag.Flag) {
		f.Value.Set(f.DefValue)
	})
}

type countingTransport struct {
	count   int
	lastReq *http.Request
}

type nopCloser struct {
	io.Reader
}

func (nc *nopCloser) Close() error { return nil }

// RoundTrip implements the RoundTripper interface, logging the
// request, and the response body, (which may be json).
func (t *countingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Using %#v results in an escaped string we can use in code.
	t.lastReq = req
	t.count++
	resp := &http.Response{}
	resp.StatusCode = http.StatusOK
	resp.Body = &nopCloser{strings.NewReader("ok")}
	return resp, nil
}

// NewLoggingClient returns an HTTP client that also logs all requests
// and responses.
func newCountingClient() (*http.Client, *countingTransport) {
	client := &http.Client{}
	tp := &countingTransport{}
	client.Transport = tp
	return client, tp
}

func Options() []option.ClientOption {
	opts := []option.ClientOption{}
	if os.Getenv("TRAVIS") != "" {
		authOpt := option.WithCredentialsFile("../travis-testing.key")
		opts = append(opts, authOpt)
	}

	return opts
}

func TestPostDay(t *testing.T) {
	fake, tp := newCountingClient()
	q, err := batch.CreateQueuer(fake, Options(), "test-", 8, "fake-project", "archive-mlab-test", true)
	if err != nil {
		t.Fatal(err)
	}
	q.PostDay(nil, "ndt/2017/09/24/")
	if tp.count != 76 {
		t.Errorf("Should have made 76 http requests: %d\n", tp.count)
	}
}

func TestPostMonth(t *testing.T) {
	fake, tp := newCountingClient()
	q, err := batch.CreateQueuer(fake, Options(), "test-", 8, "fake-project", "archive-mlab-test", true)
	if err != nil {
		t.Fatal(err)
	}
	err = q.PostMonth("ndt/2017/10/")
	if err != nil {
		t.Fatal(err)
	}
	if tp.count != 100 {
		t.Errorf("Should have made 100 http requests: %d\n", tp.count)
	}
}

// NOTE: This test depends on the contents of the test-archive-mlab-sandbox
// bucket.  If it fails, check whether that bucket has been modified.
func ExampleMonth() {
	log.SetFlags(0)

	log.SetOutput(os.Stdout) // Redirect to stdout so test framework sees it.
	fake, _ := newCountingClient()
	q, err := batch.CreateQueuer(fake, Options(), "test-", 8, "fake-project", "archive-mlab-test", true)
	if err != nil {
		log.Println(err)
	}
	err = q.PostMonth("ndt/2017/10/")
	if err != nil {
		log.Println(err)
	}

	log.SetOutput(os.Stderr) // restore
	// Unordered output:
	// Adding  ndt/2017/10/28/  to  test-3
	// Adding  ndt/2017/10/29/  to  test-4
	// Added  10  tasks to  test-3
	// Added  90  tasks to  test-4
}

// NOTE: This test depends on the contents of the test-archive-mlab-sandbox
// bucket.  If it fails, check whether that bucket has been modified.
func ExampleDay() {
	log.SetFlags(0)

	log.SetOutput(os.Stdout) // Redirect to stdout so test framework sees it.
	fake, _ := newCountingClient()
	q, err := batch.CreateQueuer(fake, Options(), "test-", 8, "fake-project", "archive-mlab-test", true)
	if err != nil {
		log.Println(err)
	}
	err = q.PostDay(nil, "ndt/2017/09/24/")
	if err != nil {
		log.Println(err)
	}
	log.SetOutput(os.Stderr) // restore
	// Output:
	// Adding  ndt/2017/09/24/  to  test-1
	// Added  76  tasks to  test-1
}
