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

var (
	fProject = flag.String("project", "", "Project containing queues.")
	fQueue   = flag.String("queue", "etl-ndt-batch-", "Base of queue name.")
	// TODO implement listing queues to determine number of queue, and change this to 0
	fNumQueues = flag.Int("num_queues", 8, "Number of queues.  Normally determined by listing queues.")
	fBucket    = flag.String("bucket", "archive-mlab-oti", "Source bucket.")
	fExper     = flag.String("experiment", "ndt", "Experiment prefix, without trailing slash.")
	fMonth     = flag.String("month", "", "Single month spec, as YYYY/MM")
	fDay       = flag.String("day", "", "Single day spec, as YYYY/MM/DD")
	fDryRun    = flag.Bool("dry_run", false, "Prevents all output to queue_pusher.")
)

func Options() []option.ClientOption {
	opts := []option.ClientOption{}
	if os.Getenv("TRAVIS") != "" {
		authOpt := option.WithCredentialsFile("../travis-testing.key")
		opts = append(opts, authOpt)
	}

	return opts
}

func TestPostDay(t *testing.T) {
	flag.CommandLine.Parse([]string{
		"-project", "fake-project", "-queue", "base-",
		"-bucket=archive-mlab-test", "-dry_run",
		"-day=2017/09/24"})
	defer ResetFlags()

	fake, tp := newCountingClient()
	q, err := batch.NewQueuer(fake, Options(), *fQueue, *fNumQueues, *fProject, *fBucket, *fDryRun)
	if err != nil {
		t.Fatal(err)
	}
	q.PostDay(nil, *fExper+"/"+*fDay+"/")
	if tp.count != 76 {
		t.Errorf("Should have made 76 http requests: %d\n", tp.count)
	}
}

func TestPostMonth(t *testing.T) {
	flag.CommandLine.Parse([]string{
		"-project", "fake-project", "-queue", "base-",
		"-bucket=archive-mlab-test", "-dry_run",
		"-month=2017/10"})
	defer ResetFlags()

	fake, tp := newCountingClient()
	q, err := batch.NewQueuer(fake, Options(), *fQueue, *fNumQueues, *fProject, *fBucket, *fDryRun)
	if err != nil {
		t.Fatal(err)
	}
	q.PostMonth(*fExper + "/" + *fMonth + "/")
	if tp.count != 100 {
		t.Errorf("Should have made 100 http requests: %d\n", tp.count)
	}
}

// NOTE: This test depends on the contents of the test-archive-mlab-sandbox
// bucket.  If it fails, check whether that bucket has been modified.
func ExampleMonth() {
	log.SetFlags(0)
	flag.CommandLine.Parse([]string{
		"-project", "fake-project", "-queue", "base-",
		"-bucket=archive-mlab-test", "-dry_run",
		"-month=2017/10"})
	defer ResetFlags()

	log.SetOutput(os.Stdout) // Redirect to stdout so test framework sees it.
	fake, _ := newCountingClient()
	q, _ := batch.NewQueuer(fake, Options(), *fQueue, *fNumQueues, *fProject, *fBucket, *fDryRun)
	q.PostMonth(*fExper + "/" + *fMonth + "/")

	log.SetOutput(os.Stderr) // restore
	// Unordered output:
	// Adding  ndt/2017/10/28/  to  base-3
	// Adding  ndt/2017/10/29/  to  base-4
	// Added  10  tasks to  base-3
	// Added  90  tasks to  base-4
}

// NOTE: This test depends on the contents of the test-archive-mlab-sandbox
// bucket.  If it fails, check whether that bucket has been modified.
func ExampleDay() {
	log.SetFlags(0)
	flag.CommandLine.Parse([]string{
		"-project", "fake-project", "-queue", "base-",
		"-bucket=archive-mlab-test", "-dry_run",
		"-day=2017/09/24"})
	defer ResetFlags()

	log.SetOutput(os.Stdout) // Redirect to stdout so test framework sees it.
	fake, _ := newCountingClient()
	q, _ := batch.NewQueuer(fake, Options(), *fQueue, *fNumQueues, *fProject, *fBucket, *fDryRun)
	q.PostDay(nil, *fExper+"/"+*fDay+"/")
	log.SetOutput(os.Stderr) // restore
	// Output:
	// Adding  ndt/2017/09/24/  to  base-1
	// Added  76  tasks to  base-1
}
