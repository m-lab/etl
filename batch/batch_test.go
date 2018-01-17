package batch_test

import (
	"bytes"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"testing"

	"github.com/m-lab/etl/batch"
)

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

func TestPostMonth(t *testing.T) {
	flag.CommandLine.Parse([]string{
		"-queue", "base-", "-dry_run", "-bucket=test-archive-mlab-sandbox",
		"-month=2017/10"})
	defer ResetFlags()

	fake := countingHTTP{}
	q, err := batch.NewQueuer(&fake, *fQueue, *fNumQueues, *fProject, *fBucket, *fDryRun)
	if err != nil {
		t.Fatal(err)
	}
	q.PostMonth(*fExper + "/" + *fMonth + "/")
	if fake.count != 100 {
		t.Errorf("Should have made 100 http requests: %d\n", fake.count)
	}
}

// NOTE: This test depends on the contents of the test-archive-mlab-sandbox
// bucket.  If it fails, check whether that bucket has been modified.
func ExampleMonth() {
	log.SetFlags(0)
	flag.CommandLine.Parse([]string{
		"-queue", "base-", "-dry_run", "-bucket=test-archive-mlab-sandbox",
		"-month=2017/10"})
	defer ResetFlags()

	log.SetOutput(os.Stdout) // Redirect to stdout so test framework sees it.
	q, _ := batch.NewQueuer(nil, *fQueue, *fNumQueues, *fProject, *fBucket, *fDryRun)
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
		"-queue", "base-", "-dry_run", "-bucket=test-archive-mlab-sandbox",
		"-day=2017/09/24"})
	defer ResetFlags()

	log.SetOutput(os.Stdout) // Redirect to stdout so test framework sees it.
	q, _ := batch.NewQueuer(nil, *fQueue, *fNumQueues, *fProject, *fBucket, *fDryRun)
	q.PostDay(nil, *fExper+"/"+*fDay+"/")
	log.SetOutput(os.Stderr) // restore
	// Output:
	// Adding  ndt/2017/09/24/  to  base-1
	// Added  76  tasks to  base-1
}
