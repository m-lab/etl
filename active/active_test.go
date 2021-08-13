// The active package provides code for managing processing of an entire
// directory of task files.
package active_test

import (
	"context"
	"log"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"

	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/etl/active"
	"github.com/m-lab/go/cloud/gcs"
	"github.com/m-lab/go/logx"
	"github.com/m-lab/go/rtx"

	"github.com/m-lab/go/cloudtest/gcsfake"
)

var (
	job = tracker.Job{}
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	logx.LogxDebug.Set("true")
}

type counter struct {
	lock    sync.Mutex
	t       *testing.T
	outcome chan error
	fail    int
	success int
}

func (c *counter) err() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	select {
	case err := <-c.outcome:
		log.Println(err)
		c.fail++
		return err
	default:
		log.Println("ok")
		c.success++
		return nil
	}
}

type runnable struct {
	c   *counter
	obj *storage.ObjectAttrs
}

func (r *runnable) Run(ctx context.Context) error {
	log.Println(r.obj.Name)
	time.Sleep(10 * time.Millisecond)
	return r.c.err()
}
func (r *runnable) Info() string {
	return "test"
}

func (c *counter) toRunnable(obj *storage.ObjectAttrs) active.Runnable {
	log.Println("Creating runnable for", obj.Name)
	return &runnable{c, obj}
}

func (c *counter) addOutcome(err error) {
	c.outcome <- err
}

func newCounter(t *testing.T) *counter {
	return &counter{t: t, outcome: make(chan error, 100)}
}

func testClient() stiface.Client {
	client := gcsfake.GCSClient{}
	client.AddTestBucket("foobar",
		&gcsfake.BucketHandle{
			ObjAttrs: []*storage.ObjectAttrs{
				{Bucket: "foobar", Name: "ndt/ndt5/2019/01/01/obj1", Updated: time.Now()},
				{Bucket: "foobar", Name: "ndt/ndt5/2019/01/01/obj2", Updated: time.Now()},
				{Bucket: "foobar", Name: "ndt/ndt5/2019/01/01/obj3", Updated: time.Date(2000, 01, 01, 02, 03, 04, 0, time.UTC)},
				{Bucket: "foobar", Name: "ndt/ndt5/2019/01/01/subdir/obj4", Updated: time.Now()},
				{Bucket: "foobar", Name: "ndt/ndt5/2019/01/01/subdir/obj5", Updated: time.Now()},
				{Bucket: "foobar", Name: "ndt/tcpinfo/2019/01/01/obj3", Updated: time.Date(2000, 01, 01, 02, 03, 04, 0, time.UTC)},
				{Bucket: "foobar", Name: "obj6", Updated: time.Now()},
			}})
	return &client
}

func standardLister() active.FileLister {
	bh, err := gcs.GetBucket(context.Background(), testClient(), "foobar")
	rtx.Must(err, "GetBucket failed")
	return active.FileListerFunc(bh, "ndt/ndt5/2019/01/01/", nil)
}

func skipFilesListener(dataType string) active.FileLister {
	client := gcsfake.GCSClient{}
	prefix := path.Join("ndt/", dataType, "/2019/01/01/")
	client.AddTestBucket("foobar",
		&gcsfake.BucketHandle{
			ObjAttrs: []*storage.ObjectAttrs{
				{Bucket: "foobar", Name: path.Join(prefix, "obj1"), Updated: time.Now()},
				{Bucket: "foobar", Name: path.Join(prefix, "obj2"), Updated: time.Now()},
				{Bucket: "foobar", Name: path.Join(prefix, "obj3"), Updated: time.Now()},
				{Bucket: "foobar", Name: path.Join(prefix, "obj4"), Updated: time.Now()},
				{Bucket: "foobar", Name: path.Join(prefix, "obj5"), Updated: time.Now()},
				{Bucket: "foobar", Name: path.Join(prefix, "obj6"), Updated: time.Now()},
				{Bucket: "foobar", Name: path.Join(prefix, "obj7"), Updated: time.Now()},
				{Bucket: "foobar", Name: path.Join(prefix, "obj8"), Updated: time.Now()},
				{Bucket: "foobar", Name: path.Join(prefix, "obj9"), Updated: time.Now()},
				{Bucket: "foobar", Name: path.Join(prefix, "obj10"), Updated: time.Now()},
				{Bucket: "foobar", Name: path.Join(prefix, "obj11"), Updated: time.Now()},
			}})

	bh, err := gcs.GetBucket(context.Background(), &client, "foobar")
	rtx.Must(err, "GetBucket failed")
	return active.FileListerFunc(bh, prefix, nil)

}

func runAll(ctx context.Context, rSrc active.RunnableSource) (*errgroup.Group, error) {
	eg := &errgroup.Group{}
	for {
		run, err := rSrc.Next(ctx)
		if err != nil {
			log.Println(err)
			return eg, err
		}
		log.Println("Starting func")

		f := func() error {
			err := run.Run(ctx)
			return err
		}

		eg.Go(f)
	}
}

func TestGCSSourceBasic(t *testing.T) {
	p := newCounter(t)
	ctx := context.Background()
	fs, err := active.NewGCSSource(ctx, job, standardLister(), p.toRunnable)
	if err != nil {
		t.Fatal(err)
	}

	eg, err := runAll(ctx, fs)
	if err != iterator.Done {
		t.Fatal(err)
	}
	err = eg.Wait()
	if err != nil {
		t.Error(err)
	}

	if p.success != 3 {
		t.Error("All 3 tests should have succeeded.", p)
	}
}

func TestWithRunFailures(t *testing.T) {
	// First two will fail.
	p := newCounter(t)
	p.addOutcome(os.ErrInvalid)
	p.addOutcome(os.ErrInvalid)

	ctx := context.Background()
	fs, err := active.NewGCSSource(ctx, job, standardLister(), p.toRunnable)
	if err != nil {
		t.Fatal(err)
	}

	eg, err := runAll(ctx, fs)
	if err != iterator.Done {
		t.Fatal(err)
	}
	err = eg.Wait()
	if err != os.ErrInvalid {
		t.Error(err, "should be invalid argument")
	}

	if p.success != 1 {
		t.Error("1 test should have succeeded.", p.success)
	}
	if p.fail != 2 {
		t.Error("Fail", p.fail)
	}
}

func TestExpiredContext(t *testing.T) {
	p := newCounter(t)
	ctx := context.Background()
	fs, err := active.NewGCSSource(ctx, job, standardLister(), p.toRunnable)
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = fs.Next(canceled)
	if err != context.Canceled {
		t.Fatal("Expected context canceled:", err)
	}
}

func ErroringLister(ctx context.Context) ([]*storage.ObjectAttrs, int64, error) {
	return nil, 0, os.ErrInvalid
}

func TestWithStorageError(t *testing.T) {
	p := newCounter(t)

	ctx := context.Background()
	fs, err := active.NewGCSSource(ctx, job, ErroringLister, p.toRunnable)
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.Next(ctx)
	if err != os.ErrInvalid {
		t.Fatal("Should return os.ErrInvalid")
	}
}

func TestExpiredFileListerContext(t *testing.T) {
	p := newCounter(t)

	ctx := context.Background()
	fs, err := active.NewGCSSource(ctx, job, standardLister(), p.toRunnable)
	if err != nil {
		t.Fatal(err)
	}

	// This just ensures that the streaming goroutine has started.
	_, err = fs.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Race here!!
	fs.CancelStreaming()

	// This may succeed, or may error, depending on when the streaming goroutine
	// is scheduled before or after the CancelStreaming.  So just keep retrying
	// until it errors.  Should be only once or twice.
	for ; err == nil; _, err = fs.Next(ctx) {
	}
	if err != context.Canceled {
		t.Error("Should return os.ErrInvalid", err)
	}
}

func TestSkipFiles(t *testing.T) {
	tests := []struct {
		name         string
		successCount int
		failureCount int
	}{
		{
			name:         "pcap",
			successCount: 2,
			failureCount: 0,
		},
		{
			name:         "ndt7",
			successCount: 11,
			failureCount: 0,
		},
		{
			name:         "foo",
			successCount: 11,
			failureCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newCounter(t)
			ctx := context.Background()
			fs, err := active.NewGCSSource(ctx, tracker.Job{Datatype: tt.name}, skipFilesListener(tt.name), p.toRunnable)
			if err != nil {
				t.Fatal(err)
			}

			eg, err := runAll(ctx, fs)
			if err != iterator.Done {
				t.Fatal(err)
			}
			err = eg.Wait()
			if err != nil {
				t.Error(err)
			}

			if p.success != tt.successCount {
				t.Errorf("for %s, %d should have succeeded, got %d", tt.name, tt.successCount, p.success)
			}

			if p.fail != tt.failureCount {
				t.Errorf("for %s, %d should have failed, got %d", tt.name, tt.failureCount, p.fail)
			}
		})
	}
}
