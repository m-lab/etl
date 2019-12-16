// The active package provides code for managing processing of an entire
// directory of task files.
package active_test

import (
	"context"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/m-lab/go/logx"

	"cloud.google.com/go/storage"
	"github.com/m-lab/etl/active"
	"github.com/m-lab/go/cloudtest"
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

func (c *counter) runFunc(tf *storage.ObjectAttrs) active.Runnable {
	log.Println("Creating runnable for", tf)
	return func() error {
		log.Println(tf)
		time.Sleep(10 * time.Millisecond)
		return c.err()
	}
}

func (c *counter) AddOutcome(err error) {
	c.outcome <- err
}

func NewCounter(t *testing.T) *counter {
	return &counter{t: t, outcome: make(chan error, 100)}
}

func standardLister() active.FileLister {
	client := cloudtest.GCSClient{}
	client.AddTestBucket("foobar",
		cloudtest.BucketHandle{
			ObjAttrs: []*storage.ObjectAttrs{
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/ndt5/2019/01/01/obj1", Updated: time.Now()},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/ndt5/2019/01/01/obj2", Updated: time.Now()},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/ndt5/2019/01/01/obj3", Updated: time.Date(2000, 01, 01, 02, 03, 04, 0, time.UTC)},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/ndt5/2019/01/01/subdir/obj4", Updated: time.Now()},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/ndt5/2019/01/01/subdir/obj5", Updated: time.Now()},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/tcpinfo/2019/01/01/obj3", Updated: time.Date(2000, 01, 01, 02, 03, 04, 0, time.UTC)},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "obj6", Updated: time.Now()},
			}})
	return active.FileListerFunc(client, "gs://foobar/ndt/ndt5/2019/01/01/")
}

func TestGCSSourceBasic(t *testing.T) {
	p := NewCounter(t)
	ctx := context.Background()
	fs, err := active.NewGCSSource(ctx, standardLister(), p.runFunc)
	if err != nil {
		t.Fatal(err)
	}

	active.RunAll(ctx, fs)

	if p.success != 3 {
		t.Error("All 3 tests should have succeeded.", p)
	}
}

func TestWithRunFailures(t *testing.T) {
	// First two will fail.
	p := NewCounter(t)
	p.AddOutcome(os.ErrInvalid)
	p.AddOutcome(os.ErrInvalid)

	ctx := context.Background()
	fs, err := active.NewGCSSource(ctx, standardLister(), p.runFunc)
	if err != nil {
		t.Fatal(err)
	}

	active.RunAll(ctx, fs)

	if p.success != 1 {
		t.Error("1 test should have succeeded.", p.success)
	}
	if p.fail != 2 {
		t.Error("Fail", p.fail)
	}
}

func TestExpiredContext(t *testing.T) {
	p := NewCounter(t)
	ctx := context.Background()
	fs, err := active.NewGCSSource(ctx, standardLister(), p.runFunc)
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
	p := NewCounter(t)

	ctx := context.Background()
	fs, err := active.NewGCSSource(ctx, ErroringLister, p.runFunc)
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.Next(ctx)
	if err != os.ErrInvalid {
		t.Fatal("Should return os.ErrInvalid")
	}
}

func TestExpiredFileListerContext(t *testing.T) {
	p := NewCounter(t)

	ctx := context.Background()
	fs, err := active.NewGCSSource(ctx, standardLister(), p.runFunc)
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
