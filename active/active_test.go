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

	"cloud.google.com/go/storage"
	"github.com/m-lab/etl/active"
	"github.com/m-lab/go/cloudtest"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
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

func (c *counter) runFunc(tf *active.Task) active.Runnable {
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

func TestFileSourceBasic(t *testing.T) {
	client := cloudtest.GCSClient{}
	client.AddTestBucket("foobar",
		cloudtest.BucketHandle{
			ObjAttrs: []*storage.ObjectAttrs{
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/2019/01/01/obj1", Updated: time.Now()},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/2019/01/01/obj2", Updated: time.Now()},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/2019/01/01/obj3", Updated: time.Date(2000, 01, 01, 02, 03, 04, 0, time.UTC)},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/2019/01/01/subdir/obj4", Updated: time.Now()},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/2019/01/01/subdir/obj5", Updated: time.Now()},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "obj6", Updated: time.Now()},
			}})

	p := NewCounter(t)
	ctx := context.Background()
	lister := active.FileListerFunc(client, "fake", "gs://foobar/ndt/2019/01/01/")
	fs, err := active.NewFileSource(ctx, lister, 5, p.runFunc)
	if err != nil {
		t.Fatal(err)
	}

	fs.RunAll(ctx)

	if p.success != 3 {
		t.Error("All 3 tests should have succeeded.", p)
	}
}

func TestFileSourceExpiredContext(t *testing.T) {
	client := cloudtest.GCSClient{}
	client.AddTestBucket("foobar",
		cloudtest.BucketHandle{
			ObjAttrs: []*storage.ObjectAttrs{
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/2019/01/01/obj1", Updated: time.Now()},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/2019/01/01/obj2", Updated: time.Now()},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/2019/01/01/obj3", Updated: time.Now()},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/2019/01/01/obj4", Updated: time.Now()},
			}})

	p := NewCounter(t)
	ctx := context.Background()
	lister := active.FileListerFunc(client, "fake", "gs://foobar/ndt/2019/01/01/")
	fs, err := active.NewFileSource(ctx, lister, 0, p.runFunc)
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

	fs.CancelStreaming()
	// One item may arrive before cancel is detected.
	_, err = fs.Next(ctx)
	if err != context.Canceled {
		// This should return error.
		_, err = fs.Next(ctx)
		if err != context.Canceled {
			t.Fatal(err)
		}
	}
}

func TestFileSourceWithFailures(t *testing.T) {
	client := cloudtest.GCSClient{}
	client.AddTestBucket("foobar",
		cloudtest.BucketHandle{
			ObjAttrs: []*storage.ObjectAttrs{
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/2019/01/01/obj1", Updated: time.Now()},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/2019/01/01/obj2", Updated: time.Now()},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/2019/01/01/obj3", Updated: time.Date(2000, 01, 01, 02, 03, 04, 0, time.UTC)},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/2019/01/01/subdir/obj4", Updated: time.Now()},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/2019/01/01/subdir/obj5", Updated: time.Now()},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "obj6", Updated: time.Now()},
			}})

	// First two will fail.
	p := NewCounter(t)
	p.AddOutcome(os.ErrInvalid)
	p.AddOutcome(os.ErrInvalid)

	ctx := context.Background()
	lister := active.FileListerFunc(client, "fake", "gs://foobar/ndt/2019/01/01/")
	fs, err := active.NewFileSource(ctx, lister, 5, p.runFunc)
	if err != nil {
		t.Fatal(err)
	}

	fs.RunAll(ctx)

	if p.success != 1 {
		t.Error("1 test should have succeeded.", p.success)
	}
	if p.fail != 2 {
		t.Error("Fail", p.fail)
	}
}
