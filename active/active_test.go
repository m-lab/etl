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
	"google.golang.org/api/iterator"

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

type runnable struct {
	c   *counter
	obj *storage.ObjectAttrs
}

func (r *runnable) Run() error {
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
	p := newCounter(t)
	ctx := context.Background()
	fs, err := active.NewGCSSource(ctx, "test", standardLister(), p.toRunnable)
	if err != nil {
		t.Fatal(err)
	}

	eg, err := active.RunAll(ctx, fs)
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
	fs, err := active.NewGCSSource(ctx, "test", standardLister(), p.toRunnable)
	if err != nil {
		t.Fatal(err)
	}

	eg, err := active.RunAll(ctx, fs)
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
	fs, err := active.NewGCSSource(ctx, "test", standardLister(), p.toRunnable)
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
	fs, err := active.NewGCSSource(ctx, "test", ErroringLister, p.toRunnable)
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
	fs, err := active.NewGCSSource(ctx, "test", standardLister(), p.toRunnable)
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
