// The active package provides code for managing processing of an entire
// directory of task files.
package active_test

import (
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-lab/go/cloudtest"

	"github.com/m-lab/etl/active"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

type counter struct {
	t       *testing.T
	fail    int
	success int
}

func (c *counter) processTask(tf *active.TaskFile) error {
	time.Sleep(10 * time.Millisecond)
	if !strings.HasPrefix(tf.Path(), "gs://foobar/") {
		c.t.Error("Invalid path:", tf.Path())
	}
	if c.fail > 0 {
		log.Println("Intentional temporary failure:", tf)
		c.fail--
		return errors.New("intentional test failure")
	}
	log.Println(tf)
	c.success++
	return nil
}

func TestProcessAll(t *testing.T) {
	fc := cloudtest.GCSClient{}
	fc.AddTestBucket("foobar",
		cloudtest.BucketHandle{
			ObjAttrs: []*storage.ObjectAttrs{
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/2019/01/01/obj1", Updated: time.Now()},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/2019/01/01/obj2", Updated: time.Now()},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/2019/01/01/obj3"},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/2019/01/01/subdir/obj4", Updated: time.Now()},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "ndt/2019/01/01/subdir/obj5", Updated: time.Now()},
				&storage.ObjectAttrs{Bucket: "foobar", Name: "obj6", Updated: time.Now()},
			}})

	// First four attempts will fail.  This means that one of the 3 tasks will have two failures.
	p := counter{t: t, fail: 4}
	// Retry once per file.  This means one of the 3 tasks will never succeed.
	fs, err := active.NewFileSource(fc, "fake", "gs://foobar/ndt/2019/01/01/", 1, p.processTask)
	if err != nil {
		t.Fatal(err)
	}
	tokens := make(chan struct{}, 2)
	wg, err := fs.ProcessAll(context.Background(), tokens)
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	// At this point, we may be still draining the last tasks.
	for len(tokens) > 0 {
		time.Sleep(10 * time.Millisecond)
	}

	// One file should have failed twice.  Others should have failed once, then succeeded.
	if len(fs.Errors()) != 1 {
		t.Errorf("ProcessAll() had %d errors %v, %v", len(fs.Errors()), fs.Errors()[0], fs.Errors())
	}

	if p.success != 2 {
		t.Error("Expected 3 successes, got", p.success)
	}
}

func TestNoFiles(t *testing.T) {
	fc := cloudtest.GCSClient{}
	fc.AddTestBucket("foobar",
		cloudtest.BucketHandle{
			ObjAttrs: []*storage.ObjectAttrs{}})

	p := counter{t: t} // All processing attempts will succeed.
	fs, err := active.NewFileSource(fc, "fake", "gs://foobar/ndt/2019/01/01/", 1, p.processTask)
	if err != nil {
		t.Fatal(err)
	}
	tokens := make(chan struct{}, 2)
	wg, err := fs.ProcessAll(context.Background(), tokens)
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if len(fs.Errors()) > 0 {
		t.Error("ProcessAll() had errors", fs.Errors())
	}

	// At this point, we may be still draining the last tasks.
	for len(tokens) > 0 {
		time.Sleep(10 * time.Millisecond)
	}

	// processTask should never be called, because there are no files.
	if p.success+p.fail != 0 {
		t.Error("Expected 0 successes, got", p.success)
	}
}
