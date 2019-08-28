// The active package provides code for managing processing of an entire
// directory of task files.
package active_test

import (
	"context"
	"errors"
	"log"
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
	fail  int
	total int
}

func (c *counter) processTask(tf *active.TaskFile) error {
	time.Sleep(10 * time.Millisecond)
	if c.fail > 0 {
		log.Println("Failing", tf)
		c.fail--
		return errors.New("intentional test failure")
	}
	log.Println(tf)
	c.total++
	return nil
}

func TestProcessAll(t *testing.T) {
	fc := cloudtest.GCSClient{}
	fc.AddTestBucket("foobar",
		cloudtest.BucketHandle{
			ObjAttrs: []*storage.ObjectAttrs{
				&storage.ObjectAttrs{Name: "ndt/2019/01/01/obj1", Updated: time.Now()},
				&storage.ObjectAttrs{Name: "ndt/2019/01/01/obj2", Updated: time.Now()},
				&storage.ObjectAttrs{Name: "ndt/2019/01/01/obj3"},
				&storage.ObjectAttrs{Name: "ndt/2019/01/01/subdir/obj4", Updated: time.Now()},
				&storage.ObjectAttrs{Name: "ndt/2019/01/01/subdir/obj5", Updated: time.Now()},
				&storage.ObjectAttrs{Name: "obj6", Updated: time.Now()},
			}})

	p := counter{fail: 2}
	fs, err := active.NewFileSource(fc, "fake", "gs://foobar/ndt/2019/01/01/", 1, p.processTask)
	if err != nil {
		t.Fatal(err)
	}
	tokens := make(chan struct{}, 2)
	err = fs.ProcessAll(context.Background(), tokens)
	if err.Error() != "intentional test failure" {
		t.Error("Should have seen intentional test failure:", err)
	}

	// At this point, we may be still draining the last tasks.
	for len(tokens) > 0 {
		time.Sleep(10 * time.Millisecond)
	}

	if p.total != 3 {
		t.Error(p.total, "!= 3")
	}
}

func TestNoFiles(t *testing.T) {
	fc := cloudtest.GCSClient{}
	fc.AddTestBucket("foobar",
		cloudtest.BucketHandle{
			ObjAttrs: []*storage.ObjectAttrs{}})

	p := counter{}
	fs, err := active.NewFileSource(fc, "fake", "gs://foobar/ndt/2019/01/01/", 1, p.processTask)
	if err != nil {
		t.Fatal(err)
	}
	tokens := make(chan struct{}, 2)
	err = fs.ProcessAll(context.Background(), tokens)
	if err != nil {
		t.Error("ProcessAll() error", err)
	}

	// At this point, we may be still draining the last tasks.
	for len(tokens) > 0 {
		time.Sleep(10 * time.Millisecond)
	}

	if p.total != 0 {
		t.Error(p.total, "!= 0")
	}
}
