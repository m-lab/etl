package gcs_test

import (
	"context"
	"log"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-lab/etl/cloud/gcs"
	"github.com/m-lab/go/cloudtest"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestNewPlatformPrefix(t *testing.T) {
	pp, err := gcs.ParsePrefix("gs://pusher-mlab-sandbox/ndt/tcpinfo/2019/04/01/")
	if err != nil {
		t.Fatal(err)
	}

	if pp.DataType != "tcpinfo" {
		t.Error(pp)
	}
}

func TestLegacyPrefix(t *testing.T) {
	pp, err := gcs.ParsePrefix("gs://archive-mlab-sandbox/ndt/2019/04/01/")
	if err != nil {
		t.Fatal(err)
	}

	if pp.DataType != "ndt" {
		t.Error(pp)
	}

}

func TestGetFilesSince(t *testing.T) {
	fc := cloudtest.GCSClient{}
	fc.AddTestBucket("foobar",
		cloudtest.BucketHandle{
			ObjAttrs: []*storage.ObjectAttrs{
				&storage.ObjectAttrs{Name: "ndt/2019/01/01/obj1", Size: 101, Updated: time.Now()},
				&storage.ObjectAttrs{Name: "ndt/2019/01/01/obj2", Size: 2020, Updated: time.Now()},
				&storage.ObjectAttrs{Name: "ndt/2019/01/01/obj3"},
				&storage.ObjectAttrs{Name: "ndt/2019/01/01/subdir/obj4", Updated: time.Now()},
				&storage.ObjectAttrs{Name: "ndt/2019/01/01/subdir/obj5", Updated: time.Now()},
				&storage.ObjectAttrs{Name: "obj6", Updated: time.Now()},
			}})

	files, bytes, err := gcs.GetFilesSince(context.Background(), fc, "project", "gs://foobar/ndt/2019/01/01/", time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Error("Expected 2 files, got", len(files))
	}
	if bytes != 2121 {
		t.Error("Expected total 2121 bytes, got", bytes)
	}
}
