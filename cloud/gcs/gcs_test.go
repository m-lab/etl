package gcs_test

import (
	"log"
	"testing"

	"github.com/m-lab/etl/cloud/gcs"
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
