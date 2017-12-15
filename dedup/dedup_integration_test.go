// +build integration

package dedup_test

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/m-lab/etl/dedup"
	"google.golang.org/api/option"
	"gopkg.in/m-lab/go.v1/bqext"
)

func clientOpts() []option.ClientOption {
	opts := []option.ClientOption{}
	if os.Getenv("TRAVIS") != "" {
		authOpt := option.WithCredentialsFile("../travis-testing.key")
		opts = append(opts, authOpt)
	}
	return opts
}

func newTestingDataset(project, dataset string) (bqext.Dataset, error) {
	return bqext.NewDataset(project, dataset, clientOpts()...)
}

func TestGetNDTTableDetail(t *testing.T) {
	dsExt, err := newTestingDataset("mlab-testing", "etl")
	if err != nil {
		t.Fatal(err)
	}

	// Check that it handles empty partitions
	detail, err := dedup.GetNDTTableDetail(&dsExt, "TestDedupDest", "2000-12-29")
	if err != nil {
		t.Fatal(err)
	}
	if detail.TaskFileCount > 0 || detail.TestCount > 0 {
		t.Error("Should have zero counts")
	}

	// Check that it handles single partitions.
	// TODO - update to create its own test table.
	detail, err = dedup.GetNDTTableDetail(&dsExt, "TestDedupDest", "1999-01-01")
	if err != nil {
		t.Fatal(err)
	}
	if detail.TaskFileCount != 2 || detail.TestCount != 4 {
		t.Error("Wrong number of tasks or tests")
	}

	// Check that it handles full table.
	// TODO - update to create its own test table.
	detail, err = dedup.GetNDTTableDetail(&dsExt, "TestDedupSrc_19990101", "")
	if err != nil {
		t.Fatal(err)
	}
	if detail.TaskFileCount != 2 || detail.TestCount != 6 {
		t.Error("Wrong number of tasks or tests")
	}
}

func TestCheckAndDedup(t *testing.T) {
	dsExt, err := newTestingDataset("mlab-testing", "etl")
	if err != nil {
		t.Fatal(err)
	}

	info, err := dedup.GetInfoMatching(&dsExt, "TestDedupSrc_19990101")
	if err != nil {
		t.Fatal(err)
	}
	if len(info) != 1 {
		t.Fatal("No info for pattern.")
	}

	_, err = dedup.CheckAndDedup(&dsExt, info[0], "etl", "TestDedupDest", time.Hour, false)
	if err != nil {
		log.Println(err)
	}
	_, err = dedup.CheckAndDedup(&dsExt, info[0], "etl", "TestDedupDest", time.Hour, true)
	if err != nil {
		t.Error(err)
	}
}

func TestProcess(t *testing.T) {
	dsExt, err := newTestingDataset("mlab-testing", "etl")
	if err != nil {
		t.Fatal(err)
	}

	err = dedup.ProcessTablesMatching(&dsExt, "TestDedupSrc_", "etl", "TestDedupDest", 1*time.Minute)
	if err != nil {
		t.Error(err)
	}
	// TODO - actually check something interesting.
}
