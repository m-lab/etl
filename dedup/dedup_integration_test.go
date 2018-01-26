// +build integration

package dedup_test

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/m-lab/etl/dedup"
	"github.com/m-lab/go/bqext"
	"google.golang.org/api/option"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func clientOpts() []option.ClientOption {
	opts := []option.ClientOption{}
	if os.Getenv("TRAVIS") != "" {
		// See m-lab/go#11
		authOpt := option.WithCredentialsFile("../travis-testing.key")
		opts = append(opts, authOpt)
	}
	return opts
}

func newTestingDataset(project, dataset string) (bqext.Dataset, error) {
	return bqext.NewDataset(project, dataset, clientOpts()...)
}

func TestGetTableDetail(t *testing.T) {
	dsExt, err := newTestingDataset("mlab-testing", "etl")
	if err != nil {
		t.Fatal(err)
	}

	// Check that it handles empty partitions
	detail, err := dedup.GetTableDetail(&dsExt, dsExt.Table("TestDedupDest$20001229"))
	if err != nil {
		t.Fatal(err)
	}
	if detail.TaskFileCount > 0 || detail.TestCount > 0 {
		t.Error("Should have zero counts")
	}

	// Check that it handles single partitions.
	// TODO - update to create its own test table.
	detail, err = dedup.GetTableDetail(&dsExt, dsExt.Table("TestDedupDest$19990101"))
	if err != nil {
		t.Fatal(err)
	}
	if detail.TaskFileCount != 2 || detail.TestCount != 4 {
		t.Error("Wrong number of tasks or tests")
	}

	// Check that it handles full table.
	// TODO - update to create its own test table.
	detail, err = dedup.GetTableDetail(&dsExt, dsExt.Table("TestDedupSrc_19990101"))
	if err != nil {
		t.Fatal(err)
	}
	if detail.TaskFileCount != 2 || detail.TestCount != 6 {
		t.Error("Wrong number of tasks or tests")
	}
}

func TestGetTableInfo(t *testing.T) {
	dsExt, err := newTestingDataset("mlab-testing", "src")
	if err != nil {
		t.Fatal(err)
	}

	info, err := dedup.GetTableInfo(context.Background(), dsExt.Table("TestDedupSrc"))
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsPartitioned {
		t.Error("Should be partitioned")
	}
	if info.NumRows != 8 {
		t.Errorf("Wrong number of rows: %d", info.NumRows)
	}
}

func TestGetTableInfoMatching(t *testing.T) {
	dsExt, err := newTestingDataset("mlab-testing", "src")
	if err != nil {
		t.Fatal(err)
	}

	info, err := dedup.GetTableInfoMatching(context.Background(), &dsExt, "Test")
	if err != nil {
		t.Fatal(err)
	}
	if len(info) != 3 {
		t.Errorf("Wrong length: %d", len(info))
	}
}

func TestGetPartitionInfo(t *testing.T) {
	dsExt, err := newTestingDataset("mlab-testing", "src")
	if err != nil {
		t.Fatal(err)
	}

	info, err := dedup.GetPartitionInfo(context.Background(), &dsExt, dsExt.Table("TestDedupSrc$19990101"))
	if err != nil {
		t.Fatal(err)
	}
	if info.PartitionID != "19990101" {
		t.Error("wrong partitionID: " + info.PartitionID)
	}
}
