// +build integration

package dedup_test

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/m-lab/etl/dedup"
	"github.com/m-lab/go/bqext"
	"google.golang.org/api/option"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func testingAuth() []option.ClientOption {
	opts := []option.ClientOption{}
	if os.Getenv("TRAVIS") != "" {
		// See m-lab/go#11
		authOpt := option.WithCredentialsFile("../travis-testing.key")
		opts = append(opts, authOpt)
	}
	return opts
}

func TestGetTableDetail(t *testing.T) {
	destDS, err := bqext.NewDataset("mlab-testing", "etl", testingAuth()...)
	if err != nil {
		t.Fatal(err)
	}

	// Check that it handles empty partitions
	detail, err := dedup.GetTableDetail(&destDS, destDS.Table("DedupTest$20001229"))
	if err != nil {
		t.Fatal(err)
	}
	if detail.TaskFileCount > 0 || detail.TestCount > 0 {
		t.Error("Should have zero counts")
	}

	// Check that it handles single partitions.
	// TODO - update to create its own test table.
	detail, err = dedup.GetTableDetail(&destDS, destDS.Table("DedupTest$19990101"))
	if err != nil {
		t.Fatal(err)
	}
	if detail.TaskFileCount != 2 || detail.TestCount != 4 {
		t.Error("Wrong number of tasks or tests")
	}

	srcDS, err := bqext.NewDataset("mlab-testing", "src", testingAuth()...)
	if err != nil {
		t.Fatal(err)
	}

	// Check that it handles full table.
	// TODO - update to create its own test table.
	detail, err = dedup.GetTableDetail(&srcDS, srcDS.Table("DedupTest_19990101"))
	if err != nil {
		t.Fatal(err)
	}
	if detail.TaskFileCount != 2 || detail.TestCount != 6 {
		t.Error("Wrong number of tasks or tests")
	}
}

func TestAnnotationTableMeta(t *testing.T) {
	// TODO - Make NewDataSet return a pointer, for consistency with bigquery.
	dsExt, err := bqext.NewDataset("mlab-testing", "src", testingAuth()...)
	if err != nil {
		t.Fatal(err)
	}

	tbl := dsExt.Table("DedupTest")
	at := dedup.NewAnnotatedTable(tbl, &dsExt)
	meta, err := at.CachedMeta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if meta.NumRows != 8 {
		t.Errorf("Wrong number of rows: %d", meta.NumRows)
	}
	if meta.TimePartitioning == nil {
		t.Error("Should be partitioned")
	}

	tbl = dsExt.Table("XYZ")
	at = dedup.NewAnnotatedTable(tbl, &dsExt)
	meta, err = at.CachedMeta(nil)
	if err != dedup.ErrNilContext {
		t.Fatal("Should be an error when no context provided")
	}
	meta, err = at.CachedMeta(context.Background())
	if err == nil {
		t.Fatal("Should be an error when fetching bad table meta")
	}
}

func TestAnnotationDetail(t *testing.T) {
	dsExt, err := bqext.NewDataset("mlab-testing", "src", testingAuth()...)
	if err != nil {
		t.Fatal(err)
	}

	tbl := dsExt.Table("DedupTest")
	at := dedup.NewAnnotatedTable(tbl, &dsExt)
	_, err = at.CachedDetail(context.Background())
	if err != nil {
		t.Fatal(err)
	}
}

func TestGetTablesMatching(t *testing.T) {
	dsExt, err := bqext.NewDataset("mlab-testing", "src", testingAuth()...)
	if err != nil {
		t.Fatal(err)
	}

	atList, err := dedup.GetTablesMatching(context.Background(), &dsExt, "Test")
	if err != nil {
		t.Fatal(err)
	}
	if len(atList) != 3 {
		t.Errorf("Wrong length: %d", len(atList))
	}
}

func TestAnnotatedTableGetPartitionInfo(t *testing.T) {
	dsExt, err := bqext.NewDataset("mlab-testing", "src", testingAuth()...)
	if err != nil {
		t.Fatal(err)
	}

	tbl := dsExt.Table("DedupTest$19990101")
	at := dedup.NewAnnotatedTable(tbl, &dsExt)
	info, err := at.GetPartitionInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.PartitionID != "19990101" {
		t.Error("wrong partitionID: " + info.PartitionID)
	}

	// Check behavior for missing partition
	tbl = dsExt.Table("DedupTest$17760101")
	at = dedup.NewAnnotatedTable(tbl, &dsExt)
	info, err = at.GetPartitionInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.PartitionID != "" {
		t.Error("Non-existent partition should return empty PartitionID")
	}
}

// TODO - should check some failure cases.
func TestCheckAndDedup(t *testing.T) {
	dsExt, err := bqext.NewDataset("mlab-testing", "src", testingAuth()...)
	if err != nil {
		t.Fatal(err)
	}

	atList, err := dedup.GetTablesMatching(context.Background(), &dsExt, "DedupTest_19990101")
	if err != nil {
		t.Fatal(err)
	}
	if len(atList) != 1 {
		t.Fatal("No info for pattern.")
	}

	destTable := dsExt.BqClient.DatasetInProject(dsExt.ProjectID, "etl").Table("DedupTest$19990101")
	// TODO - clean up pointer vs non-pointer args everywhere.
	job := dedup.NewJob(&dsExt, &atList[0], dedup.NewAnnotatedTable(destTable, &dsExt))
	err = job.CheckAndDedup(context.Background(), dedup.Options{time.Minute, false, false, false})
	if err != nil {
		log.Println(err)
	}
	err = job.CheckAndDedup(context.Background(), dedup.Options{time.Minute, true, false, false})
	if err != nil {
		t.Error(err)
	}
}

func TestProcess(t *testing.T) {
	dsExt, err := bqext.NewDataset("mlab-testing", "src", testingAuth()...)
	if err != nil {
		t.Fatal(err)
	}

	err = dedup.ProcessTablesMatching(&dsExt, "DedupTest_", "etl", dedup.Options{1 * time.Minute, false, false, false})
	if err != nil && err != dedup.ErrSrcOlderThanDest {
		t.Error(err)
	}
	// TODO - actually check something interesting.
}
