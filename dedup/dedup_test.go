package dedup

import (
	"log"
	"testing"

	"cloud.google.com/go/bigquery"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// getTableParts separates a table name into prefix/base, separator, and partition date.
func Test_getTableParts(t *testing.T) {
	parts, err := getTableParts("table$20160102")
	if err != nil {
		t.Fatal(err)
	}
	if !parts.isPartitioned {
		t.Error("Should be partitioned")
	}
	if parts.prefix != "table" {
		t.Error("incorrect prefix: " + parts.prefix)
	}
	if parts.yyyymmdd != "20160102" {
		t.Error("incorrect partition: " + parts.yyyymmdd)
	}
}

// getTable constructs a bigquery Table object from project/dataset/table/partition.
// The project/dataset/table/partition may or may not actually exist.
// This does NOT do any network operations.
func Test_getTable(t *testing.T) {
	//bqClient *bigquery.Client, project, dataset, table, partition string) (*bigquery.Table, error) {
	foo, err := getTable(&bigquery.Client{}, "project", "dataset", "table", "20160102")
	if err != nil {
		t.Fatal(err)
	}

	if foo.DatasetID != "dataset" {
		t.Error("Bad parsing")
	}
	// TODO check for invalid table base.
}
