package dedup

import (
	"log"
	"testing"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// GetTableNameParts separates a table name into Prefix/base, separator, and partition date.
func Test_GetTableNameParts(t *testing.T) {
	parts, err := GetTableNameParts("table$20160102")
	if err != nil {
		t.Error(err)
	} else {
		if !parts.IsPartitioned {
			t.Error("Should be partitioned")
		}
		if parts.Prefix != "table" {
			t.Error("incorrect Prefix: " + parts.Prefix)
		}
		if parts.Yyyymmdd != "20160102" {
			t.Error("incorrect partition: " + parts.Yyyymmdd)
		}
	}

	parts, err = GetTableNameParts("table_20160102")
	if err != nil {
		t.Error(err)
	} else {
		if parts.IsPartitioned {
			t.Error("Should not be partitioned")
		}
	}
	parts, err = GetTableNameParts("table$2016010")
	if err == nil {
		t.Error("Should error when partition is incomplete")
	}
	parts, err = GetTableNameParts("table$201601022")
	if err == nil {
		t.Error("Should error when partition is too long")
	}
	parts, err = GetTableNameParts("table$20162102")
	if err == nil {
		t.Error("Should error when partition is invalid")
	}
}

// getTable constructs a bigquery Table object from project/dataset/table/partition.
// The project/dataset/table/partition may or may not actually exist.
// This does NOT do any network operations.
func Test_getTable(t *testing.T) {
	// We won't do anything with the tables, so we can use a nil client.
	table, err := getTable(nil, "project", "dataset", "table", "20160102")
	if err != nil {
		t.Error(err)
	} else {
		if table.DatasetID != "dataset" {
			t.Error("Bad parsing")
		}
	}

	table, err = getTable(nil, "project", "dataset", "table$124", "20160102")
	if err == nil {
		t.Error("Bad table name not detected")
	}

	table, err = getTable(nil, "project", "dataset", "table", "201601020")
	if err == nil {
		t.Error("Bad suffix not detected")
	}
}
