package main_test

import (
	"log"
	"testing"
	"time"

	"github.com/m-lab/etl/bqext"
)

func TestDedup(t *testing.T) {
	start := time.Now() // Later, we will compare partition time to this.

	tExt, err := bqext.NewDataset("mlab-testing", "etl")
	if err != nil {
		log.Fatal(err)
	}

	// First check that source table has expected number of rows.
	// TestDedupSrc should have 6 rows, of which 4 should be unique.
	type QR struct {
		NumRows int64
	}
	result := QR{}
	err = tExt.QueryAndParse("select count(test_id) as NumRows from `TestDedupSrc_19990101`", &result)
	if result.NumRows != 6 {
		t.Fatal("Source table has wrong number rows: ", result.NumRows)
	}

	tExt.GetInfoMatching("etl", "TestDedupSrc_19990101")

	// TODO - should have suffix in destination??
	tExt.Dedup("TestDedupSrc_19990101", true, "mlab-testing", "etl", "TestDedupDest$19990101")

	pi, err := tExt.GetPartitionInfo("TestDedupDst", "19990101")
	if err != nil {
		t.Fatal(err)
	}
	if pi.CreationTime.Before(start) {
		t.Error("Partition not overwritten??? ", pi.CreationTime)
	}

	err = tExt.QueryAndParse("select count(test_id) as NumRows from `TestDedupDest` where _PARTITIONTIME = timestamp("+`"1999-01-01 00:00:00"`+")", &result)
	if err != nil {
		t.Fatal(err)
	}
	if result.NumRows != 4 {
		t.Error("Destination has wrong number of rows: ", result.NumRows)
	}
}
