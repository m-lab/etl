package bq_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/m-lab/etl/fake"
	"github.com/m-lab/etl/intf"
)

// Item represents a row item.
type Item struct {
	Name   string
	Count  int
	Foobar int `json:"foobar"`
}

// NB: This test has side effects and depends on BigQuery service and
// test table.
// Do not run this test from travis.
// TODO - use emulator when available.
func TestInsert(t *testing.T) {
	tag := "new"
	var items []interface{}
	items = append(items, &Item{Name: tag + "_x0", Count: 17, Foobar: 44})
	items = append(items, &Item{Name: tag + "_x1", Count: 12, Foobar: 44})

	in, err := fake.NewFakeInserter(
		intf.InserterParams{"mlab-sandbox", "mlab_sandbox", "test2", 10 * time.Second, 100})
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return
	}

	if err = in.InsertRow(Item{Name: tag + "_x0", Count: 17, Foobar: 44}); err != nil {
		fmt.Fprintf(os.Stderr, "failed to insert rows: %v\n", err)
	}
	if err = in.InsertRows(items); err != nil {
		fmt.Fprintf(os.Stderr, "failed to insert rows: %v\n", err)
	}
	// TODO - uncomment when this bug is resolved.
	//if in.Count() != 2 {
	//	t.Error("Should have inserted two rows")
	//}
	in.Flush()
}
