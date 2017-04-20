package bq

import (
	"fmt"
	"os"
	"testing"
	"time"
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
	items := [...]Item{
		// Each item implements the ValueSaver interface.
		{Name: tag + "_x0", Count: 17, Foobar: 44},
		{Name: tag + "_x1", Count: 12, Foobar: 44},
	}

	in, err := NewInserter(
		InserterParams{"mlab-sandbox", "mlab_sandbox", "test2", 10 * time.Second, 100})
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return
	}

	if err = in.InsertRows(items[:]); err != nil {
		fmt.Fprintf(os.Stderr, "failed to insert rows: %v\n", err)
	}
	// TODO - uncomment when this bug is resolved.
	//if in.InsertCount() != 2 {
	//	t.Error("Should have inserted two rows")
	//}
	in.Flush()
}
