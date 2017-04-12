package bq

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// Bad - has side effects.
func TestInsert(t *testing.T) {
	tag := "new"
	items := []*Item{
		// Each item implements the ValueSaver interface.
		{Name: tag + "_x0", Count: 17, Foobar: 44},
		{Name: tag + "_x1", Count: 12, Foobar: 44},
	}

	in, err := NewInserter("mlab-sandbox", "mlab_sandbox", "test2")
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return
	}

	if err = in.InsertRows(items, 10*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "failed to insert rows: %v\n", err)
	}
}
