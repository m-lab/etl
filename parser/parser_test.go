// TODO(soon) Implement good tests for the existing parsers.
//
package parser_test

import (
	"fmt"
	"testing"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/parser"
)

// countingInserter counts the calls to InsertRows and Flush.
// Inject into Parser for testing.
type countingInserter struct {
	etl.Inserter
	CallCount  int
	RowCount   int
	FlushCount int
}

func (ti *countingInserter) InsertRow(data interface{}) error {
	ti.CallCount++
	ti.RowCount++
	return nil
}
func (ti *countingInserter) InsertRows(data []interface{}) error {
	ti.CallCount++
	ti.RowCount += len(data)
	return nil
}
func (ti *countingInserter) Flush() error {
	ti.FlushCount++
	return nil
}

func TestPlumbing(t *testing.T) {
	foo := [10]byte{1, 2, 3, 4, 5, 1, 2, 3, 4, 5}
	tci := countingInserter{}
	var ti etl.Inserter = &tci
	var p etl.Parser = parser.NewTestParser(ti)
	err := p.ParseAndInsert(nil, "foo", foo[:])
	if err != nil {
		fmt.Println(err)
	}
	if tci.CallCount != 1 {
		t.Error("Should have called the inserter")
	}
}
