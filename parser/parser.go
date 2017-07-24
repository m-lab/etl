// Package parser defines the Parser interface and implementations for the different
// test types, NDT, Paris Traceroute, and SideStream.
package parser

import (
	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
)

func NewParser(dt etl.DataType, ins etl.Inserter) etl.Parser {
	switch dt {
	case etl.NDT:
		return NewNDTParser(ins)
	case etl.SS:
		return NewSSParser(ins)
	case etl.PT:
		return NewPTParser(ins)
	case etl.SW:
		return NewDiscoParser(ins)
	default:
		return nil
	}
}

//=====================================================================================
//                       Parser implementations
//=====================================================================================

// FakeRowStats provides trivial implementation of RowStats interface.
type FakeRowStats struct {
}

func (s *FakeRowStats) RowsInBuffer() int {
	return 0
}
func (s *FakeRowStats) Accepted() int {
	return 0
}
func (s *FakeRowStats) Committed() int {
	return 0
}
func (s *FakeRowStats) Failed() int {
	return 0
}

type NullParser struct {
	FakeRowStats
}

func (np *NullParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error {
	metrics.TestCount.WithLabelValues("table", "null", "ok").Inc()
	return nil
}

func (np *NullParser) TableName() string {
	return "null-table"
}

//------------------------------------------------------------------------------------
// TestParser ignores the content, returns a MapSaver containing meta data and
// "testname":"..."
// TODO add tests
type TestParser struct {
	inserter     etl.Inserter
	etl.RowStats // Allows RowStats to be implemented through an embedded struct.
}

func NewTestParser(ins etl.Inserter) etl.Parser {
	return &TestParser{
		ins,
		&FakeRowStats{}} // Use a FakeRowStats to provide the RowStats functions.
}

func (tp *TestParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error {
	metrics.TestCount.WithLabelValues("table", "test", "ok").Inc()
	values := make(map[string]bigquery.Value, len(meta)+1)
	// TODO is there a better way to do this?
	for k, v := range meta {
		values[k] = v
	}
	values["testname"] = testName
	return tp.inserter.InsertRow(bq.MapSaver{Values: values})
}

// These functions are also required to complete the etl.Parser interface.
func (tp *TestParser) Flush() error {
	return nil
}
func (tp *TestParser) TableName() string {
	return "test-table"
}
func (tp *TestParser) FullTableName() string {
	return "test-table"
}
