// Package parser defines the Parser interface and implementations for the different
// test types, NDT, Paris Traceroute, and SideStream.
package parser

import (
	"cloud.google.com/go/bigquery"

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
	case etl.NEUBOT:
		return NewNeubotParser(ins)
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
func (np *NullParser) TaskError() error {
	return nil
}
