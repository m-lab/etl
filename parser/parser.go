// Package parser defines the Parser interface and implementations for the different
// test types, NDT, Paris Traceroute, and SideStream.
package parser

import (
	"os"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
)

func init() {
	initParserVersion()
}

var gParserVersion string

// initParserVersion initializes the gParserVersion variable for use by all parsers.
func initParserVersion() {
	release, ok := os.LookupEnv("RELEASE_TAG")
	if ok && release != "empty_tag" {
		gParserVersion = "https://github.com/m-lab/etl/tree/" + release
	} else {
		hash := os.Getenv("COMMIT_HASH")
		if len(hash) >= 8 {
			gParserVersion = "https://github.com/m-lab/etl/tree/" + hash[0:8]
		} else {
			gParserVersion = "local development"
		}
	}
}

// Version returns the parser version used by parsers to annotate data rows.
func Version() string {
	return gParserVersion
}

// NewParser creates an appropriate parser for a given data type.
func NewParser(dt etl.DataType, ins etl.Inserter) etl.Parser {
	switch dt {
	case etl.NDT:
		return NewNDTParser(ins)
	case etl.SS:
		return NewDefaultSSParser(ins) // TODO fix this hack.
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
func (np *NullParser) TaskError() error {
	return nil
}
