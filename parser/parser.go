// The parser package defines the Parser interface and implementations for the different
// test types, NDT, Paris Traceroute, and SideStream.
package parser

import (
	"log"

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
		// TODO - substitute appropriate parsers here and below.
		return NewTestParser(ins)
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
type NullParser struct {
	etl.Parser
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
	inserter etl.Inserter
}

func NewTestParser(ins etl.Inserter) etl.Parser {
	return &TestParser{ins}
}

func (tp *TestParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error {
	metrics.TestCount.WithLabelValues("table", "test", "ok").Inc()
	log.Printf("Parsing %s", testName)
	values := make(map[string]bigquery.Value, len(meta)+1)
	// TODO is there a better way to do this?
	for k, v := range meta {
		values[k] = v
	}
	values["testname"] = testName
	return tp.inserter.InsertRow(bq.MapSaver{values})
}

func (tp *TestParser) TableName() string {
	return "test-table"
}
