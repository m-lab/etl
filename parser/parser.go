// The parser package defines the Parser interface and implementations for the different
// test types, NDT, Paris Traceroute, and SideStream.
package parser

import (
	"log"

	"cloud.google.com/go/bigquery"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
)

func NewParser(dt etl.DataType, ins etl.Inserter) etl.Parser {
	switch dt {
	case etl.NDT:
		// TODO - eliminate need for "/mnt/tmpfs"
		return NewNDTParser(ins, etl.DataTypeToTable[etl.NDT], "/mnt/tmpfs")
	case etl.SS:
		// TODO - substitute appropriate parsers here and below.
		return NewTestParser(ins)
	case etl.PT:
		return NewTestParser(ins)
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
	metrics.TestCount.With(prometheus.Labels{"table": np.TableName(), "type": "null"}).Inc()
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
	metrics.TestCount.With(prometheus.Labels{"table": tp.TableName(), "type": "test"}).Inc()
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
