// The parser package defines the Parser interface and implementations for the different
// test types, NDT, Paris Traceroute, and SideStream.
package parser

import (
	"cloud.google.com/go/bigquery"
	"log"

	"github.com/prometheus/client_golang/prometheus"
)

//=====================================================================================
//                       Parser Interface and implementations
//=====================================================================================
type Parser interface {
	// fn - Name of test file
	// table - biq query table name (for error logging only)
	// test - binary test data
	HandleTest(fn string, table string, test []byte) (interface{}, error)
}

type NullParser struct {
	Parser
}

func (np *NullParser) HandleTest(fn string, table string, test []byte) (interface{}, error) {
	test_count.With(prometheus.Labels{"table": table}).Inc()
	return nil, nil
}

// TestParser ignores the content, returns a map[string]Value "filename":"..."
// TODO add tests
type TestParser struct {
	Parser
}

// TODO - use or delete this struct
type FileNameSaver struct {
	values map[string]bigquery.Value
	bigquery.ValueSaver
}

func (fns *FileNameSaver) Save() (row map[string]bigquery.Value, insertID string, err error) {
	return fns.values, "", nil
}

func (np *TestParser) HandleTest(fn string, table string, test []byte) (interface{}, error) {
	test_count.With(prometheus.Labels{"table": table}).Inc()
	log.Printf("Parsing %s", fn)
	return struct{ Filename string }{fn}, nil
	//	return FileNameSaver{map[string]bigquery.Value{"filename": fn}, nil}, nil
}

//=====================================================================================
//                       Prometheus Monitoring
//=====================================================================================

var (
	test_count = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "etl_parser_test_count",
		Help: "Number of tests processed.",
	}, []string{"table"})

	failure_count = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "etl_parser_failure_count",
		Help: "Number of test processing failures.",
	}, []string{"table", "failure_type"})
)

func init() {
	prometheus.MustRegister(test_count)
	prometheus.MustRegister(failure_count)
}
