// The parser package defines the Parser interface and implementations for the different
// test types, NDT, Paris Traceroute, and SideStream.
package parser

import (
	"cloud.google.com/go/bigquery"
	"github.com/prometheus/client_golang/prometheus"
)

//=====================================================================================
//                       Parser Interface and implementations
//=====================================================================================
type Parser interface {
	// fn - Name of test file
	// table - biq query table name (for error logging only)
	// test - binary test data
	HandleTest(fn string, table string, test []byte) (bigquery.ValueSaver, error)
}

type NullParser struct {
	Parser
}

func (np *NullParser) HandleTest(fn string, table string, test []byte) (bigquery.ValueSaver, error) {
	test_count.With(prometheus.Labels{"table": table}).Inc()
	return nil, nil
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
