package parser

// This file defines the Parser subtype that handles NDTRow data.

import (
	"bytes"
	"encoding/json"
	"log"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/schema"
)

//=====================================================================================
//                       NDTRow Parser
//=====================================================================================
type NDTRowParser struct {
	inserter     etl.Inserter
	etl.RowStats // RowStats implemented for NDTRowParser with an embedded struct.
}

func NewNDTRowParser(ins etl.Inserter) etl.Parser {
	return &NDTRowParser{
		inserter: ins,
		RowStats: ins, // Delegate RowStats functions to the Inserter.
	}
}

func (dp *NDTRowParser) TaskError() error {
	return nil
}

// IsParsable returns the canonical test type and whether to parse data.
func (dp *NDTRowParser) IsParsable(testName string, data []byte) (string, bool) {
	// Files look like: "<UUID>.json"
	if strings.HasSuffix(testName, "json") {
		return "ndt_result", true
	}
	return "unknown", false
}

// NOTE: data.NDTResult is a JSON object that should be pushed directly into BigQuery.
// We read the value into a struct, for compatibility with current inserter
// backend and to eventually rely on the schema inference in m-lab/go/bqx.CreateTable().

// ParseAndInsert decodes the data.NDTResult JSON and inserts it into BQ.
func (dp *NDTRowParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error {
	// TODO: derive 'ndt5' (or 'ndt7') labels from testName.
	metrics.WorkerState.WithLabelValues(dp.TableName(), "ndt_result").Inc()
	defer metrics.WorkerState.WithLabelValues(dp.TableName(), "ndt_result").Dec()

	// An older version of the NDT result struct used a JSON object (Go map) to
	// store ClientMetadata. Results in that format will fail to parse. This step
	// simply removes the ClientMetadta formatted as a JSON object so that the
	// parsing will succeed. This should only apply to data from 2019-07-17 (v0.10)
	// to 2019-08-26 (v0.12). For these tests the ClientMetadata will be empty.
	var re = regexp.MustCompile(`,"ClientMetadata":{[^}]+}`)
	test = []byte(re.ReplaceAllString(string(test), ``))

	rdr := bytes.NewReader(test)
	dec := json.NewDecoder(rdr)
	rowCount := 0

	for dec.More() {
		stats := schema.NDTRow{
			TestID: testName,
			ParseInfo: &schema.ParseInfo{
				TaskFileName:  meta["filename"].(string),
				ParseTime:     time.Now(),
				ParserVersion: Version(),
			},
		}
		err := dec.Decode(&stats.Result)
		if err != nil {
			metrics.TestCount.WithLabelValues(
				dp.TableName(), "ndt_result", "Decode").Inc()
			return err
		}
		rowCount++

		// Set the LogTime to the Result.StartTime
		stats.LogTime = stats.Result.StartTime.Unix()

		// Estimate the row size based on the input JSON size.
		metrics.RowSizeHistogram.WithLabelValues(
			dp.TableName()).Observe(float64(len(test)))

		err = dp.inserter.InsertRow(stats)
		if err != nil {
			switch t := err.(type) {
			case bigquery.PutMultiError:
				// TODO improve error handling??
				metrics.TestCount.WithLabelValues(
					dp.TableName(), "ndt_result", "insert-multi").Inc()
				log.Printf("%v\n", t[0].Error())
			default:
				metrics.TestCount.WithLabelValues(
					dp.TableName(), "ndt_result", "insert-other").Inc()
			}
			return err
		}
		// Count successful inserts.
		metrics.TestCount.WithLabelValues(dp.TableName(), "ndt_result", "ok").Inc()
	}

	return nil
}

// NB: These functions are also required to complete the etl.Parser interface.
// For NDTRow, we just forward the calls to the Inserter.

func (dp *NDTRowParser) Flush() error {
	return dp.inserter.Flush()
}

func (dp *NDTRowParser) TableName() string {
	return dp.inserter.TableBase()
}

func (dp *NDTRowParser) FullTableName() string {
	return dp.inserter.FullTableName()
}
