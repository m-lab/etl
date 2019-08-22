package parser

// This file defines the Parser subtype that handles NDTResult data.

import (
	"bytes"
	"encoding/json"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/schema"
)

//=====================================================================================
//                       NDTResult Parser
//=====================================================================================
type NDTResultParser struct {
	inserter     etl.Inserter
	etl.RowStats // RowStats implemented for NDTResultParser with an embedded struct.
}

func NewNDTResultParser(ins etl.Inserter) etl.Parser {
	return &NDTResultParser{
		inserter: ins,
		RowStats: ins, // Delegate RowStats functions to the Inserter.
	}
}

func (dp *NDTResultParser) TaskError() error {
	return nil
}

// IsParsable returns the canonical test type and whether to parse data.
func (dp *NDTResultParser) IsParsable(testName string, data []byte) (string, bool) {
	// Files look like: "<UUID>.json"
	if !strings.HasSuffix(testName, "json") {
		return "unknown", false
	}
	// Earlier versions of the unified result objects recorded the ClientMetadata
	// as an object, which cannot parse with the new struct.
	if strings.Contains(string(data), `"ClientMetadata":{`) {
		return "unknown", false
	}
	return "ndt_result", true
}

// NOTE: NDTResult data is a JSON object that should be pushed directly into BigQuery.
// We read the value into a struct, for compatibility with current inserter
// backend and to eventually rely on the schema inference in m-lab/go/bqx.CreateTable().

// ParseAndInsert decodes the NDT Result JSON data and inserts it into BQ.
func (dp *NDTResultParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error {
	// TODO: derive 'ndt5' (or 'ndt7') labels from testName.
	metrics.WorkerState.WithLabelValues(dp.TableName(), "ndt_result").Inc()
	defer metrics.WorkerState.WithLabelValues(dp.TableName(), "ndt_result").Dec()

	rdr := bytes.NewReader(test)
	dec := json.NewDecoder(rdr)
	rowCount := 0

	for dec.More() {
		stats := schema.NDTResult{
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
// For NDTResult, we just forward the calls to the Inserter.

func (dp *NDTResultParser) Flush() error {
	return dp.inserter.Flush()
}

func (dp *NDTResultParser) TableName() string {
	return dp.inserter.TableBase()
}

func (dp *NDTResultParser) FullTableName() string {
	return dp.inserter.FullTableName()
}
