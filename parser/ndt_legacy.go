// Package parser defines the Parser interface and implementations for the different
// test types, NDT, Paris Traceroute, and SideStream.
package parser

// This file defines the Parser subtype that handles NDTLegacy data.

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
//                       NDTLegacy Parser
//=====================================================================================
type NDTLegacyParser struct {
	inserter     etl.Inserter
	etl.RowStats // RowStats implemented for NDTLegacyParser with an embedded struct.
}

func NewNDTLegacyParser(ins etl.Inserter) etl.Parser {
	return &NDTLegacyParser{
		inserter: ins,
		RowStats: ins} // Delegate RowStats functions to the Inserter.
}

func (dp *NDTLegacyParser) TaskError() error {
	return nil
}

// IsParsable returns the canonical test type and whether to parse data.
func (dp *NDTLegacyParser) IsParsable(testName string, data []byte) (string, bool) {
	// Files look like: "<UUID>.json"
	if strings.HasSuffix(testName, "json") {
		return "ndt_legacy", true
	}
	return "unknown", false
}

// NOTE: NDTLegacy data is a JSON object that should be pushed directly into BigQuery.
// We read the value into a struct, for compatibility with current inserter
// backend and to eventually rely on the schema inference in m-lab/go/bqx.CreateTable().

// ParseAndInsert decodes the NDT Result JSON data and inserts it into BQ.
func (dp *NDTLegacyParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error {
	metrics.WorkerState.WithLabelValues(dp.TableName(), "ndt_legacy").Inc()
	defer metrics.WorkerState.WithLabelValues(dp.TableName(), "ndt_legacy").Dec()

	rdr := bytes.NewReader(test)
	dec := json.NewDecoder(rdr)
	rowCount := 0

	for dec.More() {
		stats := schema.NDTLegacySchema{
			TaskFilename:  meta["filename"].(string),
			TestID:        testName,
			ParseTime:     time.Now(),
			ParserVersion: Version(),
		}
		err := dec.Decode(&stats.Result)
		if err != nil {
			metrics.TestCount.WithLabelValues(
				dp.TableName(), "ndt_legacy", "Decode").Inc()
			return err
		}
		rowCount++

		// Set the LogTime to the Result.StartTime
		stats.LogTime = stats.Result.StartTime.Unix()

		// Count the number of samples per record.
		metrics.DeltaNumFieldsHistogram.WithLabelValues(
			dp.TableName()).Observe(1.0)

		// Estimate the row size based on the input JSON size.
		metrics.RowSizeHistogram.WithLabelValues(
			dp.TableName()).Observe(float64(len(test)))

		log.Println("Inserting:", stats)
		err = dp.inserter.InsertRow(stats)
		if err != nil {
			switch t := err.(type) {
			case bigquery.PutMultiError:
				// TODO improve error handling??
				metrics.TestCount.WithLabelValues(
					dp.TableName(), "ndt_legacy", "insert-multi").Inc()
				log.Printf("%v\n", t[0].Error())
			default:
				metrics.TestCount.WithLabelValues(
					dp.TableName(), "ndt_legacy", "insert-other").Inc()
			}
			return err
		}
		// Count successful inserts.
		metrics.TestCount.WithLabelValues(dp.TableName(), "ndt_legacy", "ok").Inc()
	}

	// There should always be one row per file.
	metrics.EntryFieldCountHistogram.WithLabelValues(
		dp.TableName()).Observe(float64(rowCount))

	return nil
}

// NB: These functions are also required to complete the etl.Parser interface.
// For NDTLegacy, we just forward the calls to the Inserter.

func (dp *NDTLegacyParser) Flush() error {
	return dp.inserter.Flush()
}

func (dp *NDTLegacyParser) TableName() string {
	return dp.inserter.TableBase()
}

func (dp *NDTLegacyParser) FullTableName() string {
	return dp.inserter.FullTableName()
}
