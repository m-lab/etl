// Package parser defines the Parser interface and implementations for the different
// test types, NDT, Paris Traceroute, and SideStream.
package parser

// This file defines the Parser subtype that handles DISCO data.

import (
	"bytes"
	"encoding/json"
	"log"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/schema"
)

//=====================================================================================
//                       Disco Parser
//=====================================================================================

// TODO(dev) add tests
type DiscoParser struct {
	inserter     etl.Inserter
	etl.RowStats // RowStats implemented for DiscoParser with an embedded struct.
}

func NewDiscoParser(ins etl.Inserter) etl.Parser {
	return &DiscoParser{
		inserter: ins,
		RowStats: ins} // Delegate RowStats functions to the Inserter.
}

func (dp *DiscoParser) TaskError() error {
	return nil
}

// Disco data a JSON representation that should be pushed directly into BigQuery.
// For now, though, we parse into a struct, for compatibility with current inserter
// backend.
//
// Returns:
//   error on Decode error
//   error on InsertRows error
//   nil on success
//
// TODO - optimize this to use the JSON directly, if possible.
func (dp *DiscoParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error {
	metrics.WorkerState.WithLabelValues(dp.TableName(), "switch").Inc()
	defer metrics.WorkerState.WithLabelValues(dp.TableName(), "switch").Dec()
	meta["testname"] = testName
	ms := schema.Meta{
		FileName:  meta["filename"].(string),
		TestName:  meta["testname"].(string),
		ParseTime: meta["parse_time"].(time.Time).Unix(),
	}

	// Measure the distribution of disco file sizes. (bytes / file)
	// TODO: add table label, add extension label.
	metrics.FileSizeHistogram.WithLabelValues("normal").Observe(float64(len(test)))

	rdr := bytes.NewReader(test)
	dec := json.NewDecoder(rdr)
	rowCount := 0

	for dec.More() {
		var stats schema.SwitchStats
		stats.Meta = ms
		err := dec.Decode(&stats)
		if err != nil {
			metrics.TestCount.WithLabelValues(
				dp.TableName(), "disco", "Decode").Inc()
			// TODO(dev) Should accumulate errors, instead of aborting?
			return err
		}
		rowCount++

		// Count the number of samples per record.
		metrics.DeltaNumFieldsHistogram.WithLabelValues(
			dp.TableName()).Observe(float64(len(stats.Sample)))

		// TODO: measure metrics.RowSizeHistogram every so often with json size.
		metrics.RowSizeHistogram.WithLabelValues(
			dp.TableName()).Observe(float64(stats.Size()))

		err = dp.inserter.InsertRow(stats)
		if err != nil {
			switch t := err.(type) {
			case bigquery.PutMultiError:
				// TODO improve error handling??
				metrics.TestCount.WithLabelValues(
					dp.TableName(), "disco", "insert-multi").Inc()
				log.Printf("%v\n", t[0].Error())
			default:
				metrics.TestCount.WithLabelValues(
					dp.TableName(), "disco", "insert-other").Inc()
			}
			// TODO(dev) Should accumulate errors, instead of aborting?
			return err
		}
	}

	// Measure the distribution of records per file.
	metrics.EntryFieldCountHistogram.WithLabelValues(
		dp.TableName()).Observe(float64(rowCount))

	metrics.TestCount.WithLabelValues(dp.TableName(), "disco", "ok").Inc()

	return nil
}

// These functions are also required to complete the etl.Parser interface.  For Disco,
// we just forward the calls to the Inserter.
func (dp *DiscoParser) Flush() error {
	return dp.inserter.Flush()
}

func (dp *DiscoParser) TableName() string {
	return dp.inserter.TableBase()
}

func (dp *DiscoParser) FullTableName() string {
	return dp.inserter.FullTableName()
}
