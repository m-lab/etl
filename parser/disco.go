// Package parser defines the Parser interface and implementations for the different
// data types.
package parser

// This file defines the Parser subtype that handles DISCO data.

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

// IsParsable returns the canonical test type and whether to parse data.
func (dp *DiscoParser) IsParsable(testName string, data []byte) (string, bool) {
	// Files look like: "<date>-to-<date>-switch.json.gz"
	// Notice the "-" before switch.
	// Look for JSON and JSONL files.
	if strings.HasSuffix(testName, "switch.json") ||
		strings.HasSuffix(testName, "switch.jsonl") ||
		strings.HasSuffix(testName, "switch.json.gz") ||
		strings.HasSuffix(testName, "switch.jsonl.gz") {
		return "switch", true
	}
	return "unknown", false
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

	rdr := bytes.NewReader(test)
	dec := json.NewDecoder(rdr)
	rowCount := 0

	for dec.More() {
		stats := schema.SwitchStats{
			TaskFilename:  meta["filename"].(string),
			TestID:        testName,
			ParseTime:     time.Now(),
			ParserVersion: Version(),
			// TODO: original archive "log_time" is unknown.
		}
		tmp := schema.SwitchStats{}
		err := dec.Decode(&tmp)
		if err != nil {
			metrics.TestCount.WithLabelValues(
				dp.TableName(), "disco", "Decode").Inc()
			// TODO(dev) Should accumulate errors, instead of aborting?
			return err
		}
		rowCount++

		// For collectd in the "utilization" experiment, by design, the raw data
		// time range starts and ends on the hour. This means that the raw
		// dataset inclues 361 time bins (360 + 1 extra). Originally, this was
		// so the last sample of the current time range would overlap with the
		// first sample of the next time range. However, this parser does not
		// use the extra sample, so we unconditionally ignore it here. However,
		// this is not the case for DISCOv2, so we use the whole sample from
		// DISCOv2. DISCOv2 can be differentiated from collectd by the "jsonl"
		// suffix.
		if len(tmp.Sample) > 0 {
			if strings.HasSuffix(testName, "switch.jsonl") ||
				strings.HasSuffix(testName, "switch.jsonl.gz") {
				stats.Sample = tmp.Sample
			} else {
				stats.Sample = tmp.Sample[:len(tmp.Sample)-1]
				// DISCOv1's Timestamp field in each sample represents the
				// *beginning* of a 10s sample window, while v2's Timestamp
				// represents the time at which the sample was taken, which is
				// representative of the previous 10s. Since v2's behavior is
				// what we want, we add 10s to all v1 Timestamps so that the
				// timestamps represent the same thing for v1 and v2.
				for i, v := range stats.Sample {
					stats.Sample[i].Timestamp = v.Timestamp + 10
				}
			}
		}

		// Copy remaining fields.
		stats.Metric = tmp.Metric
		stats.Hostname = tmp.Hostname
		stats.Experiment = tmp.Experiment

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
		// Count successful inserts.
		metrics.TestCount.WithLabelValues(dp.TableName(), "disco", "ok").Inc()
	}

	// Measure the distribution of records per file.
	metrics.EntryFieldCountHistogram.WithLabelValues(
		dp.TableName()).Observe(float64(rowCount))

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
