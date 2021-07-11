package parser

// This file defines the Parser subtype that handles NDT5Result data.

import (
	"bytes"
	"encoding/json"
	"log"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	v2as "github.com/m-lab/annotation-service/api/v2"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/etl/schema"
)

//=====================================================================================
//                       NDT5Result Parser
//=====================================================================================

// NDT5ResultParser handles parsing of NDT5Result archives.
type NDT5ResultParser struct {
	*row.Base
	table  string
	suffix string
}

// NewNDT5ResultParser returns a parser for NDT5Result archives.
func NewNDT5ResultParser(sink row.Sink, label, suffix string, ann v2as.Annotator) etl.Parser {
	bufSize := etl.NDT5.BQBufferSize()
	if ann == nil {
		ann = v2as.GetAnnotator(etl.BatchAnnotatorURL)
	}

	return &NDT5ResultParser{
		Base:   row.NewBase(label, sink, bufSize, ann),
		table:  label,
		suffix: suffix,
	}
}

// TaskError returns non-nil if the task had enough failures to justify
// recording the entire task as in error.  For now, this is any failure
// rate exceeding 10%.
func (dp *NDT5ResultParser) TaskError() error {
	stats := dp.GetStats()
	if stats.Total() < 10*stats.Failed {
		log.Printf("Warning: high row commit errors (more than 10%%): %d failed of %d accepted\n",
			stats.Failed, stats.Total())
		return etl.ErrHighInsertionFailureRate
	}
	return nil
}

// IsParsable returns the canonical test type and whether to parse data.
func (dp *NDT5ResultParser) IsParsable(testName string, data []byte) (string, bool) {
	// Files look like: "<UUID>.json"
	if strings.HasSuffix(testName, "json") {
		return "ndt5_result", true
	}
	return "unknown", false
}

// NOTE: data.NDT5Result is a JSON object that should be pushed directly into BigQuery.
// We read the value into a struct, for compatibility with current inserter
// backend and to eventually rely on the schema inference in m-lab/go/cloud/bqx.CreateTable().

// ParseAndInsert decodes the data.NDT5Result JSON and inserts it into BQ.
func (dp *NDT5ResultParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error {
	// TODO: derive 'ndt5' (or 'ndt7') labels from testName.
	metrics.WorkerState.WithLabelValues(dp.TableName(), "ndt5_result").Inc()
	defer metrics.WorkerState.WithLabelValues(dp.TableName(), "ndt5_result").Dec()

	// An older version of the NDT result struct used a JSON object (Go map) to
	// store ClientMetadata. Results in that format will fail to parse. This step
	// simply removes the ClientMetadta formatted as a JSON object so that the
	// parsing will succeed. This should only apply to data from 2019-07-17 (v0.10)
	// to 2019-08-26 (v0.12). For these tests the ClientMetadata will be empty.
	var re = regexp.MustCompile(`,"ClientMetadata":{[^}]+}`)
	test = []byte(re.ReplaceAllString(string(test), ``))

	rdr := bytes.NewReader(test)
	dec := json.NewDecoder(rdr)

	for dec.More() {
		stats := schema.NDT5ResultRow{
			TestID: testName,
			ParseInfo: &schema.ParseInfoV0{
				TaskFileName:  meta["filename"].(string),
				ParseTime:     time.Now(),
				ParserVersion: Version(),
			},
		}
		err := dec.Decode(&stats.Result)
		if err != nil {
			log.Println(err)
			metrics.TestCount.WithLabelValues(
				dp.TableName(), "ndt5_result", "Decode").Inc()
			return err
		}

		// Set the LogTime to the Result.StartTime
		stats.LogTime = stats.Result.StartTime.Unix()

		// Estimate the row size based on the input JSON size.
		metrics.RowSizeHistogram.WithLabelValues(
			dp.TableName()).Observe(float64(len(test)))

		if err = dp.Base.Put(&stats); err != nil {
			return err
		}
		// Count successful inserts.
		metrics.TestCount.WithLabelValues(dp.TableName(), "ndt5_result", "ok").Inc()
	}

	return nil
}

// NB: These functions are also required to complete the etl.Parser interface.
// For NDT5Result, we just forward the calls to the Inserter.

func (dp *NDT5ResultParser) Flush() error {
	return dp.Base.Flush()
}

func (dp *NDT5ResultParser) TableName() string {
	return dp.table
}

func (dp *NDT5ResultParser) FullTableName() string {
	return dp.table + dp.suffix
}

// RowsInBuffer returns the count of rows currently in the buffer.
func (dp *NDT5ResultParser) RowsInBuffer() int {
	return dp.GetStats().Pending
}

// Committed returns the count of rows successfully committed to BQ.
func (dp *NDT5ResultParser) Committed() int {
	return dp.GetStats().Committed
}

// Accepted returns the count of all rows received through InsertRow(s)
func (dp *NDT5ResultParser) Accepted() int {
	return dp.GetStats().Total()
}

// Failed returns the count of all rows that could not be committed.
func (dp *NDT5ResultParser) Failed() int {
	return dp.GetStats().Failed
}
