package parser

// This file defines the Parser subtype that handles NDTResult data.

import (
	"bytes"
	"encoding/json"
	"log"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	v2as "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/etl/annotation"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/etl/schema"
)

//=====================================================================================
//                       NDTResult Parser
//=====================================================================================

// NDTResultParser
type NDTResultParser struct {
	*row.Base
	table  string
	suffix string
}

func NewNDTResultParser(sink row.Sink, table, suffix string, ann v2as.Annotator) etl.Parser {
	bufSize := etl.NDT5.BQBufferSize()
	if ann == nil {
		ann = v2as.GetAnnotator(annotation.BatchURL)
	}

	return &NDTResultParser{
		Base:   row.NewBase("foobar", sink, bufSize, ann),
		table:  table,
		suffix: suffix,
	}
}

func (dp *NDTResultParser) TaskError() error {
	stats := dp.GetStats()
	if stats.Total() < 10*stats.Failed {
		log.Printf("Warning: high row insert errors (more than 10%%): %d failed of %d accepted\n",
			stats.Failed, stats.Total())
		return etl.ErrHighInsertionFailureRate
	}
	return nil
}

// IsParsable returns the canonical test type and whether to parse data.
func (dp *NDTResultParser) IsParsable(testName string, data []byte) (string, bool) {
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
func (dp *NDTResultParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error {
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

	for dec.More() {
		stats := schema.NDTResultRow{
			TestID: testName,
			ParseInfo: &schema.ParseInfo{
				TaskFileName:  meta["filename"].(string),
				ParseTime:     time.Now(),
				ParserVersion: Version(),
			},
		}
		err := dec.Decode(&stats.Result)
		if err != nil {
			log.Println(err)
			metrics.TestCount.WithLabelValues(
				dp.TableName(), "ndt_result", "Decode").Inc()
			return err
		}

		// Set the LogTime to the Result.StartTime
		stats.LogTime = stats.Result.StartTime.Unix()

		// Estimate the row size based on the input JSON size.
		metrics.RowSizeHistogram.WithLabelValues(
			dp.TableName()).Observe(float64(len(test)))

		dp.Base.Put(&stats)
		// Count successful inserts.
		metrics.TestCount.WithLabelValues(dp.TableName(), "ndt_result", "ok").Inc()
	}

	return nil
}

// NB: These functions are also required to complete the etl.Parser interface.
// For NDTResult, we just forward the calls to the Inserter.

func (dp *NDTResultParser) Flush() error {
	return dp.Base.Flush()
}

func (dp *NDTResultParser) TableName() string {
	return dp.table
}

func (dp *NDTResultParser) FullTableName() string {
	return dp.table + dp.suffix
}

// RowsInBuffer returns the count of rows currently in the buffer.
func (dp *NDTResultParser) RowsInBuffer() int {
	return dp.GetStats().Pending
}

// Committed returns the count of rows successfully committed to BQ.
func (dp *NDTResultParser) Committed() int {
	return dp.GetStats().Committed
}

// Accepted returns the count of all rows received through InsertRow(s)
func (dp *NDTResultParser) Accepted() int {
	return dp.GetStats().Total()
}

// Failed returns the count of all rows that could not be committed.
func (dp *NDTResultParser) Failed() int {
	return dp.GetStats().Failed
}
