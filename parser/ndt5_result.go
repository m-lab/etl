package parser

// This file defines the Parser subtype that handles NDT5Result data.

import (
	"encoding/json"
	"log"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"

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
func NewNDT5ResultParser(sink row.Sink, label, suffix string) etl.Parser {
	bufSize := etl.NDT5.BQBufferSize()
	return &NDT5ResultParser{
		Base:   row.NewBase(label, sink, bufSize),
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
	metrics.WorkerState.WithLabelValues(dp.TableName(), "ndt5_result").Inc()
	defer metrics.WorkerState.WithLabelValues(dp.TableName(), "ndt5_result").Dec()

	// An older version of the NDT result struct used a JSON object (Go map) to
	// store ClientMetadata. Results in that format will fail to parse. This step
	// simply removes the ClientMetadta formatted as a JSON object so that the
	// parsing will succeed. This should only apply to data from 2019-07-17 (v0.10)
	// to 2019-08-26 (v0.12). For these tests the ClientMetadata will be empty.
	var re = regexp.MustCompile(`,"ClientMetadata":{[^}]+}`)
	test = []byte(re.ReplaceAllString(string(test), ``))
	if len(test) == 0 {
		// This is an empty test.
		// NOTE: We may wish to record these for full e2e accounting.
		metrics.RowSizeHistogram.WithLabelValues(dp.TableName()).Observe(float64(len(test)))
		return nil
	}

	parser := schema.ParseInfo{
		Version:    Version(),
		Time:       time.Now(),
		ArchiveURL: meta["filename"].(string),
		Filename:   testName,
		GitCommit:  GitCommit(),
	}
	date := meta["date"].(civil.Date)

	// Since ndt5 rows can include both download (S2C) and upload (C2S)
	// measurements (or neither), check and write independent rows for either
	// direction. This approach results in one row for upload, one row for
	// download just like the ndt7 data. The `Raw.Control` structure will be
	// shared when there are upload and download measurements on the same test.

	// S2C
	result, err := dp.newResult(test, parser, date)
	if err != nil {
		metrics.TestTotal.WithLabelValues(dp.TableName(), "ndt5_result", "Decode").Inc()
		return err
	}
	if result.Raw.S2C != nil && result.Raw.S2C.UUID != "" {
		dp.prepareS2CRow(result)
		if err = dp.Base.Put(result); err != nil {
			return err
		}
	}

	// C2S
	result, err = dp.newResult(test, parser, date)
	if err != nil {
		metrics.TestTotal.WithLabelValues(dp.TableName(), "ndt5_result", "Decode").Inc()
		return err
	}
	if result.Raw.C2S != nil && result.Raw.C2S.UUID != "" {
		dp.prepareC2SRow(result)
		if err = dp.Base.Put(result); err != nil {
			return err
		}
	}

	// Neither C2S nor S2C
	result, err = dp.newResult(test, parser, date)
	if err != nil {
		metrics.TestTotal.WithLabelValues(dp.TableName(), "ndt5_result", "Decode").Inc()
		return err
	}
	if result.Raw.C2S == nil && result.Raw.S2C == nil {
		result.ID = result.Raw.Control.UUID
		result.A = nil // nothing to summarize.
		if err = dp.Base.Put(result); err != nil {
			return err
		}
	}

	// Estimate the row size based on the input JSON size.
	metrics.RowSizeHistogram.WithLabelValues(dp.TableName()).Observe(float64(len(test)))

	// Count successful inserts.
	metrics.TestTotal.WithLabelValues(dp.TableName(), "ndt5_result", "ok").Inc()
	return nil
}

func (dp *NDT5ResultParser) newResult(test []byte, parser schema.ParseInfo, date civil.Date) (*schema.NDT5ResultRowV2, error) {
	result := &schema.NDT5ResultRowV2{
		Parser: parser,
		Date:   date,
	}
	err := json.Unmarshal(test, &result.Raw)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (dp *NDT5ResultParser) prepareS2CRow(row *schema.NDT5ResultRowV2) {
	// Record S2C result.
	s2c := row.Raw.S2C
	s2c.UUID = strings.ReplaceAll(s2c.UUID, "_unsafe", "")
	row.ID = s2c.UUID
	row.A = &schema.NDT5Summary{
		UUID:               s2c.UUID,
		TestTime:           s2c.StartTime,
		MeanThroughputMbps: s2c.MeanThroughputMbps,
		CongestionControl:  "cubic",
	}

	// The ndt5 result added the TCPInfo snapshot starting June 18 2020.
	// Since the s2c.MinRTT value is a lower resolution user-space estimate,
	// use TCPInfo if present, and fall back to the low resolution MinRTT otherwise.
	if s2c.TCPInfo != nil {
		// TCPInfo.MinRTT is a uint32.
		row.A.MinRTT = float64(s2c.TCPInfo.MinRTT) / 1000.0 / 1000.0
	} else {
		// MinRTT is a time.Duration.
		row.A.MinRTT = float64(s2c.MinRTT) / float64(time.Millisecond)
	}
	// NOTE: the TCPInfo structure was introduced in v0.18.0. Measurements
	// from earlier versions will not have values in the TCPInfo struct here.
	if s2c.TCPInfo != nil && s2c.TCPInfo.BytesSent > 0 {
		row.A.LossRate = float64(s2c.TCPInfo.BytesRetrans) / float64(s2c.TCPInfo.BytesSent)
	}
	row.Raw.C2S = nil
}

func (dp *NDT5ResultParser) prepareC2SRow(row *schema.NDT5ResultRowV2) {
	// Record C2S result.
	c2s := row.Raw.C2S
	c2s.UUID = strings.ReplaceAll(c2s.UUID, "_unsafe", "")
	row.ID = c2s.UUID
	row.A = &schema.NDT5Summary{
		UUID:               c2s.UUID,
		TestTime:           c2s.StartTime,
		MeanThroughputMbps: c2s.MeanThroughputMbps,
		CongestionControl:  "unknown",
		MinRTT:             -1, // unknown.
		LossRate:           -1, // unknown.
	}
	row.Raw.S2C = nil
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
