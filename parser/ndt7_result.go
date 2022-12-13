package parser

// This file defines the Parser subtype that handles NDT7Result data.

import (
	"encoding/json"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	"cloud.google.com/go/civil"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/go/logx"
	"github.com/m-lab/ndt-server/metadata"
	"github.com/m-lab/ndt-server/ndt7/model"
)

//=====================================================================================
//                       NDT7Result Parser
//=====================================================================================

// NDT7ResultParser handles parsing of NDT7Result archives.
type NDT7ResultParser struct {
	*row.Base
	table  string
	suffix string
}

// NewNDT7ResultParser returns a parser for NDT7Result archives.
func NewNDT7ResultParser(sink row.Sink, table, suffix string) etl.Parser {
	bufSize := etl.NDT7.BQBufferSize()
	return &NDT7ResultParser{
		Base:   row.NewBase(table, sink, bufSize),
		table:  table,
		suffix: suffix,
	}
}

// TaskError returns non-nil if the task had enough failures to justify
// recording the entire task as in error.  For now, this is any failure
// rate exceeding 10%.
func (dp *NDT7ResultParser) TaskError() error {
	stats := dp.GetStats()
	if stats.Total() < 10*stats.Failed {
		log.Printf("Warning: high row commit errors (more than 10%%): %d failed of %d accepted\n",
			stats.Failed, stats.Total())
		return etl.ErrHighInsertionFailureRate
	}
	return nil
}

// IsParsable returns the canonical test type and whether to parse data.
func (dp *NDT7ResultParser) IsParsable(testName string, data []byte) (string, bool) {
	// Files look like:
	// ndt7-{upload,download}-YYYYMMDDTHHMMSS.066461502Z.<UUID>.json.gz
	if strings.Contains(testName, "ndt7") && (strings.HasSuffix(testName, "json.gz") || strings.HasSuffix(testName, "json")) {
		return "ndt7_result", true
	}
	logx.Debug.Println("ndt7 unknown file:", testName)
	return "unknown", false
}

// ParseAndInsert decodes the data.NDT7Result JSON and inserts it into BQ.
func (dp *NDT7ResultParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error {
	// TODO: derive 'ndt5' (or 'ndt7') labels from testName.
	metrics.WorkerState.WithLabelValues(dp.TableName(), "ndt7_result").Inc()
	defer metrics.WorkerState.WithLabelValues(dp.TableName(), "ndt7_result").Dec()

	row := schema.NDT7ResultRow{
		Parser: schema.ParseInfo{
			Version:    Version(),
			Time:       time.Now(),
			ArchiveURL: meta["filename"].(string),
			Filename:   testName,
			GitCommit:  GitCommit(),
		},
	}

	// Parse the test.
	err := json.Unmarshal(test, &row.Raw)
	if err != nil {
		log.Println(meta["filename"].(string), testName, err)
		metrics.TestTotal.WithLabelValues(dp.TableName(), "ndt7_result", "Unmarshal").Inc()
		return err
	}

	// This is a hack to deal with the ConnectionInfo fields that are not intended to be
	// exported to bigquery.  With the GCS row.Sink, we convert to json, but we cannot
	// tag the json, because the json tag is already used for the NDT7 client comms.
	if row.Raw.Download != nil {
		row.Raw.Download.ClientMetadata = filterClientMetadata(row.Raw.Download.ClientMetadata)
		if row.Raw.Download.ServerMeasurements != nil {
			for i := range row.Raw.Download.ServerMeasurements {
				row.Raw.Download.ServerMeasurements[i].ConnectionInfo = nil
			}
		}
	}
	if row.Raw.Upload != nil {
		row.Raw.Upload.ClientMetadata = filterClientMetadata(row.Raw.Upload.ClientMetadata)
		if row.Raw.Upload.ServerMeasurements != nil {
			for i := range row.Raw.Upload.ServerMeasurements {
				row.Raw.Upload.ServerMeasurements[i].ConnectionInfo = nil
			}
		}
	}

	// NOTE: Civil is not TZ adjusted. It takes the year, month, and date from
	// the given timestamp, regardless of the timestamp's timezone. Since we
	// run our systems in UTC, all timestamps will be relative to UTC and as
	// will these dates.
	row.Date = meta["date"].(civil.Date)
	if row.Raw.Download != nil {
		row.A = downSummary(row.Raw.Download)
	} else if row.Raw.Upload != nil {
		row.A = upSummary(row.Raw.Upload)
	} else {
		metrics.WarningCount.WithLabelValues(
			dp.TableName(), "ndt7", "download and upload are both nil").Inc()
	}
	row.ID = row.A.UUID

	// Estimate the row size based on the input JSON size.
	metrics.RowSizeHistogram.WithLabelValues(
		dp.TableName()).Observe(float64(len(test)))

	// Insert the row.
	err = dp.Base.Put(&row)
	if err != nil {
		return err
	}
	// Count successful inserts.
	metrics.TestTotal.WithLabelValues(dp.TableName(), "ndt7_result", "ok").Inc()
	return nil
}

// filter client names with known values.
var filterValues = map[string]bool{"YR1tXo2zVKzBIgURbjEwjw": true, "3OSmQKi2BqIpDDW42NHp1g": true}

func filterClientMetadata(md []metadata.NameValue) []metadata.NameValue {
	// Check if the md contains "client_name".
	found := false
	var hash string
	for i := range md {
		if md[i].Name == "client_name" {
			hash = base64hash(md[i].Value)
			found = true
		}
	}
	if !found {
		// No need to allocate new memory.
		return md
	}

	// If metadata includes "client_name", check for specific values.
	if !filterValues[hash] {
		// Still no need to do anything differently.
		return md
	}

	// We found a value that should be filtered.
	ret := []metadata.NameValue{}
	for i := range md {
		if md[i].Name != "client_name" {
			ret = append(ret, md[i])
		}
	}
	return ret
}

func downSummary(down *model.ArchivalData) schema.NDT7Summary {
	down.UUID = strings.ReplaceAll(down.UUID, "_unsafe", "")
	return schema.NDT7Summary{
		UUID:               down.UUID,
		TestTime:           down.StartTime,
		CongestionControl:  "bbr",
		MeanThroughputMbps: downRate(down.ServerMeasurements),
		MinRTT:             minRTT(down.ServerMeasurements),
		LossRate:           lossRate(down.ServerMeasurements),
	}
}
func upSummary(up *model.ArchivalData) schema.NDT7Summary {
	up.UUID = strings.ReplaceAll(up.UUID, "_unsafe", "")
	return schema.NDT7Summary{
		UUID:               up.UUID,
		TestTime:           up.StartTime,
		CongestionControl:  "bbr", // TODO: what is the right value here?
		MeanThroughputMbps: upRate(up.ServerMeasurements),
		MinRTT:             minRTT(up.ServerMeasurements),
		LossRate:           0, // TODO: what is the correct measure for upload?
	}
}

func lossRate(m []model.Measurement) float64 {
	var loss float64
	if len(m) > 0 {
		loss = float64(m[len(m)-1].TCPInfo.BytesRetrans) / float64(m[len(m)-1].TCPInfo.BytesSent)
	}
	return loss
}

func downRate(m []model.Measurement) float64 {
	var mbps float64
	if len(m) > 0 {
		// Convert to Mbps.
		mbps = 8 * float64(m[len(m)-1].TCPInfo.BytesAcked) / float64(m[len(m)-1].TCPInfo.ElapsedTime)
	}
	return mbps
}

func upRate(m []model.Measurement) float64 {
	var mbps float64
	if len(m) > 0 {
		// Convert to Mbps.
		mbps = 8 * float64(m[len(m)-1].TCPInfo.BytesReceived) / float64(m[len(m)-1].TCPInfo.ElapsedTime)
	}
	return mbps
}

func minRTT(m []model.Measurement) float64 {
	var rtt float64
	if len(m) > 0 {
		// Convert to milliseconds.
		rtt = float64(m[len(m)-1].TCPInfo.MinRTT) / 1000
	}
	return rtt
}

// NB: These functions are also required to complete the etl.Parser interface.
// For NDT7Result, we just forward the calls to the Inserter.

func (dp *NDT7ResultParser) Flush() error {
	return dp.Base.Flush()
}

func (dp *NDT7ResultParser) TableName() string {
	return dp.table
}

func (dp *NDT7ResultParser) FullTableName() string {
	return dp.table + dp.suffix
}

// RowsInBuffer returns the count of rows currently in the buffer.
func (dp *NDT7ResultParser) RowsInBuffer() int {
	return dp.GetStats().Pending
}

// Committed returns the count of rows successfully committed to BQ.
func (dp *NDT7ResultParser) Committed() int {
	return dp.GetStats().Committed
}

// Accepted returns the count of all rows received through InsertRow(s)
func (dp *NDT7ResultParser) Accepted() int {
	return dp.GetStats().Total()
}

// Failed returns the count of all rows that could not be committed.
func (dp *NDT7ResultParser) Failed() int {
	return dp.GetStats().Failed
}
