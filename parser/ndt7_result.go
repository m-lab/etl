package parser

// This file defines the Parser subtype that handles NDT7Result data.

import (
	"bytes"
	"encoding/json"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	v2as "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/etl/annotation"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/ndt-server/ndt7/model"
)

//=====================================================================================
//                       NDT7Result Parser
//=====================================================================================

// NDT7ResultParser
type NDT7ResultParser struct {
	*row.Base
	table  string
	suffix string
}

func NewNDT7ResultParser(sink row.Sink, table, suffix string, ann v2as.Annotator) etl.Parser {
	bufSize := etl.NDT7.BQBufferSize()
	if ann == nil {
		ann = v2as.GetAnnotator(annotation.BatchURL)
	}

	return &NDT7ResultParser{
		Base:   row.NewBase("ndt7", sink, bufSize, ann),
		table:  table,
		suffix: suffix,
	}
}

func (dp *NDT7ResultParser) TaskError() error {
	stats := dp.GetStats()
	if stats.Total() < 10*stats.Failed {
		log.Printf("Warning: high row insert errors (more than 10%%): %d failed of %d accepted\n",
			stats.Failed, stats.Total())
		return etl.ErrHighInsertionFailureRate
	}
	return nil
}

// IsParsable returns the canonical test type and whether to parse data.
func (dp *NDT7ResultParser) IsParsable(testName string, data []byte) (string, bool) {
	// Files look like: "<UUID>.json"
	if strings.HasPrefix(testName, "ndt7") && strings.HasSuffix(testName, "json") {
		return "ndt7_result", true
	}
	return "unknown", false
}

// ParseAndInsert decodes the data.NDT7Result JSON and inserts it into BQ.
func (dp *NDT7ResultParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error {
	// TODO: derive 'ndt5' (or 'ndt7') labels from testName.
	metrics.WorkerState.WithLabelValues(dp.TableName(), "ndt7_result").Inc()
	defer metrics.WorkerState.WithLabelValues(dp.TableName(), "ndt7_result").Dec()

	rdr := bytes.NewReader(test)
	dec := json.NewDecoder(rdr)

	for dec.More() {
		row := schema.NDT7ResultRow{
			ParseInfo: schema.ParseInfo{
				ArchiveURL:    meta["filename"].(string),
				ParseTime:     time.Now(),
				ParserVersion: Version(),
				Filename:      testName,
			},
		}
		err := dec.Decode(&row.Raw)
		if err != nil {
			log.Println(err)
			metrics.TestCount.WithLabelValues(
				dp.TableName(), "ndt7_result", "Decode").Inc()
			return err
		}
		row.TestTime = row.Raw.StartTime
		if row.Raw.Download != nil {
			row.A = downSummary(row.Raw.Download)
		} else if row.Raw.Upload != nil {
			row.A = upSummary(row.Raw.Upload)
		}

		// Estimate the row size based on the input JSON size.
		metrics.RowSizeHistogram.WithLabelValues(
			dp.TableName()).Observe(float64(len(test)))

		dp.Base.Put(&row)
		// Count successful inserts.
		metrics.TestCount.WithLabelValues(dp.TableName(), "ndt7_result", "ok").Inc()
	}

	return nil
}

func downSummary(down *model.ArchivalData) schema.NDT7Summary {
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
		// Convert to seconds.
		rtt = float64(m[len(m)-1].TCPInfo.MinRTT) / 1000000
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
