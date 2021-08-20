package parser

import (
	"encoding/json"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	v2as "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/etl/schema"
)

//=====================================================================================
//                       HopAnnotation1 Parser
//=====================================================================================

// HopAnnotation1Parser handles parsing for the HopAnnotation1 datatype.
type HopAnnotation1Parser struct {
	*row.Base
	table  string
	suffix string
}

// NewHopAnnotation1Parser returns a new parser for the HopAnnotation1 archives.
func NewHopAnnotation1Parser(sink row.Sink, table, suffix string, ann v2as.Annotator) etl.Parser {
	bufSize := etl.HOPANNOTATION1.BQBufferSize()
	if ann == nil {
		ann = v2as.GetAnnotator(etl.BatchAnnotatorURL)
	}

	return &HopAnnotation1Parser{
		Base:   row.NewBase(table, sink, bufSize, ann),
		table:  table,
		suffix: suffix,
	}
}

// IsParsable returns the canonical test type and whether to parse data.
func (p *HopAnnotation1Parser) IsParsable(testName string, data []byte) (string, bool) {
	if strings.HasSuffix(testName, "json") {
		return "hopannotation1", true
	}
	return "", false
}

// ParseAndInsert decodes the HopAnnotation1 data and inserts it into BQ.
func (p *HopAnnotation1Parser) ParseAndInsert(fileMetadata map[string]bigquery.Value, testName string, rawContent []byte) error {
	metrics.WorkerState.WithLabelValues(p.TableName(), "hopannotation1").Inc()
	defer metrics.WorkerState.WithLabelValues(p.TableName(), "hopannotation1").Dec()

	row := schema.HopAnnotation1Row{
		Parser: schema.ParseInfo{
			Version:    Version(),
			Time:       time.Now(),
			ArchiveURL: fileMetadata["filename"].(string),
			Filename:   testName,
			GitCommit:  GitCommit(),
		},
	}

	raw := schema.HopAnnotation1{}
	err := json.Unmarshal(rawContent, &raw)
	if err != nil {
		metrics.TestCount.WithLabelValues(p.TableName(), "hopannotation1", "decode-location-error").Inc()
		return err
	}

	// Fill in the row.
	row.ID = raw.ID
	row.Raw = &raw
	// NOTE: Civil is not TZ adjusted. It takes the year, month, and date from
	// the given timestamp, regardless of the timestamp's timezone. Since we
	// run our systems in UTC, all timestamps will be relative to UTC and as
	// will these dates.
	row.Date = fileMetadata["date"].(civil.Date)

	// Estimate the row size based on the input JSON size.
	metrics.RowSizeHistogram.WithLabelValues(p.TableName()).Observe(float64(len(rawContent)))

	// Insert the row.
	err = p.Base.Put(&row)
	if err != nil {
		return err
	}
	// Count successful inserts.
	metrics.TestCount.WithLabelValues(p.TableName(), "hopannotation1", "ok").Inc()

	return nil
}

// NB: These functions are also required to complete the etl.Parser interface
// For HopAnnotation1, we just forward the calls to the Inserter.

func (p *HopAnnotation1Parser) Flush() error {
	return p.Base.Flush()
}

func (p *HopAnnotation1Parser) TableName() string {
	return p.table
}

func (p *HopAnnotation1Parser) FullTableName() string {
	return p.table + p.suffix
}

// RowsInBuffer returns the count of rows currently in the buffer.
func (p *HopAnnotation1Parser) RowsInBuffer() int {
	return p.GetStats().Pending
}

// Committed returns the count of rows successfully committed to BQ.
func (p *HopAnnotation1Parser) Committed() int {
	return p.GetStats().Committed
}

// Accepted returns the count of all rows received through InsertRow(s).
func (p *HopAnnotation1Parser) Accepted() int {
	return p.GetStats().Total()
}

// Failed returns the count of all rows that could not be committed.
func (p *HopAnnotation1Parser) Failed() int {
	return p.GetStats().Failed
}
