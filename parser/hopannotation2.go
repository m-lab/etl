package parser

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/traceroute-caller/hopannotation"
)

//=====================================================================================
//                       HopAnnotation2 Parser
//=====================================================================================

// HopAnnotation2Parser handles parsing for the HopAnnotation2 datatype.
type HopAnnotation2Parser struct {
	*row.Base
	table  string
	suffix string
}

// NewHopAnnotation2Parser returns a new parser for the HopAnnotation2 archives.
func NewHopAnnotation2Parser(sink row.Sink, table, suffix string) etl.Parser {
	bufSize := etl.HOPANNOTATION2.BQBufferSize()
	return &HopAnnotation2Parser{
		Base:   row.NewBase(table, sink, bufSize),
		table:  table,
		suffix: suffix,
	}
}

// IsParsable returns the canonical test type and whether to parse data.
func (p *HopAnnotation2Parser) IsParsable(testName string, data []byte) (string, bool) {
	if strings.HasSuffix(testName, "json") {
		return "hopannotation2", true
	}
	return "", false
}

// ParseAndInsert decodes the HopAnnotation2 data and inserts it into BQ.
func (p *HopAnnotation2Parser) ParseAndInsert(meta etl.Metadata, testName string, rawContent []byte) error {
	metrics.WorkerState.WithLabelValues(p.TableName(), "hopannotation2").Inc()
	defer metrics.WorkerState.WithLabelValues(p.TableName(), "hopannotation2").Dec()

	row := schema.HopAnnotation2Row{
		Parser: schema.ParseInfo{
			Version:    meta.Version,
			Time:       time.Now(),
			ArchiveURL: meta.ArchiveURL,
			Filename:   testName,
			GitCommit:  meta.GitCommit,
		},
	}

	// TODO(soltesz): update traceroute-caller type.
	raw := hopannotation.HopAnnotation1{}
	err := json.Unmarshal(rawContent, &raw)
	if err != nil {
		metrics.TestTotal.WithLabelValues(p.TableName(), "hopannotation2", "decode-location-error").Inc()
		return err
	}

	// Fill in the row.
	row.ID = raw.ID
	row.Raw = &raw
	// NOTE: Civil is not TZ adjusted. It takes the year, month, and date from
	// the given timestamp, regardless of the timestamp's timezone. Since we
	// run our systems in UTC, all timestamps will be relative to UTC and as
	// will these dates.
	row.Date = meta.Date

	// Estimate the row size based on the input JSON size.
	metrics.RowSizeHistogram.WithLabelValues(p.TableName()).Observe(float64(len(rawContent)))

	// Insert the row.
	err = p.Base.Put(&row)
	if err != nil {
		return err
	}
	// Count successful inserts.
	metrics.TestTotal.WithLabelValues(p.TableName(), "hopannotation2", "ok").Inc()

	return nil
}

// NB: These functions are also required to complete the etl.Parser interface
// For HopAnnotation2, we just forward the calls to the Inserter.

func (p *HopAnnotation2Parser) Flush() error {
	return p.Base.Flush()
}

func (p *HopAnnotation2Parser) TableName() string {
	return p.table
}

func (p *HopAnnotation2Parser) FullTableName() string {
	return p.table + p.suffix
}

// RowsInBuffer returns the count of rows currently in the buffer.
func (p *HopAnnotation2Parser) RowsInBuffer() int {
	return p.GetStats().Pending
}

// Committed returns the count of rows successfully committed to BQ.
func (p *HopAnnotation2Parser) Committed() int {
	return p.GetStats().Committed
}

// Accepted returns the count of all rows received through InsertRow(s).
func (p *HopAnnotation2Parser) Accepted() int {
	return p.GetStats().Total()
}

// Failed returns the count of all rows that could not be committed.
func (p *HopAnnotation2Parser) Failed() int {
	return p.GetStats().Failed
}
