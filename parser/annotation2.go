package parser

import (
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/uuid-annotator/annotator"
)

//=====================================================================================
//                       Annotation2 Parser
//=====================================================================================

// Annotation2Parser parses the annotation datatype from the uuid-annotator.
type Annotation2Parser struct {
	*row.Base
	table  string
	suffix string
}

// NewAnnotation2Parser creates a new parser for annotation data.
func NewAnnotation2Parser(sink row.Sink, label, suffix string) etl.Parser {
	bufSize := etl.ANNOTATION2.BQBufferSize()
	return &Annotation2Parser{
		Base:   row.NewBase(label, sink, bufSize),
		table:  label,
		suffix: suffix,
	}
}

// TaskError returns non-nil if the task had enough failures to justify
// recording the entire task as in error.  For now, this is any failure
// rate exceeding 10%.
func (ap *Annotation2Parser) TaskError() error {
	stats := ap.GetStats()
	if stats.Total() < 10*stats.Failed {
		log.Printf("Warning: high row commit errors (more than 10%%): %d failed of %d accepted\n",
			stats.Failed, stats.Total())
		return etl.ErrHighInsertionFailureRate
	}
	return nil
}

// IsParsable returns the canonical test type and whether to parse data.
func (ap *Annotation2Parser) IsParsable(testName string, data []byte) (string, bool) {
	// Files look like: "<UUID>.json"
	if strings.HasSuffix(testName, "json") {
		return "annotation2", true
	}
	return "unknown", false
}

// ParseAndInsert decodes the data.Annotation2 JSON and inserts it into BQ.
func (ap *Annotation2Parser) ParseAndInsert(meta etl.Metadata, testName string, test []byte) error {
	metrics.WorkerState.WithLabelValues(ap.TableName(), "annotation2").Inc()
	defer metrics.WorkerState.WithLabelValues(ap.TableName(), "annotation2").Dec()

	row := schema.Annotation2Row{
		Parser: schema.ParseInfo{
			Version:    meta.Version,
			Time:       time.Now(),
			ArchiveURL: meta.ArchiveURL,
			Filename:   testName,
			GitCommit:  meta.GitCommit,
		},
	}

	// Parse the raw test.
	raw := annotator.Annotations{}
	err := json.Unmarshal(test, &raw)
	if err != nil {
		log.Println(err)
		metrics.TestTotal.WithLabelValues(ap.TableName(), "annotation2", "decode-location-error").Inc()
		return err
	}

	// Fill in the row.
	row.UUID = raw.UUID
	row.Server = raw.Server
	row.Client = raw.Client

	// NOTE: Due to https://github.com/m-lab/etl/issues/1069, we mask the Region
	// field found in synthetic uuid annotations prior to 2020-03-12, and no
	// longer found in later Geo2 annotations.
	if row.Server.Geo != nil {
		row.Server.Geo.Region = ""
	}
	if row.Client.Geo != nil {
		row.Client.Geo.Region = ""
	}

	// NOTE: annotations are joined with other tables using the UUID, so
	// finegrain timestamp is not necessary.
	//
	// NOTE: Civil is not TZ adjusted. It takes the year, month, and date from
	// the given timestamp, regardless of the timestamp's timezone. Since we
	// run our systems in UTC, all timestamps will be relative to UTC and as
	// will these dates.
	row.Date = meta.Date

	// Estimate the row size based on the input JSON size.
	metrics.RowSizeHistogram.WithLabelValues(ap.TableName()).Observe(float64(len(test)))

	// Insert the row.
	if err = ap.Base.Put(&row); err != nil {
		return err
	}

	// Count successful inserts.
	metrics.TestTotal.WithLabelValues(ap.TableName(), "annotation2", "ok").Inc()
	return nil
}

// NB: These functions are also required to complete the etl.Parser interface.
// For Annotation, we just forward the calls to the Inserter.

func (ap *Annotation2Parser) Flush() error {
	return ap.Base.Flush()
}

func (ap *Annotation2Parser) TableName() string {
	return ap.table
}

func (ap *Annotation2Parser) FullTableName() string {
	return ap.table + ap.suffix
}

// RowsInBuffer returns the count of rows currently in the buffer.
func (ap *Annotation2Parser) RowsInBuffer() int {
	return ap.GetStats().Pending
}

// Committed returns the count of rows successfully committed to BQ.
func (ap *Annotation2Parser) Committed() int {
	return ap.GetStats().Committed
}

// Accepted returns the count of all rows received through InsertRow(s)
func (ap *Annotation2Parser) Accepted() int {
	return ap.GetStats().Total()
}

// Failed returns the count of all rows that could not be committed.
func (ap *Annotation2Parser) Failed() int {
	return ap.GetStats().Failed
}
