package parser

import (
	"log"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	"cloud.google.com/go/civil"
	"github.com/m-lab/annotation-service/api"
	v2as "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/uuid-annotator/annotator"
)

//=====================================================================================
//                       NDTTraceToAnnotation Parser
//=====================================================================================

// Unfortunately, there are only about 50 to 100 ndttrace files per archive, so there will be a LOT
// of archives - roughly 20K per day in 2018, and 50K per day in 2019.

// NDTTraceToAnnotation parses the NDTTrace datatype and produces synthetic uuid-annotator records.
type NDTTraceToAnnotation struct {
	*row.Base
	table  string
	suffix string
}

// NewNDTTraceToAnnotation creates a new parser for annotation data.
func NewNDTTraceToAnnotation(sink row.Sink, label, suffix string, ann v2as.Annotator) etl.Parser {
	bufSize := etl.ANNOTATION.BQBufferSize()
	if ann == nil {
		ann = &nullAnnotator{}
	}

	return &NDTTraceToAnnotation{
		Base:   row.NewBase(label, sink, bufSize, ann),
		table:  label,
		suffix: suffix,
	}
}

// TaskError returns non-nil if the task had enough failures to justify
// recording the entire task as in error.  For now, this is any failure
// rate exceeding 10%.
func (ap *NDTTraceToAnnotation) TaskError() error {
	stats := ap.GetStats()
	if stats.Total() < 10*stats.Failed {
		log.Printf("Warning: high row commit errors (more than 10%%): %d failed of %d accepted\n",
			stats.Failed, stats.Total())
		return etl.ErrHighInsertionFailureRate
	}
	return nil
}

// IsParsable returns the canonical test type and whether to parse data.
func (ap *NDTTraceToAnnotation) IsParsable(testName string, data []byte) (string, bool) {
	// Files look like: "<UUID>.json"
	if strings.HasSuffix(testName, "_ndttrace") || strings.HasSuffix(testName, "_ndttrace.gz") {
		return "ndttrace", true
	}
	return "unknown", false
}

// ParseAndInsert decodes the NDTTrace records, and produces synthetic annotation records.
func (ap *NDTTraceToAnnotation) ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error {
	metrics.WorkerState.WithLabelValues(ap.TableName(), "annotation").Inc()
	defer metrics.WorkerState.WithLabelValues(ap.TableName(), "annotation").Dec()

	synth := annotations{}

	// Construct UUID from filename
	synth.UUID = ""
	synth.Client.

		// NOTE: annotations are joined with other tables using the UUID, so
		// finegrain timestamp is not necessary.
		//
		// NOTE: Civil is not TZ adjusted. It takes the year, month, and date from
		// the given timestamp, regardless of the timestamp's timezone. Since we
		// run our systems in UTC, all timestamps will be relative to UTC and as
		// will these dates.
		row.Date = meta["date"].(civil.Date)

	// Estimate the row size based on the input JSON size.
	metrics.RowSizeHistogram.WithLabelValues(ap.TableName()).Observe(float64(len(test)))

	// Insert the row.
	if err = ap.Base.Put(&row); err != nil {
		return err
	}

	// Count successful inserts.
	metrics.TestCount.WithLabelValues(ap.TableName(), "annotation", "ok").Inc()
	return nil
}

// NB: These functions are also required to complete the etl.Parser interface.
// For Annotation, we just forward the calls to the Inserter.

func (ap *NDTTraceToAnnotation) Flush() error {
	return ap.Base.Flush()
}

func (ap *NDTTraceToAnnotation) TableName() string {
	return ap.table
}

func (ap *NDTTraceToAnnotation) FullTableName() string {
	return ap.table + ap.suffix
}

// RowsInBuffer returns the count of rows currently in the buffer.
func (ap *NDTTraceToAnnotation) RowsInBuffer() int {
	return ap.GetStats().Pending
}

// Committed returns the count of rows successfully committed to BQ.
func (ap *NDTTraceToAnnotation) Committed() int {
	return ap.GetStats().Committed
}

// Accepted returns the count of all rows received through InsertRow(s)
func (ap *NDTTraceToAnnotation) Accepted() int {
	return ap.GetStats().Total()
}

// Failed returns the count of all rows that could not be committed.
func (ap *NDTTraceToAnnotation) Failed() int {
	return ap.GetStats().Failed
}

//=====================================================================================
// This wraps the Annotations struct, and implements Annotatable
// so we can fill in Client and Server from Annotation Service.
//=====================================================================================

type annotations struct {
	annotator.Annotations
	serverIP string // Used by Annotatable
	clientIP string // Used by Annotatable
	logTime  time.Time
}

func (row *annotations) GetClientIPs() []string {
	return []string{row.clientIP}
}

func (row *annotations) GetServerIP() string {
	return row.serverIP
}

func (row *annotations) AnnotateClients(remote map[string]*api.Annotations) error {
	row.Client = remote[row.GetClientIPs()[0]]
	return nil
}

func (row *annotations) AnnotateServer(local *api.GeoData) error {
	row.Server = local
	return nil
}

func (row *annotations) GetLogTime() time.Time {
	return row.logTime
}

func assertTestRowAnnotatable(r *Row) {
	func(annotatable) {}(r)
}
