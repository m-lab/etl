package parser

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/annotation-service/api"
	v2as "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/etl/schema"
)

//=====================================================================================
//                       Annotation Parser
//=====================================================================================

// AnnotationParser parses the annotation datatype from the uuid-annotator.
type AnnotationParser struct {
	*row.Base
	table  string
	suffix string
}

type nullAnnotator struct{}

func (ann *nullAnnotator) GetAnnotations(ctx context.Context, date time.Time, ips []string, info ...string) (*v2as.Response, error) {
	return &v2as.Response{AnnotatorDate: time.Now(), Annotations: make(map[string]*api.Annotations, 0)}, nil
}

// NewAnnotationParser creates a new parser for annotation data.
func NewAnnotationParser(sink row.Sink, table, suffix string, ann v2as.Annotator) etl.Parser {
	bufSize := etl.ANNOTATION.BQBufferSize()
	if ann == nil {
		ann = &nullAnnotator{}
	}

	return &AnnotationParser{
		Base:   row.NewBase(table, sink, bufSize, ann),
		table:  table,
		suffix: suffix,
	}
}

func (ap *AnnotationParser) TaskError() error {
	stats := ap.GetStats()
	if stats.Total() < 10*stats.Failed {
		log.Printf("Warning: high row insert errors (more than 10%%): %d failed of %d accepted\n",
			stats.Failed, stats.Total())
		return etl.ErrHighInsertionFailureRate
	}
	return nil
}

// IsParsable returns the canonical test type and whether to parse data.
func (ap *AnnotationParser) IsParsable(testName string, data []byte) (string, bool) {
	// Files look like: "<UUID>.json"
	if strings.HasSuffix(testName, "json") {
		return "annotation", true
	}
	return "unknown", false
}

// ParseAndInsert decodes the data.Annotation JSON and inserts it into BQ.
func (ap *AnnotationParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error {
	metrics.WorkerState.WithLabelValues(ap.TableName(), "annotation").Inc()
	defer metrics.WorkerState.WithLabelValues(ap.TableName(), "annotation").Dec()

	row := schema.AnnotationRow{
		Parser: schema.ParseInfo{
			ArchiveURL: meta["filename"].(string),
			Time:       time.Now(),
			Version:    Version(),
			Filename:   testName,
		},
	}

	// Parse the test.
	err := json.Unmarshal(test, &row)
	if err != nil {
		log.Println(err)
		metrics.TestCount.WithLabelValues(ap.TableName(), "annotation", "decode-location-error").Inc()
		return err
	}

	// Estimate the row size based on the input JSON size.
	metrics.RowSizeHistogram.WithLabelValues(ap.TableName()).Observe(float64(len(test)))

	// Insert the row.
	ap.Base.Put(&row)

	// Count successful inserts.
	metrics.TestCount.WithLabelValues(ap.TableName(), "annotation", "ok").Inc()
	return nil
}

// NB: These functions are also required to complete the etl.Parser interface.
// For Annotation, we just forward the calls to the Inserter.

func (ap *AnnotationParser) Flush() error {
	return ap.Base.Flush()
}

func (ap *AnnotationParser) TableName() string {
	return ap.table
}

func (ap *AnnotationParser) FullTableName() string {
	return ap.table + ap.suffix
}

// RowsInBuffer returns the count of rows currently in the buffer.
func (ap *AnnotationParser) RowsInBuffer() int {
	return ap.GetStats().Pending
}

// Committed returns the count of rows successfully committed to BQ.
func (ap *AnnotationParser) Committed() int {
	return ap.GetStats().Committed
}

// Accepted returns the count of all rows received through InsertRow(s)
func (ap *AnnotationParser) Accepted() int {
	return ap.GetStats().Total()
}

// Failed returns the count of all rows that could not be committed.
func (ap *AnnotationParser) Failed() int {
	return ap.GetStats().Failed
}
