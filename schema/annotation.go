package schema

import (
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/go/bqx"
	"github.com/m-lab/uuid-annotator/annotator"
)

// AnnotationRow defines the BQ schema using 'Standard Columns' conventions for
// the annotation datatype produced by the uuid-annotator.
type AnnotationRow struct {
	UUID      string                      // NOTE: there is no 'a' record for AnnotationRows.
	Server    annotator.ServerAnnotations `bigquery:"server"`
	Client    annotator.ClientAnnotations `bigquery:"client"`
	ParseInfo ParseInfo                   `bigquery:"parseInfo"`
	TestTime  time.Time                   `bigquery:"testTime" json:"Timestamp"`

	// NOTE: there is no 'Raw' field for annotation datatypes because the
	// uuid-annotator output schema was designed to be used directly by the parser.

	// NOT part of struct schema. Included only to provide a fake annotator interface.
	row.NullAnnotator `bigquery:"-"`
}

// Schema returns the BigQuery schema for NDT7ResultRow.
func (row *AnnotationRow) Schema() (bigquery.Schema, error) {
	sch, err := bigquery.InferSchema(row)
	if err != nil {
		return bigquery.Schema{}, err
	}
	docs := FindSchemaDocsFor(row)
	for _, doc := range docs {
		bqx.UpdateSchemaDescription(sch, doc)
	}
	rr := bqx.RemoveRequired(sch)
	return rr, err
}
