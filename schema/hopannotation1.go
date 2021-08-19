package schema

import (
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/go/cloud/bqx"
	"github.com/m-lab/uuid-annotator/annotator"
)

// Schema stub for HopAnnotation1.
// TODO(cristinaleon): Remove this definition when traceroute-caller is available.
type HopAnnotation1 struct {
	ID          string
	Timestamp   time.Time
	Annotations *annotator.ClientAnnotations
}

// HopAnnotation1Row describes a single BQ row of HopAnnotation1 data.
type HopAnnotation1Row struct {
	ID     string          `bigquery:"id"`
	Parser ParseInfo       `bigquery:"parser"`
	Date   civil.Date      `bigquery:"date"`
	Raw    *HopAnnotation1 `json:",omitempty" bigquery:"raw"`

	// NOT part of struct schema. Included only to provide a fake annotator interface.
	row.NullAnnotator `bigquery:"-"`
}

// Schema returns the Bigquery schema for HopAnnotation1.
func (row *HopAnnotation1Row) Schema() (bigquery.Schema, error) {
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
