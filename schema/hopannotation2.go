package schema

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/m-lab/go/cloud/bqx"
	"github.com/m-lab/traceroute-caller/hopannotation"
)

// HopAnnotation2Row describes a single BQ row of HopAnnotation2 data.
type HopAnnotation2Row struct {
	ID     string     `bigquery:"id"`
	Parser ParseInfo  `bigquery:"parser"`
	Date   civil.Date `bigquery:"date"`
	// TODO(soltesz): update traceroute-caller type.
	Raw *hopannotation.HopAnnotation1 `json:",omitempty" bigquery:"raw"`
}

// Schema returns the Bigquery schema for HopAnnotation2.
func (row *HopAnnotation2Row) Schema() (bigquery.Schema, error) {
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
