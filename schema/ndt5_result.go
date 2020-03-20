package schema

import (
	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/go/bqx"
	"github.com/m-lab/ndt-server/data"
)

// NDT5ResultRow defines the BQ schema for the data.NDT5Result produced by the
// ndt-server for NDT client measurements.
type NDT5ResultRow struct {
	ParseInfo *ParseInfoV0
	TestID    string          `json:"test_id,string" bigquery:"test_id"`
	LogTime   int64           `json:"log_time,int64" bigquery:"log_time"`
	Result    data.NDT5Result `json:"result" bigquery:"result"`

	// NOT part of struct schema. Included only to provide a fake annotator interface.
	*row.NullAnnotator `bigquery:"-"`
}

// Schema returns the BigQuery schema for NDT5ResultRow.
func (row *NDT5ResultRow) Schema() (bigquery.Schema, error) {
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
