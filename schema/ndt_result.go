package schema

import (
	"cloud.google.com/go/bigquery"
	"github.com/m-lab/go/bqx"
	"github.com/m-lab/ndt-server/data"
)

// NDTResultRow defines the BQ schema for the data.NDTResult produced by the
// ndt-server for NDT client measurements.
type NDTResultRow struct {
	ParseInfo *ParseInfo
	TestID    string         `json:"test_id,string" bigquery:"test_id"`
	LogTime   int64          `json:"log_time,int64" bigquery:"log_time"`
	Result    data.NDTResult `json:"result" bigquery:"result"`
}

// Schema returns the BigQuery schema for NDTResultRow.
func (row *NDTResultRow) Schema() (bigquery.Schema, error) {
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
