package schema

import (
	"cloud.google.com/go/bigquery"
	"github.com/m-lab/go/bqx"

	"github.com/m-lab/ndt-server/data"
)

// NDT5Result defines the BQ schema for the NDT Result produced by the
// ndt-server for the NDT5 clients.
type NDT5Result struct {
	ParseInfo *ParseInfo
	TestID    string         `json:"test_id,string" bigquery:"test_id"`
	LogTime   int64          `json:"log_time,int64" bigquery:"log_time"`
	Result    data.NDTResult `json:"result" bigquery:"result"`
}

// Schema returns the BigQuery schema for NDTResult.
func (row *NDT5Result) Schema() (bigquery.Schema, error) {
	sch, err := bigquery.InferSchema(row)
	if err != nil {
		return bigquery.Schema{}, err
	}
	rr := bqx.RemoveRequired(sch)
	return rr, nil
}
