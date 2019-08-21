package schema

import (
	"cloud.google.com/go/bigquery"
	"github.com/m-lab/go/bqx"
	"github.com/m-lab/ndt-server/data"
)

// NameValue is a BigQuery-compatible type for ClientMetadata "name"/"value" pairs.
type NameValue struct {
	Name  string
	Value string
}

// NDTResult defines the BQ schema for the NDT Result produced by the
// ndt-server for the NDT clients.
type NDTResult struct {
	ParseInfo *ParseInfo
	TestID    string         `json:"test_id,string" bigquery:"test_id"`
	LogTime   int64          `json:"log_time,int64" bigquery:"log_time"`
	Result    data.NDTResult `json:"result" bigquery:"result"`
}

// A local struct used to infer the schema we wish to append to the
// result.Control, result.Upload, and result.Download fields.
type clientMetadata struct {
	ClientMetadata []NameValue
}

// Schema returns the BigQuery schema for NDTResult.
func (row *NDTResult) Schema() (bigquery.Schema, error) {
	sch, err := bigquery.InferSchema(row)
	if err != nil {
		return bigquery.Schema{}, err
	}
	rr := bqx.RemoveRequired(sch)
	var md clientMetadata
	mdSch, err := bigquery.InferSchema(&md)
	if err != nil {
		return bigquery.Schema{}, err
	}
	mdSch = bqx.RemoveRequired(mdSch)
	c := bqx.CustomizeAppend(rr, map[string]*bigquery.FieldSchema{
		"Control":  mdSch[0],
		"Upload":   mdSch[0],
		"Download": mdSch[0],
	})
	return c, err
}
