package schema

import (
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"

	"github.com/m-lab/go/cloud/bqx"
	"github.com/m-lab/ndt-server/data"
	"github.com/m-lab/uuid-annotator/annotator"

	"github.com/m-lab/etl/row"
)

// NDT5ResultRow defines the BQ schema for the data.NDT5Result produced by the
// ndt-server for NDT client measurements.
// Deprecated - use V1 for Gardener 2.0
type NDT5ResultRow struct {
	ParseInfo *ParseInfoV0
	TestID    string          `json:"test_id,string" bigquery:"test_id"`
	LogTime   int64           `json:"log_time,int64" bigquery:"log_time"`
	Result    data.NDT5Result `json:"result" bigquery:"result"`

	// NOT part of struct schema. Included only to provide a fake annotator interface.
	row.NullAnnotator `bigquery:"-"`
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

// NDT5ResultRowV1 defines the BQ schema for the data.NDT5Result produced by the
// ndt-server for NDT client measurements.
type NDT5ResultRowV1 struct {
	ID     string          `bigquery:"id"`
	A      NDT5Summary     `bigquery:"a"`
	Parser ParseInfo       `bigquery:"parser"`
	Date   civil.Date      `bigquery:"date"`
	Raw    data.NDT5Result `bigquery:"raw"`

	// These will be populated by the join with the annotator data.
	Server annotator.ServerAnnotations `bigquery:"server" json:"server"`
	Client annotator.ClientAnnotations `bigquery:"client" json:"client"`

	// NOT part of struct schema. Included only to provide a fake annotator interface.
	row.NullAnnotator `bigquery:"-"`
}

// NDT5Summary contains fields summarizing or derived from the raw data.
// This should be consolidated with NDT7Summary, and also used for web100.
type NDT5Summary struct {
	UUID               string
	TestTime           time.Time
	CongestionControl  string
	MeanThroughputMbps float64
	MinRTT             float64
	LossRate           float64
}

// Schema returns the BigQuery schema for NDT5ResultRow.
func (row *NDT5ResultRowV1) Schema() (bigquery.Schema, error) {
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
