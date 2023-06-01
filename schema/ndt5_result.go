package schema

import (
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"

	"github.com/m-lab/go/cloud/bqx"
	"github.com/m-lab/ndt-server/data"
)

// NDT5ResultRowV2 defines the BQ schema for the data.NDT5Result produced by the
// ndt-server for NDT client measurements.
type NDT5ResultRowV2 struct {
	ID     string          `json:"id" bigquery:"id"`
	A      *NDT5Summary    `json:"a" bigquery:"a"`
	Parser ParseInfo       `json:"parser" bigquery:"parser"`
	Date   civil.Date      `json:"date" bigquery:"date"`
	Raw    data.NDT5Result `json:"raw" bigquery:"raw"`
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
func (row *NDT5ResultRowV2) Schema() (bigquery.Schema, error) {
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
