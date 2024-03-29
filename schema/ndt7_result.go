package schema

import (
	"time"

	"cloud.google.com/go/bigquery"

	"cloud.google.com/go/civil"
	"github.com/m-lab/go/cloud/bqx"

	"github.com/m-lab/ndt-server/data"
)

// NDT7ResultRow defines the BQ schema using 'Standard Columns' conventions for
// the data.NDT7Result produced by the ndt-server for NDT7 client measurements.
type NDT7ResultRow struct {
	ID     string          `bigquery:"id"`
	A      NDT7Summary     `bigquery:"a"`
	Parser ParseInfo       `bigquery:"parser"`
	Date   civil.Date      `bigquery:"date"`
	Raw    data.NDT7Result `bigquery:"raw"`
}

// NDT7Summary contains fields summarizing or derived from the raw data.
type NDT7Summary struct {
	UUID               string
	TestTime           time.Time
	CongestionControl  string
	MeanThroughputMbps float64
	MinRTT             float64
	LossRate           float64
}

// Schema returns the BigQuery schema for NDT7ResultRow.
func (row *NDT7ResultRow) Schema() (bigquery.Schema, error) {
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
