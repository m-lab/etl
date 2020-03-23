package schema

import (
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/go/bqx"
	"github.com/m-lab/ndt-server/data"
)

// NDT7ResultRow defines the BQ schema using 'Standard Columns' conventions for
// the data.NDT7Result produced by the ndt-server for NDT7 client measurements.
type NDT7ResultRow struct {
	A         NDT7Summary     `bigquery:"a"`
	ParseInfo ParseInfo       `bigquery:"parseInfo"`
	TestTime  time.Time       `bigquery:"testTime"`
	Flags     int64           `bigquery:"flags"`
	Raw       data.NDT7Result `bigquery:"raw"`

	// NOT part of struct schema. Included only to provide a fake annotator interface.
	row.NullAnnotator `bigquery:"-"`
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
