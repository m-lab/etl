package schema

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/m-lab/go/cloud/bqx"
)

// PCAPRow describes a single BQ row of pcap (packet capture) data.
type PCAPRow struct {
	ID     string     `bigquery:"id"`
	Parser ParseInfo  `bigquery:"parser"`
	Date   civil.Date `bigquery:"date"`
}

// Schema returns the Bigquery schema for Pcap.
func (row *PCAPRow) Schema() (bigquery.Schema, error) {
	sch, err := bigquery.InferSchema(row)
	if err != nil {
		return bigquery.Schema{}, err
	}
	docs := FindSchemaDocsFor(row)
	for _, doc := range docs {
		bqx.UpdateSchemaDescription(sch, doc)
	}
	rr := bqx.RemoveRequired(sch)
	return rr, nil
}
