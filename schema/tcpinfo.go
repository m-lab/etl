package schema

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"

	"github.com/m-lab/go/cloud/bqx"
	"github.com/m-lab/tcp-info/inetdiag"
	"github.com/m-lab/tcp-info/snapshot"
)

// TCPInfoSummary includes a summary or derived fields from the raw record.
type TCPInfoSummary struct {
	SockID        inetdiag.SockID
	FinalSnapshot snapshot.Snapshot
}

// TCPInfoRow defines the BQ schema using 'Standard Columns' conventions for
// tcp-info measurements.
type TCPInfoRow struct {
	ID     string                  `json:"id" bigquery:"id"`
	A      *TCPInfoSummary         `json:"a" bigquery:"a"`
	Parser ParseInfo               `json:"parser" bigquery:"parser"`
	Date   civil.Date              `json:"date" bigquery:"date"`
	Raw    *snapshot.ConnectionLog `json:"raw" bigquery:"raw"`
}

// Schema returns the Bigquery schema for TCPInfoRow.
func (row *TCPInfoRow) Schema() (bigquery.Schema, error) {
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
