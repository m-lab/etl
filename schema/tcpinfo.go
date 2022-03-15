package schema

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"

	"github.com/m-lab/go/cloud/bqx"
	"github.com/m-lab/tcp-info/inetdiag"
	"github.com/m-lab/tcp-info/netlink"
	"github.com/m-lab/tcp-info/snapshot"

	"github.com/m-lab/etl/row"
)

// TCPInfoSummary includes a summary or derived fields from the raw record.
type TCPInfoSummary struct {
	SockID        inetdiag.SockID
	FinalSnapshot *snapshot.Snapshot
}

// TCPInfoRawRecord contains raw data from the tcp-info format.
type TCPInfoRawRecord struct {
	Metadata  netlink.Metadata
	Snapshots []*snapshot.Snapshot
}

// TCPInfoRow defines the BQ schema using 'Standard Columns' conventions for
// tcp-info measurements.
type TCPInfoRow struct {
	ID     string            `json:"id" bigquery:"id"`
	A      *TCPInfoSummary   `json:"a" bigquery:"a"`
	Parser ParseInfo         `json:"parser" bigquery:"parser"`
	Date   civil.Date        `json:"date" bigquery:"date"`
	Raw    *TCPInfoRawRecord `json:"raw" bigquery:"raw"`

	// NOT part of struct schema. Included only to provide a fake annotator interface.
	row.NullAnnotator `bigquery:"-"`
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
