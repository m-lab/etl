package schema

import (
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"

	"github.com/m-lab/go/cloud/bqx"
	"github.com/m-lab/tcp-info/inetdiag"
	"github.com/m-lab/tcp-info/snapshot"

	"github.com/m-lab/etl/row"
)

type TCPInfoSummary struct {
	SockID        inetdiag.SockID
	FinalSnapshot *snapshot.Snapshot
}

type TCPInfoRawRecord struct {
	StartTime time.Time
	Sequence  int
	Snapshots []*snapshot.Snapshot
}

type TCPInfoRow struct {
	ID     string            `json:"id" bigquery:"id"`
	A      *TCPInfoSummary   `json:"a" bigquery:"a"`
	Parser ParseInfo         `json:"parser" bigquery:"parser"`
	Date   civil.Date        `json:"date" bigquery:"date"`
	Raw    *TCPInfoRawRecord `json:"raw" bigquery:"raw"`

	// NOT part of struct schema. Included only to provide a fake annotator interface.
	row.NullAnnotator `bigquery:"-"`
}

// Schema returns the Bigquery schema for TCPRow.
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
