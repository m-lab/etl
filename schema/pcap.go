package schema

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/google/gopacket/layers"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/go/cloud/bqx"
)

type Packet struct {
	Seq     uint32
	Ack     uint32
	Options []layers.TCPOption
}

type AlphaFields struct {
	Packets      int64   `bigquery:"packets" json:"packets"`
	OptionCounts []int64 `bigquery:"option_counts" json:"option_counts"`
	Sacks        int64   `bigquery:"sacks" json:"sacks"`
}

// PCAPRow describes a single BQ row of pcap (packet capture) data.
type PCAPRow struct {
	ID     string     `bigquery:"id" json:"id"`
	Parser ParseInfo  `bigquery:"parser" json:"parser"`
	Date   civil.Date `bigquery:"date" json:"date"`

	Alpha AlphaFields `bigquery:"alpha" json:"alpha"`

	// NOT part of struct schema. Included only to provide a fake annotator interface.
	row.NullAnnotator `bigquery:"-"`
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
