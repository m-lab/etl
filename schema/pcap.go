package schema

import (
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/go/cloud/bqx"
)

type AlphaFields struct {
	SynAckIntervalNsec int64     `bigquery:"syn_ack_interval_nsec"`
	SynPacket          int64     `bigquery:"syn_packet" json:"syn_packet"`
	SynTime            time.Time `bigquery:"syn_time" json:"syn_time"`
	SynAckPacket       int64     `bigquery:"syn_ack_packet" json:"syn_ack_packet"`
	SynAckTime         time.Time `bigquery:"syn_ack_time" json:"syn_ack_time"`
	Packets            int64     `bigquery:"packets" json:"packets"`
	OptionCounts       []int64   `bigquery:"option_counts" json:"option_counts"`
	FirstECECount      uint64    `bigquery:"first_ece_count" json:"first_ece_count"`
	SecondECECount     uint64    `bigquery:"second_ece_count" json:"second_ece_count"`
	FirstRetransmits   uint64    `bigquery:"first_retransmits" json:"first_retransmits"`
	SecondRetransmits  uint64    `bigquery:"second_retransmits" json:"second_retransmits"`
	Sacks              int64     `bigquery:"sacks" json:"sacks"`
	TotalSrcSeq        int64     `bigquery:"total_src_seq" json:"total_src_seq"`
	TotalDstSeq        int64     `bigquery:"total_dst_seq" json:"total_dst_seq"`
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
