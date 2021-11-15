package schema

import (
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/go/cloud/bqx"
)

type TcpStats struct {
	Packets               int64
	Truncated             int64
	ECE                   int64
	RetransmitPackets     int64
	RetransmitBytes       int64
	Sacks                 int64
	OptionCounts          []int64 // 16 counts, indicating how often each option type occurred.
	BadSacks              int64   // Number of sacks with bad boundaries
	BadDeltas             int64   // Number of seqs and acks that were more than 1<<30 off from previous value.
	MissingPackets        int64   // Observations of packet sequence numbers that didn't match previous payload length.
	SendNextExceededLimit int64   // Number of times SendNext() returned a value that exceeded the receiver window limit.
}

type AlphaFields struct {
	TruncatedPackets int64     `bigquery:"truncated_packets"`
	SynPacket        int64     `bigquery:"syn_packet" json:"syn_packet"`
	SynTime          time.Time `bigquery:"syn_time" json:"syn_time"`
	SynAckPacket     int64     `bigquery:"syn_ack_packet" json:"syn_ack_packet"`
	SynAckTime       time.Time `bigquery:"syn_ack_time" json:"syn_ack_time"`
	Packets          int64     `bigquery:"packets" json:"packets"`
	Sacks            int64     `bigquery:"sacks" json:"sacks"`
	TTLChanges       int64     `bigquery:"ttl_changes" json:"ttl_changes"`
	IPChanges        int64     `bigquery:"ip_changes" json:"ip_changes"`

	LeftStats  TcpStats
	RightStats TcpStats
}

// PCAPRow describes a single BQ row of pcap (packet capture) data.
type PCAPRow struct {
	ID     string     `bigquery:"id" json:"id"`
	Parser ParseInfo  `bigquery:"parser" json:"parser"`
	Date   civil.Date `bigquery:"date" json:"date"`

	Alpha *AlphaFields `bigquery:"alpha" json:"alpha"`

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
