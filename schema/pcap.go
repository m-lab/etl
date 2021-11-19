package schema

import (
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/go/cloud/bqx"
)

type TcpStats struct {
	Packets   int64
	Truncated int64

	OptionCounts []int64 // 16 counts, indicating how often each option type occurred.

	RetransmitPackets int64
	RetransmitBytes   int64

	Sacks int64

	ECECount      int64
	WindowChanges int64

	// Errors and anomalies
	BadSacks              int64 // Number of sacks with bad boundaries
	BadDeltas             int64 // Number of seqs and acks that were more than 1<<30 off from previous value.
	MissingPackets        int64 // Observations of packet sequence numbers that didn't match previous payload length.
	SendNextExceededLimit int64 // Number of times SendNext() returned a value that exceeded the receiver window limit.
	TTLChanges            int64 // Observed number of TTL values that don't match first IP header.
	SrcPortErrors         int64 // Observed number of source ports that don't match first IP header.
	DstPortErrors         int64 // Observed number of dest ports that don't match tcp.DstPort
	OtherErrors           int64 // Number of other errors that occurred.

	Delay        float64 // Delay in seconds between TSVal and TSecr.
	Jitter       float64 // Jitter in seconds between TSVal and packet capture time.
	TickInterval float64 // Interval between TSVal ticks in seconds.
}

type AlphaFields struct {
	TruncatedPackets int64     `bigquery:"truncated_packets"`
	SynPacket        int64     `bigquery:"syn_packet" json:"syn_packet"`
	SynTime          time.Time `bigquery:"syn_time" json:"syn_time"`
	SynAckPacket     int64     `bigquery:"syn_ack_packet" json:"syn_ack_packet"`
	SynAckTime       time.Time `bigquery:"syn_ack_time" json:"syn_ack_time"`
	Packets          int64     `bigquery:"packets" json:"packets"`
	Sacks            int64     `bigquery:"sacks" json:"sacks"`
	IPAddrErrors     int64     `bigquery:"ip_addr_errors" json:"ip_addr_errors"` // Number of packets with IP addresses that don't match first IP header at all.
	WithoutTCPLayer  int64     `bigquery:"no_tcp_layer" json:"no_tcp_layer"`     // Number of packets with no TCP layer.

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
