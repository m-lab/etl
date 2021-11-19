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
	TruncatedPackets int64
	SynPacket        int64
	SynTime          time.Time
	SynAckPacket     int64
	SynAckTime       time.Time
	Packets          int64
	Sacks            int64
	IPAddrErrors     int64 // Number of packets with IP addresses that don't match first IP header at all.
	WithoutTCPLayer  int64 // Number of packets with no TCP layer.

	LeftStats  TcpStats
	RightStats TcpStats
}

// PCAPRow describes a single BQ row of pcap (packet capture) data.
type PCAPRow struct {
	ID     string     `bigquery:"id" json:"id"`
	Parser ParseInfo  `bigquery:"parser" json:"parser"`
	Date   civil.Date `bigquery:"date" json:"date"`

	Exp *AlphaFields `bigquery:"exp_a" json:"alpha"`

	// NOT part of struct schema. Included only to provide a fake annotator interface.
	row.NullAnnotator `bigquery:"-" json:"-"`
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
