package schema

import (
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/go/cloud/bqx"
)

// PCAPSummary is used for the 'a' field, and contains summary data
// describing high level characteristics of the connection.
type PCAPSummary struct {
	StartTime       time.Time
	EndTime         time.Time
	PacketsSent     int // requires IP header parsing
	PacketsReceived int // requires IP header parsing
}

// PCAPRaw is used for the raw field, and contains basic information
// derived trivially from the PCAP data.
type PCAPRaw struct {
	ClientIP   string
	ClientPort int
	ServerIP   string
	ServerPort int
}

// PCAPExperimental is used for the exp field, and contains proposed
// summary data that is not yet solid enough to put into the 'a' field.
type PCAPExperimental struct {
	// Can't have an empty record, but we will use this later.
	// MinRTT float64
}

// PCAPRow describes a single BQ row of pcap (packet capture) data.
type PCAPRow struct {
	ID     string     `bigquery:"id" json:"id"`
	Parser ParseInfo  `bigquery:"parser" json:"parser"`
	Date   civil.Date `bigquery:"date" json:"date"`

	// For now, these are omitempty, but in future they should be unconditional.
	A   PCAPSummary `bigquery:"a" json:"a,omitempty"`
	Raw PCAPRaw     `bigquery:"raw" json:"raw,omitempty"`
	//	Exp PCAPExperimental `bigquery:"exp" json:"exp,omitempty"`

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
