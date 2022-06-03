package schema

// The below BQ.* structs are the BQ-supported versions of the
// corresponding traceroute-caller structs.
// The reasons using mixed structs from TRC and BQ in the schema are:
// 1. To add HopIDs the Tracelb.Nodes field.
// 2. To replace the Tracelb.Nodes.Links nested array field from TRC,
// since nested arrays are not supported in BQ.

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/m-lab/go/cloud/bqx"
	"github.com/m-lab/traceroute-caller/parser"
	"github.com/m-lab/traceroute-caller/tracer"
)

// BQScamperLinkArray defines an array of ScamperLinks.
// BigQuery does not support arrays of arrays, so this struct breaks down
// traceroute-caller's ScamperNode.Links ([][]SamperLink) field into two.
type BQScamperLinkArray struct {
	Links []parser.ScamperLink
}

// BQScamperNode describes a layer of links.
type BQScamperNode struct {
	HopID string               `json:"hop_id" bigquery:"hop_id"`
	Addr  string               `bigquery:"addr"`
	Name  string               `bigquery:"name"`
	QTTL  int                  `json:"q_ttl" bigquery:"q_ttl"`
	Linkc int64                `bigquery:"linkc"`
	Links []BQScamperLinkArray `bigquery:"links"`
}

// BQTracelbLine contains the actual scamper trace details.
type BQTracelbLine struct {
	Type        string          `bigquery:"type"`
	Version     string          `bigquery:"version"`
	Userid      float64         `bigquery:"userid"`
	Method      string          `bigquery:"method"`
	Src         string          `bigquery:"src"`
	Dst         string          `bigquery:"dst"`
	Start       parser.TS       `bigquery:"start"`
	ProbeSize   float64         `json:"probe_size" bigquery:"probe_size"`
	Firsthop    float64         `bigquery:"firsthop"`
	Attempts    float64         `bigquery:"attempts"`
	Confidence  float64         `bigquery:"confidence"`
	Tos         float64         `bigquery:"tos"`
	Gaplimit    float64         `bigquery:"gaplimit"`
	WaitTimeout float64         `json:"wait_timeout" bigquery:"wait_timeout"`
	WaitProbe   float64         `json:"wait_probe" bigquery:"wait_probe"`
	Probec      float64         `bigquery:"probec"`
	ProbecMax   float64         `json:"probec_max" bigquery:"probec_max"`
	Nodec       float64         `bigquery:"nodec"`
	Linkc       float64         `bigquery:"linkc"`
	Nodes       []BQScamperNode `bigquery:"nodes"`
}

// BQScamperOutput encapsulates the four lines of a traceroute:
//   {"UUID":...}
//   {"type":"cycle-start"...}
//   {"type":"tracelb"...}
//   {"type":"cycle-stop"...}
type BQScamperOutput struct {
	Metadata   tracer.Metadata
	CycleStart parser.CyclestartLine
	Tracelb    BQTracelbLine
	CycleStop  parser.CyclestopLine
}

// Scamper1Row defines the BQ schema using 'Standard Columns' conventions for
// the scamper1 datatype produced by traceroute-caller.
type Scamper1Row struct {
	ID     string          `bigquery:"id"`
	Parser ParseInfo       `bigquery:"parser"`
	Date   civil.Date      `bigquery:"date"`
	Raw    BQScamperOutput `bigquery:"raw"`
}

// Schema returns the BigQuery schema for Scamper1Row.
func (row *Scamper1Row) Schema() (bigquery.Schema, error) {
	sch, err := bigquery.InferSchema(row)
	if err != nil {
		return bigquery.Schema{}, err
	}
	docs := FindSchemaDocsFor(row)
	for _, doc := range docs {
		bqx.UpdateSchemaDescription(sch, doc)
	}
	rr := bqx.RemoveRequired(sch)
	return rr, err
}
