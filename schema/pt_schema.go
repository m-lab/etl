// This files contains schema for Paris TraceRoute tests.
package schema

import (
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/go/cloud/bqx"
	"github.com/m-lab/traceroute-caller/hopannotation"
	"github.com/m-lab/uuid-annotator/annotator"
)

type HopIP struct {
	IP             string                        `json:"ip" bigquery:"IP"`
	City           string                        `json:"city" bigquery:"City"`
	CountryCode    string                        `json:"country_code" bigquery:"CountryCode"`
	Hostname       string                        `json:"hostname" bigquery:"Hostname"`
	ASN            uint32                        `json:"asn,uint32" bigquery:"ASN"`
	HopAnnotation1 *hopannotation.HopAnnotation1 `json:"hopannotation1" bigquery:"HopAnnotation1"`
}

type HopProbe struct {
	Flowid int64     `json:"flowid,int64"`
	Rtt    []float64 `json:"rtt"`
}

type HopLink struct {
	HopDstIP string     `json:"hop_dst_ip"`
	TTL      int64      `json:"ttl,int64"`
	Probes   []HopProbe `json:"probes"`
}

type ScamperHop struct {
	Source HopIP     `json:"source"`
	Linkc  int64     `json:"linkc,int64"`
	Links  []HopLink `json:"link"`
}

type PTTest struct {
	UUID           string       `json:"uuid" bigquery:"uuid"`
	TestTime       time.Time    `json:"testtime"`
	Parseinfo      ParseInfoV0  `json:"parseinfo"`
	StartTime      int64        `json:"start_time,int64" bigquery:"start_time"`
	StopTime       int64        `json:"stop_time,int64" bigquery:"stop_time"`
	ScamperVersion string       `json:"scamper_version" bigquery:"scamper_version"`
	Source         ServerInfo   `json:"source"`
	Destination    ClientInfo   `json:"destination"`
	ProbeSize      int64        `json:"probe_size,int64"`
	ProbeC         int64        `json:"probec,int64"`
	Hop            []ScamperHop `json:"hop"`
	ExpVersion     string       `json:"exp_version" bigquery:"exp_version"`
	CachedResult   bool         `json:"cached_result,bool" bigquery:"cached_result"`

	// ServerX and ClientX are for the synthetic UUID annotator export process.
	ServerX annotator.ServerAnnotations
	ClientX annotator.ClientAnnotations
}

// Schema returns the Bigquery schema for PTTest.
func (row *PTTest) Schema() (bigquery.Schema, error) {
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
