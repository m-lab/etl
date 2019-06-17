// This files contains schema for Paris TraceRoute tests.
package schema

import (
	"cloud.google.com/go/bigquery"
	"github.com/m-lab/go/bqx"
)

type HopIP struct {
	Ip          string `json:"ip,string"`
	City        string `json:"city,string"`
	CountryCode string `json:"country_code,string"`
	Hostname    string `json:"hostname,string"`
}

type HopProbe struct {
	Flowid int64     `json:"flowid,int64"`
	Rtt    []float64 `json:"rtt"`
}

type HopLink struct {
	HopDstIp string     `json:"hop_dst_ip,string"`
	TTL      int64      `json:"ttl,int64"`
	Probes   []HopProbe `json:"probes"`
}

type ScamperHop struct {
	Source HopIP     `json:"source"`
	Linkc  int64     `json:"linkc,int64"`
	Links  []HopLink `json:"link"`
}

type PTTest struct {
	UUID           string       `json:"uuid,string" bigquery:"uuid"`
	Parseinfo      ParseInfo    `json:"parseinfo"`
	StartTime      int64        `json:"start_time,int64" bigquery:"start_time"`
	StopTime       int64        `json:"stop_time,int64" bigquery:"stop_time"`
	ScamperVersion string       `json:"scamper_version,string" bigquery:"scamper_version"`
	Source         ServerInfo   `json:"source"`
	Destination    ClientInfo   `json:"destination"`
	ProbeSize      int64        `json:"probe_size,int64"`
	ProbeC         int64        `json:"probec,int64"`
	Hop            []ScamperHop `json:"hop"`
}

// Schema returns the Bigquery schema for PTTest.
func (row *PTTest) Schema() (bigquery.Schema, error) {
	sch, err := bigquery.InferSchema(row)
	if err != nil {
		return bigquery.Schema{}, err
	}
	rr := bqx.RemoveRequired(sch)
	return rr, nil
}
