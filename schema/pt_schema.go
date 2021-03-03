// This files contains schema for Paris TraceRoute tests.
package schema

import (
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/go/cloud/bqx"

	"github.com/m-lab/etl/metrics"
)

type HopIP struct {
	IP          string `json:"ip"`
	City        string `json:"city"`
	CountryCode string `json:"country_code"`
	Hostname    string `json:"hostname"`
	ASN         uint32 `json:"asn,uint32"`
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

// Implement parser.Annotatable

// GetLogTime returns the timestamp that should be used for annotation.
func (row *PTTest) GetLogTime() time.Time {
	return row.TestTime
}

// GetClientIPs returns the client (remote) IP for annotation.  See parser.Annotatable
func (row *PTTest) GetClientIPs() []string {
	requestIPs := make(map[string]bool, len(row.Hop)+1)
	requestIPs[row.Destination.IP] = true
	for _, hop := range row.Hop {
		requestIPs[hop.Source.IP] = true
	}
	batchRequest := make([]string, 0, len(requestIPs))
	for key, _ := range requestIPs {
		batchRequest = append(batchRequest, key)
	}
	return batchRequest
}

// GetServerIP returns the server (local) IP for annotation.  See parser.Annotatable
func (row *PTTest) GetServerIP() string {
	return row.Source.IP
}

func (row *PTTest) AnnotateHops(annMap map[string]*api.Annotations) error {
	for index, _ := range row.Hop {
		ann, ok := annMap[row.Hop[index].Source.IP]
		if !ok {
			metrics.AnnotationMissingCount.WithLabelValues("No annotation for PT hop").Inc()
			continue
		}
		if ann.Geo == nil {
			metrics.AnnotationMissingCount.WithLabelValues("Empty PT Geo").Inc()
		} else {
			row.Hop[index].Source.City = ann.Geo.City
			row.Hop[index].Source.CountryCode = ann.Geo.CountryCode
		}
		if ann.Network == nil {
			metrics.AnnotationMissingCount.WithLabelValues("Empty PT ASN").Inc()
		} else {
			asn, err := ann.Network.BestASN()
			if err != nil {
				metrics.AnnotationMissingCount.WithLabelValues("PT Hop ASN failed").Inc()
			}
			row.Hop[index].Source.ASN = uint32(asn)
		}
	}
	return nil
}

// AnnotateClients adds the client annotations. See parser.Annotatable
// annMap must not be null
func (row *PTTest) AnnotateClients(annMap map[string]*api.Annotations) error {
	ip := row.Destination.IP

	ann, ok := annMap[ip]
	if !ok {
		metrics.AnnotationMissingCount.WithLabelValues("No annotation for PT client IP").Inc()
		return nil
	}
	if ann.Geo == nil {
		metrics.AnnotationMissingCount.WithLabelValues("Empty ann.Geo").Inc()
	} else {
		row.Destination.Geo = ann.Geo
	}

	if ann.Network == nil {
		metrics.AnnotationMissingCount.WithLabelValues("Empty ann.Network for PT client IP").Inc()
		return nil
	}
	row.Destination.Network = ann.Network

	row.AnnotateHops(annMap)
	return nil
}

// AnnotateServer adds the server annotations. See parser.Annotatable
// local must not be nil
func (row *PTTest) AnnotateServer(local *api.Annotations) error {
	row.Source.Geo = local.Geo
	if local.Network == nil {
		return nil
	}
	row.Source.Network = local.Network
	return nil
}
