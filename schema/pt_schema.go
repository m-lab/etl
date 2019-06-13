// This files contains schema for Paris TraceRoute tests.
package schema

import (
	"time"

	"github.com/m-lab/annotation-service/api"
)

// TODO: The following schema will be deprecated soon.

type ParisTracerouteHop struct {
	Protocol         string            `json:"protocal,string"`
	Src_ip           string            `json:"src_ip,string"`
	Src_af           int32             `json:"src_af,int32"`
	Dest_ip          string            `json:"dest_ip,string"`
	Dest_af          int32             `json:"dest_af,int32"`
	Src_hostname     string            `json:"src_hostname,string"`
	Dest_hostname    string            `json:"dest_hostname,string"`
	Rtt              []float64         `json:"rtt,[]float64"`
	Src_geolocation  api.GeolocationIP `json:"src_geolocation"`
	Dest_geolocation api.GeolocationIP `json:"dest_geolocation"`
}

type MLabConnectionSpecification struct {
	Server_ip          string            `json:"server_ip,string"`
	Server_af          int32             `json:"server_af,int32"`
	Client_ip          string            `json:"client_ip,string"`
	Client_af          int32             `json:"client_af,int32"`
	Data_direction     int32             `json:"data_direction,int32"`
	Server_geolocation api.GeolocationIP `json:"server_geolocation"`
	Client_geolocation api.GeolocationIP `json:"client_geolocation"`
}

type PT struct {
	TestID               string                      `json:"test_id,string" bigquery:"test_id"`
	Project              int32                       `json:"project,int32" bigquery:"project"`
	TaskFilename         string                      `json:"task_filename,string" bigquery:"task_filename"`
	ParseTime            time.Time                   `json:"parse_time" bigquery:"parse_time"`
	ParserVersion        string                      `json:"parser_version,string" bigquery:"parser_version"`
	LogTime              int64                       `json:"log_time,int64" bigquery:"log_time"`
	Connection_spec      MLabConnectionSpecification `json:"connection_spec"`
	Paris_traceroute_hop ParisTracerouteHop          `json:"paris_traceroute_hop"`
	Type                 int32                       `json:"type,int32"`
}

// This will be new schema for traceroute tests.

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
	Parseinfo      ParseInfo    `json:"parseinfo"`
	UUID           string       `json:"uuid,string" bigquery:"uuid"`
	StartTime      int64        `json:"start_time,int64" bigquery:"start_time"`
	StopTime       int64        `json:"stop_time,int64" bigquery:"stop_time"`
	ScamperVersion string       `json:"scamper_version,string" bigquery:"scamper_version"`
	Source         ServerInfo   `json:"source"`
	Destination    ClientInfo   `json:"destination"`
	ProbeSize      int64        `json:"probe_size,int64"`
	ProbeC         int64        `json:"probec,int64"`
	Hop            []ScamperHop `json:"hop"`
}
