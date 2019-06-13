// This files contains schema for Paris TraceRoute tests.
package schema

import (
	"time"
)

// TODO: The following schema will be deprecated soon.

type GeoLocation struct {
	AreaCode      int32   `json:"area_code,int32"`
	City          string  `json:"city,string"`
	ContinentCode string  `json:"continent_code,string"`
	CountryCode   string  `json:"country_code,string"`
	CountryCode3  string  `json:"country_code3,string"`
	CountryName   string  `json:"country_code,string"`
	Latitude      float64 `json:"latitude,float64"`
	Longitude     float64 `json:"longitude,float64"`
	MetroCode     int64   `json:"metro_code,int64"`
	PostalCode    string  `json:"postal_code,string"`
	Region        string  `json:"region,string"`
	Radius        int64   `json:"radius,int32"`
}

type ConnectionIP struct {
	Ip       string      `json:"ip,string"`
	Hostname string      `json:"hostname,string"`
	Asn      int64       `json:"asn,int64"`
	Geo      GeoLocation `json:"geo"`
}

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

type PT struct {
	TaskFilename   string       `json:"task_filename,string" bigquery:"task_filename"`
	ParseTime      time.Time    `json:"parse_time" bigquery:"parse_time"`
	ParserVersion  string       `json:"parser_version,string" bigquery:"parser_version"`
	UUID           string       `json:"uuid,string" bigquery:"uuid"`
	StartTime      int64        `json:"start_time,int64" bigquery:"start_time"`
	StopTime       int64        `json:"stop_time,int64" bigquery:"stop_time"`
	ScamperVersion string       `json:"scamper_version,string" bigquery:"scamper_version"`
	Source         ConnectionIP `json:"source"`
	Destination    ConnectionIP `json:"destination"`
	ProbeSize      int64        `json:"probe_size,int64"`
	ProbeC         int64        `json:"probec,int64"`
	Hop            []ScamperHop `json:"hop"`
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
