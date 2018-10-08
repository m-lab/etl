// This files contains schema for Paris TraceRoute tests.
package schema

import (
	"time"

	"github.com/m-lab/etl/annotation"
)

// TODO(dev): use mixed case Go variable names throughout

type ParisTracerouteHop struct {
	Protocol         string                   `json:"protocal,string"`
	Src_ip           string                   `json:"src_ip,string"`
	Src_af           int32                    `json:"src_af,int32"`
	Dest_ip          string                   `json:"dest_ip,string"`
	Dest_af          int32                    `json:"dest_af,int32"`
	Src_hostname     string                   `json:"src_hostname,string"`
	Dest_hostname    string                   `json:"dest_hostname,string"`
	Rtt              []float64                `json:"rtt,[]float64"`
	Src_geolocation  annotation.GeolocationIP `json:"src_geolocation"`
	Dest_geolocation annotation.GeolocationIP `json:"dest_geolocation"`
}

type MLabConnectionSpecification struct {
	Server_ip          string                   `json:"server_ip,string"`
	Server_af          int32                    `json:"server_af,int32"`
	Client_ip          string                   `json:"client_ip,string"`
	Client_af          int32                    `json:"client_af,int32"`
	Data_direction     int32                    `json:"data_direction,int32"`
	Server_geolocation annotation.GeolocationIP `json:"server_geolocation"`
	Client_geolocation annotation.GeolocationIP `json:"client_geolocation"`
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
