package schema

import (
	"time"

	"github.com/m-lab/tcp-info/inetdiag"
)

type anomalies struct {
	NoMeta         bool
	SnaplogError   bool
	NumSnaps       int64 // uint32 ?
	BlacklistFlags int64
}

type fullGeolocation struct {
	ContinentCode string  `json:"continent_code,string" bigquery:"continent_code"`
	CountryCode   string  `json:"country_code,string" bigquery:"country_code"`
	CountryCode3  string  `json:"country_code3,string" bigquery:"country_code3"`
	CountryName   string  `json:"country_name,string" bigquery:"country_name"`
	Region        string  `json:"region,string" bigquery:"region"`
	MetroCode     int64   `json:"metro_code,int64" bigquery:"metro_code"`
	City          string  `json:"city,string" bigquery:"city"`
	AreaCode      int64   `json:"area_code,int64" bigquery:"metro_code"`
	PostalCode    int64   `json:"postal_code,int64" bigquery:"postal_code"`
	Latitude      float64 `json:"latitude,float64" bigquery:"latitude"`
	Longitude     float64 `json:"longitude,float64" bigquery:"longitude"`
}

type fullConnectionSpec struct {
	ServerIP            string `json:"server_ip,string" bigquery:"server_ip"`
	ServerAF            uint16 `json:"server_af,int64" bigquery:"server_af"`
	ServerHostname      string `json:"server_hostname,string" bigquery:"server_hostname"`
	ServerKernelVersion string `json:"server_kernel_version,string" bigquery:"server_kernel_version"`
	ClientIP            string `json:"client_ip,string" bigquery:"client_ip"`
	ClientAF            uint16 `json:"client_af,int64" bigquery:"client_af"`
	ClientHostname      string `json:"client_hostname,string" bigquery:"client_hostname"`
	ClientOS            string `json:"client_os,string" bigquery:"client_os"`
	ClientKernelVersion string `json:"client_kernel_version,string" bigquery:"client_kernel_version"`
	ClientVersion       string `json:"client_version,string" bigquery:"client_version"`
	ClientBrowser       string `json:"client_browser,string" bigquery:"client_browser"`
	ClientApplication   string `json:"client_application,string" bigquery:"client_application"`
	DataDirection       uint16 `json:"data_direction,int64" bigquery:"data_direction"`
	TLS                 bool   `json:"tls,bool" bigquery:"tls"`
	Websockets          bool   `json:"websockets,bool" bigquery:"websockets"`
	ClientGeolocation   fullGeolocation
	ServerGeolocation   fullGeolocation
}

type NDTObsolete struct {
	TestID         string    `json:"server_ip,string" bigquery:"server_ip"`
	TaskFilename   string    `json:"task_filename,string" bigquery:"task_filename"`
	ParseTime      time.Time `json:"parse_time,string" bigquery:"parse_time"`
	LogTime        time.Time `json:"log_time,string" bigquery:"log_time"`
	BlacklistFlags int64     `json:"blacklist_flags,int64" bigquery:"blacklist_flags"`
	Anomalies      anomalies `json:"anomalies" bigquery:"anomalies"`
	ConnectionSpec fullConnectionSpec

	// --
	// 		"name": "client_geolocation",
	// 		"type": "RECORD"
	// --
	// 		"name": "server_geolocation",
	// 		"type": "RECORD"
	// --
	// 			"name": "network",
	// 			"type": "RECORD"
	// --
	// 		"name": "client",
	// 		"type": "RECORD"
	// --
	// 			"name": "network",
	// 			"type": "RECORD"
	// --
	// 		"name": "server",
	// 		"type": "RECORD"
	// --
	// 	"name": "connection_spec",
	// 	"type": "RECORD"
	// --
	// 		"name": "connection_spec",
	// 		"type": "RECORD"
	// --
	// 		"name": "snap",
	// 		"type": "RECORD"
	// --
	// 		"name": "deltas",
	// 		"type": "RECORD"
	// --
	// 	"name": "web100_log_entry",
	// 	"type": "RECORD"

}

// TCPRow describes a single BQ row of TCPInfo data.
type NDTRow struct {
	UUID     string    // Top level just because
	TestTime time.Time // Must be top level for partitioning

	ClientASN uint32 // Top level for clustering
	ServerASN uint32 // Top level for clustering

	ParseInfo *ParseInfoV0

	SockID inetdiag.SockID

	Server *ServerInfo
	Client *ClientInfo
}
