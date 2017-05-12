// This files contains schema for Paris TraceRoute tests.
package schema

import (
	"cloud.google.com/go/bigquery"
)

// TODO(prod): Create a struct that satisfies the web100.Saver interface?

// Web100ValueMap implements the web100.Saver interface for recording web100 values.
type PTValueMap map[string]bigquery.Value

type ParisTracerouteHop struct {
	Protocol     string
	Src_ip       string
	Src_af       int32
	Dest_ip      string
	Dest_af      int32
	Src_hostname string
	Des_hostname string
	Rtt          []float64
}

func (i *ParisTracerouteHop) Save() map[string]bigquery.Value {
	return map[string]bigquery.Value{
		"protocol":     i.Protocol,
		"src_ip":       i.Src_ip,
		"src_af":       i.Src_af,
		"dest_ip":      i.Dest_ip,
		"dest_af":      i.Dest_af,
		"src_hostname": i.Src_hostname,
		"des_hostname": i.Des_hostname,
		"rtt":          i.Rtt,
	}
}

type GeolocationIP struct {
	continent_code string
	country_code   string
	country_code3  string
	country_name   string
	region         string
	metro_code     int64
	city           string
	area_code      int64
	postal_code    string
	latitude       float64
	longitude      float64
}

type MLabConnectionSpecification struct {
	Server_ip          string
	Server_af          int32
	Client_ip          string
	Client_af          int32
	Data_direction     int32 // 0 for SERVER_TO_CLIENT
	Client_geolocation GeolocationIP
	Server_geolocation GeolocationIP
}

func (i *MLabConnectionSpecification) Save() map[string]bigquery.Value {
	return map[string]bigquery.Value{
		"server_ip":      i.Server_ip,
		"server_af":      i.Server_af,
		"client_ip":      i.Client_ip,
		"client_af":      i.Client_af,
		"data_direction": i.Data_direction,
	}
}

// MLabSnapshot in legacy code
type PT struct {
	test_id              string
	project              int32 // 3 for PARIS_TRACEROUTE
	log_time             int64
	connection_spec      MLabConnectionSpecification
	paris_traceroute_hop ParisTracerouteHop
}

// Save implements the ValueSaver interface.
func (i *PT) Save() map[string]bigquery.Value {
	return map[string]bigquery.Value{
		"test_id":              i.test_id,
		"project":              i.project,
		"log_time":             i.log_time,
		"type":                 int32(2),
		"connection_spec":      i.connection_spec,
		"paris_traceroute_hop": i.paris_traceroute_hop,
	}
}

// NewPTFullRecord creates a value map with all supported fields.
// This is suitable when creating a schema definition for a new bigquery table.
func NewPTFullRecord(test_id string, logTime int64, connect_spec, hop map[string]bigquery.Value) map[string]bigquery.Value {
	return map[string]bigquery.Value{
		"test_id":              test_id,
		"project":              int32(3),
		"log_time":             logTime,
		"connection_spec":      connect_spec,
		"type":                 int32(2),
		"paris_traceroute_hop": hop,
	}
}
