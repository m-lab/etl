// The schema package provides an interface for the flexible map-based full
// schema for web100 tests.
package schema

import (
	"cloud.google.com/go/bigquery"
)

// TODO(prod): Create a struct that satisfies the web100.Saver interface?

// Web100ValueMap implements the web100.Saver interface for recording web100 values.
type Web100ValueMap map[string]bigquery.Value

// SetInt64 saves an int64 in a field with the given name.
func (s Web100ValueMap) SetInt64(name string, value int64) {
	s[name] = value
}

// SetString saves a string in a field with the given name.
func (s Web100ValueMap) SetString(name string, value string) {
	s[name] = value
}

// NewWeb100FullRecord creates a web100 value map with all supported fields.
// This is suitable when creating a schema definition for a new bigquery table.
func NewWeb100FullRecord(version string, logTime int64, connSpec, snapValues map[string]bigquery.Value) Web100ValueMap {
	return Web100ValueMap{
		"test_id":  "",
		"log_time": 0,
		// Can this be part of the metadata service?
		"connection_spec": FullConnectionSpec(),
		"web100_log_entry": map[string]bigquery.Value{
			"version":         version,
			"log_time":        logTime,
			"connection_spec": connSpec,
			"snap":            snapValues,
		},
	}
}

// NewWeb100FullRecord creates a web100 value map with all supported fields.
// This is suitable when creating a schema definition for a new bigquery table.
func NewWeb100Skeleton() Web100ValueMap {
	return Web100ValueMap{
		"connection_spec": EmptyConnectionSpec(),
		"web100_log_entry": Web100ValueMap{
			"connection_spec": Web100ValueMap{},
		},
	}
}

func FullConnectionSpec() *Web100ValueMap {
	return &Web100ValueMap{
		"server_ip":             "",
		"server_af":             0,
		"server_hostname":       "",
		"server_kernel_version": "",
		"client_ip":             "",
		"client_af":             0,
		"client_hostname":       "",
		"client_os":             "",
		"client_kernel_version": "",
		"client_version":        "",
		"client_browser":        "",
		"client_application":    "",
		"data_direction":        0,
		"client_geolocation":    FullGeolocation(),
		"server_geolocation":    FullGeolocation(),
	}
}

func EmptyConnectionSpec() *Web100ValueMap {
	return &Web100ValueMap{
		"client_geolocation": EmptyGeolocation(),
		"server_geolocation": EmptyGeolocation(),
	}
}

func FullGeolocation() *Web100ValueMap {
	return &Web100ValueMap{
		"continent_code": "",
		"country_code":   "",
		"country_code3":  "",
		"country_name":   "",
		"region":         "",
		"metro_code":     0,
		"city":           "",
		"area_code":      0,
		"postal_code":    "",
		"latitude":       0.0,
		"longitude":      0.0,
	}
}

func EmptyGeolocation() *Web100ValueMap {
	return &Web100ValueMap{}
}

// NewWeb100MinimalRecord creates a web100 value map with only the given fields.
// All undefined fields will be set to null after a BQ insert.
func NewWeb100MinimalRecord(version string, logTime int64, connSpec, snapValues map[string]bigquery.Value) map[string]bigquery.Value {
	return map[string]bigquery.Value{
		"web100_log_entry": map[string]bigquery.Value{
			"version":         version,
			"log_time":        logTime,
			"connection_spec": connSpec,
			"snap":            snapValues,
		},
	}
}
