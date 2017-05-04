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
func NewWeb100FullRecord(version string, logTime int64, connSpec, snapValues map[string]bigquery.Value) map[string]bigquery.Value {
	return map[string]bigquery.Value{
		"test_id":  "",
		"log_time": 0,
		// TODO(prod): parse the *.meta files for this data?
		// Can this be part of the metadata service?
		"connection_spec": map[string]bigquery.Value{
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
			// TODO(prod): add geolocation sub-records.
			//  client_geolocation: record
			//  server_geolocation: record
		},
		"web100_log_entry": map[string]bigquery.Value{
			"version":         version,
			"log_time":        logTime,
			"connection_spec": connSpec,
			"snap":            snapValues,
		},
		// TODO(dev): add paris_traceroute_hop records here or separately?
	}
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
