package schema

import (
	"cloud.google.com/go/bigquery"
)

// Map performs a type conversion on the given value.
func Map(v bigquery.Value) map[string]bigquery.Value {
	switch v.(type) {
	case map[string]bigquery.Value:
		return v.(map[string]bigquery.Value)
	default:
		return nil
	}
}

func NewRecord() map[string]bigquery.Value {
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
			"version":  "",
			"log_time": 0,
			"connection_spec": map[string]bigquery.Value{
				"remote_ip":   "",
				"remote_port": 0,
				"local_ip":    "",
				"local_af":    0,
				"local_port":  0,
			},
			"snap": map[string]bigquery.Value{},
		},
		// TODO(dev): add paris_traceroute_hop records here or separately?
	}
}
