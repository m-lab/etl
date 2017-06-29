// The schema package provides an interface for the flexible map-based full
// schema for web100 tests.
package schema

// TODO(prod) Improve unit test coverage.
import (
	"log"

	"cloud.google.com/go/bigquery"
)

// TODO(prod): Create a struct that satisfies the web100.Saver interface?

// Web100ValueMap implements the web100.Saver interface for recording web100 values.
type Web100ValueMap map[string]bigquery.Value

// Returns the contained map, or nil if it doesn't exist.
func (vm Web100ValueMap) Get(name string) Web100ValueMap {
	wl, ok := vm[name]
	if !ok {
		return nil
	}
	return wl.(Web100ValueMap)
}

// Get the string at a path in the nested map.  Return value, true if found,
// or nil, false if not found.
func (vm Web100ValueMap) GetString(path []string) (string, bool) {
	if len(path) <= 1 {
		val, ok := vm[path[0]]
		if ok {
			return val.(string), ok
		} else {
			return "", ok
		}
	} else {
		next := vm.Get(path[0])
		if next != nil {
			return next.GetString(path[1:])
		} else {
			return "", false
		}
	}
}

// Get the int64 at a path in the nested map.  Return value, true if found,
// or 0, false if not found.
func (vm Web100ValueMap) GetInt64(path []string) (int64, bool) {
	if len(path) <= 1 {
		val, ok := vm[path[0]]
		if ok {
			return val.(int64), ok
		} else {
			return 0, ok
		}
	} else {
		next := vm.Get(path[0])
		if next != nil {
			return next.GetInt64(path[1:])
		} else {
			return 0, false
		}
	}
}

// Get the int64 at a path in the nested map.  Return value or nil.
func (vm Web100ValueMap) GetMap(path []string) Web100ValueMap {
	if len(path) == 0 {
		return vm
	}
	next := vm.Get(path[0])
	if next == nil {
		return next
	}
	return next.GetMap(path[1:])
}

// SetInt64 saves an int64 in a field with the given name.
func (s Web100ValueMap) SetInt64(name string, value int64) {
	s[name] = value
}

// SetString saves a string in a field with the given name.
func (s Web100ValueMap) SetString(name string, value string) {
	s[name] = value
}

// SetBool saves a boolean in a field with the given name.
func (s Web100ValueMap) SetBool(name string, value bool) {
	s[name] = value
}

// if overwrite is false, will only add missing values.
// if overwrite is true, will overwrite existing values.
func (r Web100ValueMap) SubstituteString(overwrite bool, target []string, source []string) {
	m := r.GetMap(target[:len(target)-1])
	if m == nil {
		// Error ?
		log.Printf("No such path: %v\n", target)
		return
	}
	if _, notNull := m[target[len(target)-1]]; notNull && !overwrite {
		// All good
		return
	}
	value, ok := r.GetString(source)
	if !ok {
		log.Printf("Source not available: %v\n", source)
		return
	}
	m[target[len(target)-1]] = value
}

// if overwrite is false, will only add missing values.
// if overwrite is true, will overwrite existing values.
func (r Web100ValueMap) SubstituteInt64(overwrite bool, target []string, source []string) {
	m := r.GetMap(target[:len(target)-1])
	if m == nil {
		// Error ?
		log.Printf("No such path: %v\n", target)
		return
	}
	if _, notNull := m[target[len(target)-1]]; notNull && !overwrite {
		// All good
		return
	}
	value, ok := r.GetInt64(source)
	if !ok {
		log.Printf("Source not available: %v\n", source)
		return
	}
	m[target[len(target)-1]] = value
}

// NewWeb100FullRecord creates a web100 value map with all supported fields.
// This is suitable when creating a schema definition for a new bigquery table.
func NewWeb100FullRecord(version string, logTime int64, connSpec, snapValues map[string]bigquery.Value) Web100ValueMap {
	return Web100ValueMap{
		"test_id":  "",
		"log_time": 0,
		// Can this be part of the metadata service?
		"connection_spec": FullConnectionSpec(),
		"anomalies":       Web100ValueMap{},
		"web100_log_entry": map[string]bigquery.Value{
			"version":         version,
			"log_time":        logTime,
			"connection_spec": connSpec,
			"snap":            snapValues,
		},
	}
}

func EmptySnap10() Web100ValueMap {
	return make(Web100ValueMap, 10)
}

func EmptySnap() Web100ValueMap {
	return make(Web100ValueMap, 120)
}

// NewWeb100Skeleton creates the tree structure, with no leaf fields.
func NewWeb100Skeleton() Web100ValueMap {
	return Web100ValueMap{
		"connection_spec": EmptyConnectionSpec(),
		"web100_log_entry": Web100ValueMap{
			"connection_spec": make(Web100ValueMap, 4),
			"snap":            EmptySnap(),
		},
	}
}

func FullConnectionSpec() Web100ValueMap {
	return Web100ValueMap{
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
		"tls":                   false,
		"websocket":             false,
		"client_geolocation":    FullGeolocation(),
		"server_geolocation":    FullGeolocation(),
	}
}

func EmptyConnectionSpec() Web100ValueMap {
	return Web100ValueMap{
		"client_geolocation": EmptyGeolocation(),
		"server_geolocation": EmptyGeolocation(),
	}
}

func FullGeolocation() Web100ValueMap {
	return Web100ValueMap{
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

func EmptyGeolocation() Web100ValueMap {
	return make(Web100ValueMap, 12)
}

// NewWeb100MinimalRecord creates a web100 value map with only the given fields.
// All undefined fields will be set to null after a BQ insert.
func NewWeb100MinimalRecord(version string, logTime int64, connSpec, snapValues Web100ValueMap, deltas []Web100ValueMap) Web100ValueMap {
	return Web100ValueMap{
		"anomalies": Web100ValueMap{},
		"web100_log_entry": Web100ValueMap{
			"version":         version,
			"log_time":        logTime,
			"connection_spec": connSpec, // TODO - deprecate connection_spec here.
			"snap":            snapValues,
			"deltas":          deltas,
		},
	}
}
