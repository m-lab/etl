package etl

import (
	"regexp"
)

const date = `(\d{4}/[01]\d/[0123]\d)`

// These are here to facilitate use across queue-pusher and parsing components.
var (
	// This matches any valid test file name, and some invalid ones.
	// Field 2 contains the test type.
	//                                  1     2         3         4
	TaskPattern = regexp.MustCompile(`(.*)/([^/]*)/` + date + `/([^/]*).tgz`)
)

type DataType string

const (
	NDT     = DataType("ndt")
	SS      = DataType("sidestream")
	PT      = DataType("traceroute")
	SW      = DataType("disco")
	INVALID = DataType("invalid")
)

var (
	// Map from gs:// subdirectory to data type.
	// TODO - this should be loaded from a config.
	DirToDataType = map[string]DataType{
		"ndt":              NDT,
		"sidestream":       SS,
		"paris-traceroute": PT,
		"switch":           SW,
	}

	// Map from data type to BigQuery table name.
	// TODO - this should be loaded from a config.
	DataTypeToTable = map[DataType]string{
		NDT:     "ndt_test_full_schema",
		SS:      "ss_test",
		PT:      "pt_test",
		SW:      "disco_test",
		INVALID: "invalid",
	}

	// There is also a mapping of data types to queue names in
	// queue_pusher.go
)

// Find the type of data stored in a file from its comlete filename
func GetDataType(fn string) DataType {
	fields := TaskPattern.FindStringSubmatch(fn)
	if fields == nil {
		return INVALID
	}
	dt, ok := DirToDataType[fields[2]]
	if !ok {
		return INVALID
	}
	return dt
}
