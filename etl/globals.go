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
		NDT:     "ndt_test",
		SS:      "ss_test",
		PT:      "pt_test",
		SW:      "disco_test",
		INVALID: "invalid",
	}
)
