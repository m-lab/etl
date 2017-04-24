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

type DataType int

const (
	InvalidData DataType = iota
	NDTData
	SSData
	PTData
	SWData
)

// Temporary - should come from a config.
var (
	TableNames = map[DataType]string{
		NDTData: "ndt_test",
		SSData:  "ss_test",
		PTData:  "pt_test",
		SWData:  "disco_test",
	}
)
