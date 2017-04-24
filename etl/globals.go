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
	TypeInvalid DataType = iota
	TypeNDT
	TypeSS
	TypePT
	TypeSW
)

var (
	TableNames = map[DataType]string{
		TypeNDT: "ndt_test",
		TypeSS:  "ss_test",
		TypePT:  "pt_test",
		TypeSW:  "disco_test",
	}
)
