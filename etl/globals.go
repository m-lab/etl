package etl

import (
	"errors"
	"regexp"
)

const start = `^gs://(?P<prefix>.*)/(?P<exp>[^/]*)/`
const datePath = `(?P<datepath>\d{4}/[01]\d/[0123]\d)/`
const dateTime = `(\d{4}[01]\d[0123]\d)T000000Z`
const mlabN_podNN = `-(mlab\d)-([[:alpha:]]{3}\d[0-9t])-`
const exp_NNNN = `(.*)-(\d{4})`
const suffix = `(?:\.tar|\.tar.gz|\.tgz)$`

// These are here to facilitate use across queue-pusher and parsing components.
var (
	// This matches any valid test file name, and some invalid ones.
	TaskPattern = regexp.MustCompile(start + // #1 #2
		datePath + // #3 - YYYY/MM/DD
		dateTime + // #4 - YYYYMMDDT000000Z
		mlabN_podNN + // #5 #6 - e.g. -mlab1-lax04-
		exp_NNNN + // #7 #8 e.g. ndt-0001
		suffix) // #9 typically .tgz

	startPattern = regexp.MustCompile(start)
	endPattern   = regexp.MustCompile(suffix)
	podPattern   = regexp.MustCompile(mlabN_podNN)
)

type DataPath struct {
	// TODO(dev) Delete unused fields.
	// They are comprehensive now in anticipation of using them to populate
	// new fields in the BQ tables.
	Exp1       string // #2
	DatePath   string // #3
	PackedDate string // #4
	Host       string // #5
	Pod        string // #6
	Experiment string // #7
	FileNumber string // #8
}

func ValidateTestPath(path string) (*DataPath, error) {
	fields := TaskPattern.FindStringSubmatch(path)

	if fields == nil {
		if !startPattern.MatchString(path) {
			return nil, errors.New("Path should begin with gs://.../.../: " + path)
		}
		if !endPattern.MatchString(path) {
			return nil, errors.New("Path should end in .tar, .tgz, or .tar.gz: " + path)
		}
		if !podPattern.MatchString(path) {
			return nil, errors.New("Path should contain -mlabN-podNN: " + path)
		}
		return nil, errors.New("Invalid test path: " + path)
	}
	return &DataPath{
			fields[2], fields[3], fields[4], fields[5],
			fields[6], fields[7], fields[8]},
		nil
}

// Find the type of data stored in a file from its complete filename
func (fn *DataPath) GetDataType() DataType {
	dt, ok := DirToDataType[fn.Exp1]
	if !ok {
		return INVALID
	}
	return dt
}

//=====================================================================

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
		NDT:     "ndt",
		SS:      "ss_test",
		PT:      "pt_test",
		SW:      "disco_test",
		INVALID: "invalid",
	}

	// There is also a mapping of data types to queue names in
	// queue_pusher.go
)
