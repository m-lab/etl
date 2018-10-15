package etl

import (
	"encoding/base64"
	"errors"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// IsBatch indicates this process is a batch processing service.
var IsBatch bool

// OmitDeltas indicates we should NOT process all snapshots.
var OmitDeltas bool

func init() {
	IsBatch, _ = strconv.ParseBool(os.Getenv("BATCH_SERVICE"))
	OmitDeltas, _ = strconv.ParseBool(os.Getenv("NDT_OMIT_DELTAS"))
}

// YYYYMMDD is a regexp string for identifying dense dates.
const YYYYMMDD = `\d{4}[01]\d[0123]\d`

// MlabDomain is the DNS domain for all mlab servers.
const MlabDomain = `measurement-lab.org`

const start = `^gs://(?P<bucket>.*)/(?P<exp>[^/]*)/`
const datePath = `(?P<datepath>\d{4}/[01]\d/[0123]\d)/`
const dateTime = `(?P<packeddate>\d{4}[01]\d[0123]\d)T(?P<packedtime>\d{6})Z`
const mlabN_podNN = `-(?P<host>mlab\d)-(?P<pod>[[:alpha:]]{3}\d[0-9t])-`
const exp_NNNN = `(?P<experiment>.*)-(?P<filenumber>\d{4})(?P<etag>-e)?`
const suffix = `(?P<suffix>\.tar|\.tar.gz|\.tgz)$`

// These are here to facilitate use across queue-pusher and parsing components.
var (
	// This matches any valid test file name, and some invalid ones.
	TaskPattern = regexp.MustCompile(start + // #1 #2
		datePath + // #3 - YYYY/MM/DD
		dateTime + // #4 - YYYYMMDDTHHMMSSZ
		mlabN_podNN + // #5 #6 - e.g. -mlab1-lax04-
		exp_NNNN + // #7 #8 e.g. ndt-0001
		suffix) // #9 typically .tgz

	startPattern = regexp.MustCompile(start)
	endPattern   = regexp.MustCompile(suffix)
	podPattern   = regexp.MustCompile(mlabN_podNN)
)

// DataPath breaks out the components of a task filename.
type DataPath struct {
	// TODO(dev) Delete unused fields.
	// They are comprehensive now in anticipation of using them to populate
	// new fields in the BQ tables.
	Bucket     string // #1 -- the GCS bucket name.
	Exp1       string // #2 -- the experiment directory.
	DatePath   string // #3 -- the YYYY/MM/DD date path.
	PackedDate string // #4 -- the YYYYMMDD date.
	PackedTime string // #5 -- the HHMMSS time.
	Host       string // #6 -- the short server name, e.g. mlab1.
	Pod        string // #7 -- the pod/site name, e.g. ams02.
	Experiment string // #8 -- the experiment name, e.g. ndt
	FileNumber string // #9 -- the file number, e.g. 0001
	Suffix     string // #10 -- the archive suffix, e.g. .tgz
}

// ValidateTestPath validates a task filename.
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
	if len(fields) < 11 {
		return nil, errors.New("Path does not include all fields: " + path)
	}
	dp := &DataPath{
		Bucket:     fields[1],
		Exp1:       fields[2],
		DatePath:   fields[3],
		PackedDate: fields[4],
		PackedTime: fields[5],
		Host:       fields[6],
		Pod:        fields[7],
		Experiment: fields[8],
		FileNumber: fields[9],
		Suffix:     fields[11],
	}
	return dp, nil
}

// GetDataType finds the type of data stored in a file from its complete filename
func (fn *DataPath) GetDataType() DataType {
	dt, ok := dirToDataType[fn.Exp1]
	if !ok {
		return INVALID
	}
	return dt
}

// TableBase returns the base bigquery table name associated with the DataPath data type.
func (fn *DataPath) TableBase() string {
	return fn.GetDataType().Table()
}

// IsBatchService return true if this is a batch service.
func IsBatchService() bool {
	return IsBatch
}

// GetMetroName extracts metro name like "acc" from file name like
// 20170501T000000Z-mlab1-acc02-paris-traceroute-0000.tgz
func GetMetroName(rawFilename string) string {
	podName := podPattern.FindString(rawFilename)
	if podName != "" {
		return podName[7:10]
	}
	return ""
}

// GetIntFromIPv4 converts an IPv4 address to equivalent uint32.
func GetIntFromIPv4(p4 net.IP) uint {
	return uint(p4[0])<<24 + uint(p4[1])<<16 + uint(p4[2])<<8 + uint(p4[3])
}

// GetIntFromIPv6Upper converts the upper 64 bits of an IPv6 address into uint64.
func GetIntFromIPv6Upper(p6 net.IP) uint64 {
	return uint64(p6[0])<<56 + uint64(p6[1])<<48 + uint64(p6[2])<<40 + uint64(p6[3])<<32 + uint64(p6[4])<<24 + uint64(p6[5])<<16 + uint64(p6[6])<<8 + uint64(p6[7])
}

// NumberBitsDifferent computes how many trailing bits differ between two IP addresses.
// The second returned number is 4 for IP_v4, 6 for IP_v6, and 0 for invalid input.
func NumberBitsDifferent(first string, second string) (int, int) {
	ip1 := net.ParseIP(first)
	ip2 := net.ParseIP(second)
	if ip1.To4() != nil && ip2.To4() != nil {
		dist := uint(GetIntFromIPv4(ip1.To4()) ^ uint(GetIntFromIPv4(ip2.To4())))
		n := 0
		for ; dist != 0; dist >>= 1 {
			n++
		}
		return n, 4
	}
	if ip1.To16() != nil && ip2.To16() != nil {
		// We will only compare the upper 64 bits.
		dist := uint64(GetIntFromIPv6Upper(ip1.To16())) ^ uint64(GetIntFromIPv6Upper(ip2.To16()))
		n := 0
		for ; dist != 0; dist >>= 1 {
			n++
		}
		return n, 6
	}
	return -1, 0
}

//=====================================================================

// DataType identifies the type of data handled by a parser.
type DataType string

// BQBufferSize returns the appropriate BQ insert buffer size.
func (dt DataType) BQBufferSize() int {
	// Special case for NDT when omitting deltas.
	if dt == NDT {
		if OmitDeltas {
			return dataTypeToBQBufferSize[NDT_OMIT_DELTAS]
		}
	}
	return dataTypeToBQBufferSize[dt]
}

const (
	NDT             = DataType("ndt")
	NDT_OMIT_DELTAS = DataType("ndt_nodelta") // to support larger buffer size.
	SS              = DataType("sidestream")
	PT              = DataType("traceroute")
	SW              = DataType("switch")
	INVALID         = DataType("invalid")
)

var (
	// DirToDataType maps from gs:// subdirectory to data type.
	// TODO - this should be loaded from a config.
	dirToDataType = map[string]DataType{
		"ndt":              NDT,
		"sidestream":       SS,
		"paris-traceroute": PT,
		"switch":           SW,
	}

	// DataTypeToTable maps from data type to BigQuery table name.
	// TODO - this should be loaded from a config.
	dataTypeToTable = map[DataType]string{
		NDT:     "ndt",
		SS:      "sidestream",
		PT:      "traceroute",
		SW:      "switch",
		INVALID: "invalid",
	}

	// Map from data type to number of buffer size for BQ insertion.
	// TODO - this should be loaded from a config.
	dataTypeToBQBufferSize = map[DataType]int{
		NDT:             10,
		NDT_OMIT_DELTAS: 50,
		SS:              500, // Average json size is 2.5K
		PT:              300,
		SW:              100,
		INVALID:         0,
	}
	// There is also a mapping of data types to queue names in
	// queue_pusher.go
)

/*******************************************************************************
*  TODO: These methods to compute the appropriate project and dataset are ugly.
*  In not to distant future we need a better solution.
*  See https://github.com/m-lab/etl/issues/519
********************************************************************************/
// Translate gs dir to BQ tablename.
func DirToTablename(dir string) string {
	return dataTypeToTable[dirToDataType[dir]]
}

// BigqueryProject returns the appropriate project.
func (dt DataType) BigqueryProject() string {
	project := os.Getenv("BIGQUERY_PROJECT")
	if project != "" {
		return project
	}
	return os.Getenv("GCLOUD_PROJECT")
}

// Dataset returns the appropriate dataset to use.
// This is a bit of a hack, but works for our current needs.
func (dt DataType) Dataset() string {
	dataset := os.Getenv("BIGQUERY_DATASET")
	if dataset != "" {
		return dataset
	}
	if IsBatchService() {
		return "batch"
	}

	return "base_tables"
}

// Table returns the appropriate table to use.
func (dt DataType) Table() string {
	return dataTypeToTable[dt]
}

// GetFilename converts request received from the queue into a filename.
// TODO(dev) Add unit test
func GetFilename(filename string) (string, error) {
	if strings.HasPrefix(filename, "gs://") {
		return filename, nil
	}

	decode, err := base64.StdEncoding.DecodeString(filename)
	if err != nil {
		return "", errors.New("invalid file path: " + filename)
	}
	fn := string(decode[:])
	if strings.HasPrefix(fn, "gs://") {
		return fn, nil
	}

	return "", errors.New("invalid base64 encoded file path: " + fn)
}
