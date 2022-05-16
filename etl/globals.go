package etl

import (
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net"
	"regexp"
	"strings"
)

// TODO: Eliminate these global variables using config or env struct.
var (
	// IsBatch indicates this process is a batch processing service.
	IsBatch bool

	// OmitDeltas indicates we should NOT process all snapshots.
	OmitDeltas bool

	// GCloudProject contains the current operating environment.
	GCloudProject string

	// BigqueryProject overrides GCloudProject for BQ output.
	BigqueryProject string

	// BigqueryDataset overrides the default BQ dataset for output.
	BigqueryDataset string

	// BatchAnnotatorURL provides the base URL for batch annotation requests.
	BatchAnnotatorURL string
)

var (
	// GitCommit and Version hold the git commit id and git tags of this build.
	// It is recommended that the strings be set as part of the build/link
	// process, using commands like:
	//
	//   version="-X github.com/m-lab/etl/metrics.Version=$(git describe --tags)"
	//   commit="-X github.com/m-lab/etl/metrics.GitCommit=$(git log -1 --format=%H)"
	//   go build -ldflags "$version $commit" ./cmd/gardener
	GitCommit = "nocommit"
	Version   = "noversion"
)

// We currently have two filename patterns:
// Legacy: gs://archive-mlab-sandbox/ndt/2018/03/29/20180329T000001Z-mlab1-acc02-ndt-0000.tgz

// K8S: gs://pusher-mlab-staging/ndt/tcpinfo/2019/05/25/20190525T020001.697396Z-tcpinfo-mlab4-ord01-ndt.tgz
//  In this case we have:
//    start with bucket/exp/type/YYYY/MM/DD/YYYYMMDDTHHMMSS.MMMMMMZ-type-mlabN-pod0K-exp.tgz

// YYYYMMDD is a regexp string for identifying dense dates.
const YYYYMMDD = `\d{4}[01]\d[0123]\d`

// MlabDomain is the DNS domain for all mlab servers.
const MlabDomain = `measurement-lab.org`

// BucketPattern is used to extract gsutil bucket name.
const BucketPattern = `gs://([^/]*)/`

// ExpTypePattern is used to extract the experiment or experiment/type part of the path.
const ExpTypePattern = `(?:([a-z-]+)/)?([a-z0-9-]+)/` // experiment OR experiment/type.

// DatePathPattern is used to extract the date directory part of the path, e.g. 2017/01/02
const DatePathPattern = `(\d{4}/[01]\d/[0123]\d)/`

const dateTime = `(\d{4}[01]\d[0123]\d)T(\d{6}(\.\d{0,6})?)Z`

const type2 = `(?:-([a-z0-9-]+))?` // optional datatype string
const mlabNSiteNN = `-(mlab\d|third)-([a-z]{3}\d[0-9t]|party)-`

// This parses the experiment name, optional -NNNN sequence number, and optional -e (for old embargoed files)
const expNNNNE = `([a-z-]+)(?:-(\d{4}))?(-e)?`
const suffix = `(\.tar|\.tar.gz|\.tgz)$`

// These are here to facilitate use across queue-pusher and parsing components.
var (
	basicTaskPattern = regexp.MustCompile(
		`(?P<preamble>.*)` + dateTime + `(?P<postamble>.*)`)

	startPattern = regexp.MustCompile(`^` + BucketPattern + ExpTypePattern + DatePathPattern + `$`)
	endPattern   = regexp.MustCompile(`^` +
		type2 + // 1
		mlabNSiteNN + // 2,3
		expNNNNE + // 4,5,6
		suffix + `$`) // 7

	dateTimePattern = regexp.MustCompile(dateTime)
	sitePattern     = regexp.MustCompile(type2 + mlabNSiteNN)

	justSitePattern = regexp.MustCompile(`.*` + mlabNSiteNN + `.*`)
)

// DataPath breaks out the components of a task filename.
type DataPath struct {
	URI  string // The full URI
	Path string // The path portion of the complete URI (without scheme or bucket).

	// These fields are from the bucket and path
	Bucket   string // the GCS bucket name.
	ExpDir   string // the experiment directory.
	DataType string //
	DatePath string // the YYYY/MM/DD date path.
	// The rest are from the filename
	PackedDate string // the YYYYMMDD date.
	PackedTime string // the HHMMSS time.
	DataType2  string // new platform also embeds the data type in the filename
	Host       string // the short server name, e.g. mlab1.
	Site       string // the pod/site name, e.g. ams02.
	Experiment string // the experiment name, e.g. ndt, typically identical to ExpDir
	FileNumber string // the file number, e.g. 0001
	Embargo    string // optional
	Suffix     string // the archive suffix, e.g. .tgz
}

// ValidateTestPath validates a task filename.
func ValidateTestPath(uri string) (DataPath, error) {
	basic := basicTaskPattern.FindStringSubmatch(uri)
	if basic == nil {
		return DataPath{}, errors.New("Path missing date-time string")
	}
	preamble := startPattern.FindStringSubmatch(basic[1])
	if preamble == nil {
		return DataPath{}, errors.New("Invalid preamble: " + fmt.Sprint(basic))
	}

	post := endPattern.FindStringSubmatch(basic[5])
	if post == nil {
		return DataPath{}, errors.New("Invalid postamble: " + basic[5])
	}
	dp := DataPath{
		URI:        uri,
		Path:       path(uri),
		Bucket:     preamble[1],
		ExpDir:     preamble[2],
		DataType:   preamble[3],
		DatePath:   preamble[4],
		PackedDate: basic[2],
		PackedTime: basic[3],
		DataType2:  post[1],
		Host:       post[2],
		Site:       post[3],
		Experiment: post[4],
		FileNumber: post[5],
		Embargo:    post[6],
		Suffix:     post[7],
	}

	dataType := dp.GetDataType()
	if dataType == INVALID {
		log.Printf("Invalid filename: %s\n", uri)
		return DataPath{}, ErrBadDataType
	}

	return dp, nil
}

// GetDataType finds the type of data stored in a file from its complete filename
func (dp DataPath) GetDataType() DataType {
	dt, ok := dirToDataType[dp.DataType]
	if !ok {
		return INVALID
	}
	return dt
}

// TableBase returns the base bigquery table name associated with the DataPath data type.
func (dp DataPath) TableBase() string {
	return dp.GetDataType().Table()
}

// path returns the portion of the GCS path for a valid M-Lab GCS archive URI.
// Because ValidateTestPath() verifies other aspects of DataPath.URI, path()
// returns the empty string if it is malformed.
func path(uri string) string {
	parts := strings.SplitN(uri, "/", 4)
	if len(parts) != 4 || parts[0] != "gs:" || len(parts[3]) == 0 {
		return ""
	}
	return parts[3]
}

// IsBatchService return true if this is a batch service.
func IsBatchService() bool {
	return IsBatch
}

// GetIATACode extracts iata code like "acc" from file name like
// 20170501T000000Z-mlab1-acc02-paris-traceroute-0000.tgz
func GetIATACode(rawFilename string) string {
	parts := justSitePattern.FindStringSubmatch(rawFilename)
	if len(parts) != 3 {
		log.Println("Unable to extract IATA code from", rawFilename)
		return ""
	}
	if len(parts[2]) < 3 {
		return parts[2]
	}
	return parts[2][0:3]
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

// These constants enumerate the different data types.
// TODO - use camelcase.
const (
	ANNOTATION      = DataType("annotation")
	HOPANNOTATION1  = DataType("hopannotation1")
	NDT             = DataType("ndt")
	NDT5            = DataType("ndt5")
	NDT7            = DataType("ndt7")
	NDT_OMIT_DELTAS = DataType("ndt_nodelta") // to support larger buffer size.
	SS              = DataType("sidestream")
	PCAP            = DataType("pcap")
	PT              = DataType("traceroute")
	SCAMPER1        = DataType("scamper1")
	SW              = DataType("switch")
	TCPINFO         = DataType("tcpinfo")
	INVALID         = DataType("invalid")
)

var (
	// DirToDataType maps from gs:// subdirectory to data type.
	// TODO - this should be loaded from a config.
	dirToDataType = map[string]DataType{
		"annotation":       ANNOTATION,
		"hopannotation1":   HOPANNOTATION1,
		"ndt":              NDT,
		"ndt5":             NDT5,
		"ndt7":             NDT7,
		"sidestream":       SS,
		"paris-traceroute": PT,
		"pcap":             PCAP,
		"scamper1":         SCAMPER1,
		"switch":           SW,
		"tcpinfo":          TCPINFO,
		"traceroute":       PT,
	}

	// DataTypeToTable maps from data type to BigQuery table name.
	// TODO - this should be loaded from a config.
	dataTypeToTable = map[DataType]string{
		ANNOTATION:     "annotation",
		HOPANNOTATION1: "hopannotation1",
		NDT:            "ndt",
		SS:             "sidestream",
		PCAP:           "pcap",
		PT:             "traceroute",
		SCAMPER1:       "scamper1",
		SW:             "switch",
		TCPINFO:        "tcpinfo",
		NDT5:           "ndt5",
		NDT7:           "ndt7",
		INVALID:        "invalid",
	}

	// Map from data type to number of buffer size for BQ insertion.
	// This matters more for the legacy parsing that used BQ streaming inserts.
	// For the JSONL output in Gardener 2.0 operation, the buffer size doesn’t matter much,
	// as everything is written to gcs files, and the gcs library does it’s own buffering.
	// TODO - this should be loaded from a config
	dataTypeToBQBufferSize = map[DataType]int{
		ANNOTATION:      400, // around 1k each.
		HOPANNOTATION1:  200,
		NDT:             10,
		NDT_OMIT_DELTAS: 50,
		TCPINFO:         5,
		SS:              500, // Average json size is 2.5K
		PCAP:            200,
		PT:              20,
		SCAMPER1:        200,
		SW:              100,
		NDT5:            200,
		NDT7:            200,
		INVALID:         0,
	}
	// There is also a mapping of data types to queue names in
	// queue_pusher.go

	// Map from data type to number of files to skip when processing said type.
	// It allows us process fewer archives when there is a very high volume of data.
	// TODO - this should be loaded from a config.
	dataTypeToSkipCount = map[DataType]int{}
)

/*******************************************************************************
*  TODO: These methods to compute the appropriate project and dataset are ugly.
*  In not to distant future we need a better solution.
*  See https://github.com/m-lab/etl/issues/519
********************************************************************************/

// DirToTablename translates gs dir to BQ tablename.
func DirToTablename(dir string) string {
	return dataTypeToTable[dirToDataType[dir]]
}

// SkipCount returns the number of files to skip when processing each DataType.
func (dt DataType) SkipCount() int {
	return dataTypeToSkipCount[dt]
}

// BigqueryProject returns the appropriate project.
func (dt DataType) BigqueryProject() string {
	project := BigqueryProject
	if project != "" {
		return project
	}
	return GCloudProject
}

// Dataset returns the appropriate dataset to use.
// This is a bit of a hack, but works for our current needs.
func (dt DataType) Dataset() string {
	dataset := BigqueryDataset
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
