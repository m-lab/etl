// Package parser defines the Parser interface and implementations for the different
// data types.
package parser

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"net"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/annotation-service/api/v2"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/etl/web100"
)

func init() {
	initParserVersion()
	initParserGitCommit()
}

const uninitialized = "uninitialized"

var (
	gParserVersion   = uninitialized
	gParserGitCommit = uninitialized
)

// initParserVersion initializes the gParserVersion variable for use by all parsers.
func initParserVersion() string {
	release := etl.Version
	if release != "noversion" {
		gParserVersion = "https://github.com/m-lab/etl/tree/" + release
	} else {
		gParserVersion = "local development"
	}
	return gParserVersion
}

// initParserGitCommit initializes the gParserGitCommit variable for use by all parsers.
func initParserGitCommit() string {
	hash := etl.GitCommit
	if hash != "nocommit" {
		gParserGitCommit = hash
	}
	return gParserGitCommit
}

// Version returns the parser version used by parsers to annotate data rows.
func Version() string {
	return gParserVersion
}

// GitCommit returns the git commit hash of the build.
func GitCommit() string {
	return gParserGitCommit
}

// NormalizeIP accepts an IPv4 or IPv6 address and returns a normalized version
// of that string. This should be used to fix malformed IPv6 addresses in web100
// datasets (e.g. 2001:::abcd:2) as well as IPv4-mapped IPv6 addresses (e.g. ::ffff:1.2.3.4).
func NormalizeIP(ip string) string {
	r, err := web100.FixIPv6(ip)
	if err != nil {
		return ip
	}
	n := net.ParseIP(r)
	if n == nil {
		return r
	}
	return n.String()
}

// GetHopID creates a unique identifier to join Hop Annotations
// with traceroute datasets.
// The same logic exists in traceroute-caller.
// https://github.com/m-lab/traceroute-caller/blob/773bb092b18589d2fee20418ed1fa9aa6c5850cc/triggertrace/triggertrace.go#L147
// https://github.com/m-lab/traceroute-caller/blob/773bb092b18589d2fee20418ed1fa9aa6c5850cc/hopannotation/hopannotation.go#L235
// https://github.com/m-lab/traceroute-caller/blob/773bb092b18589d2fee20418ed1fa9aa6c5850cc/hopannotation/hopannotation.go#L237
func GetHopID(cycleStartTime float64, hostname string, address string) string {
	traceStartTime := time.Unix(int64(cycleStartTime), 0).UTC()
	date := traceStartTime.Format("20060102")
	return fmt.Sprintf("%s_%s_%s", date, hostname, address)
}

// NewSinkParser creates an appropriate parser for a given data type.
// Eventually all datatypes will use this instead of NewParser.
func NewSinkParser(dt etl.DataType, sink row.Sink, table string, ann api.Annotator) etl.Parser {
	switch dt {
	case etl.ANNOTATION:
		return NewAnnotationParser(sink, table, "", ann)
	case etl.HOPANNOTATION1:
		return NewHopAnnotation1Parser(sink, table, "", ann)
	case etl.NDT5:
		return NewNDT5ResultParser(sink, table, "", ann)
	case etl.NDT7:
		return NewNDT7ResultParser(sink, table, "", ann)
	case etl.TCPINFO:
		return NewTCPInfoParser(sink, table, "", ann)
	case etl.PCAP:
		return NewPCAPParser(sink, table, "", ann)
	case etl.SCAMPER1:
		return NewScamper1Parser(sink, table, "", ann)
	case etl.SW:
		return NewSwitchParser(sink, table, "", ann)
	default:
		return nil
	}
}

// NewParser creates an appropriate parser for a given data type.
// DEPRECATED - parsers should migrate to use NewSinkParser.
func NewParser(dt etl.DataType, ins etl.Inserter) etl.Parser {
	switch dt {
	case etl.NDT:
		return NewNDTParser(ins)
	case etl.SS:
		return NewDefaultSSParser(ins) // TODO fix this hack.
	case etl.PT:
		return NewPTParser(ins)
	default:
		return nil
	}
}

//=====================================================================================
//                       Parser implementations
//=====================================================================================

// FakeRowStats provides trivial implementation of RowStats interface.
type FakeRowStats struct {
}

func (s *FakeRowStats) RowsInBuffer() int {
	return 0
}
func (s *FakeRowStats) Accepted() int {
	return 0
}
func (s *FakeRowStats) Committed() int {
	return 0
}
func (s *FakeRowStats) Failed() int {
	return 0
}

type NullParser struct {
	FakeRowStats
}

func (np *NullParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error {
	metrics.TestTotal.WithLabelValues("table", "null", "ok").Inc()
	return nil
}
func (np *NullParser) TableName() string {
	return "null-table"
}
func (np *NullParser) TaskError() error {
	return nil
}

// base64hash produces an opaque, filename-safe string. The resulting string
// should only be used to provide unique identifiers.
func base64hash(input string) string {
	b := md5.Sum([]byte(input))
	return base64.RawURLEncoding.EncodeToString(b[:])
}

// ptSyntheticUUID constructs a synthetic UUID for the .paris format for
// traceroute rows using the same fields used by gardener's dedup, which
// guarantees uniqueness only for joining with annotations.
func ptSyntheticUUID(t time.Time, srcIP, dstIP string) string {
	return base64hash(t.Format(time.RFC3339) + "-" + srcIP + "-" + dstIP)
}

// ndtWeb100SyntheticUUID constructs a synthetic UUID for ndt web100 rows using
// the test filename.  The filename is the same field used by gardener's dedup,
// which guarantees uniqueness only for joining with annotations.
func ndtWeb100SyntheticUUID(fn string) string {
	return base64hash(fn)
}

// ssSyntheticUUID constructs a synthetic UUID for sidestream rows using the
// same fields used by gardener's dedup, which guarantees uniqueness only for
// joining with annotations.
func ssSyntheticUUID(id string, start int64, srcIP string, srcPort int64, dstIP string, dstPort int64) string {
	return base64hash(fmt.Sprintf("%s-%d-%s-%d-%s-%d", id, start, srcIP, srcPort, dstIP, dstPort))
}
