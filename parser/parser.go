// Package parser defines the Parser interface and implementations for the different
// test types, NDT, Paris Traceroute, and SideStream.
package parser

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"log"
	"reflect"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/annotation-service/api/v2"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
)

func init() {
	initParserVersion()
}

var gParserVersion = "uninitialized"

// initParserVersion initializes the gParserVersion variable for use by all parsers.
func initParserVersion() string {
	release := etl.Version
	if release != "noversion" {
		gParserVersion = "https://github.com/m-lab/etl/tree/" + release
	} else {
		hash := etl.GitCommit
		if hash != "nocommit" && len(hash) >= 8 {
			gParserVersion = "https://github.com/m-lab/etl/tree/" + hash[0:8]
		} else {
			gParserVersion = "local development"
		}
	}
	return gParserVersion
}

// Version returns the parser version used by parsers to annotate data rows.
func Version() string {
	return gParserVersion
}

// NewSinkParser creates an appropriate parser for a given data type.
// Eventually all datatypes will use this instead of NewParser.
func NewSinkParser(dt etl.DataType, sink row.Sink, table string, ann api.Annotator) etl.Parser {
	switch dt {
	case etl.ANNOTATION:
		return NewAnnotationParser(sink, table, "", ann)
	case etl.NDT5:
		return NewNDT5ResultParser(sink, table, "", ann)
	case etl.NDT7:
		return NewNDT7ResultParser(sink, table, "", ann)
	case etl.TCPINFO:
		return NewTCPInfoParser(sink, table, "", ann)
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
	case etl.NDT5:
		sink, ok := ins.(row.Sink)
		if !ok {
			log.Printf("%v is not a Sink\n", ins)
			log.Println(reflect.TypeOf(ins))
			return nil
		}
		return NewNDT5ResultParser(sink, ins.TableBase(), ins.TableSuffix(), nil)

	case etl.SS:
		return NewDefaultSSParser(ins) // TODO fix this hack.
	case etl.PT:
		return NewPTParser(ins)
	case etl.SW:
		return NewDiscoParser(ins)
	case etl.TCPINFO:
		sink, ok := ins.(row.Sink)
		if !ok {
			log.Printf("%v is not a Sink\n", ins)
			log.Println(reflect.TypeOf(ins))
			return nil
		}
		return NewTCPInfoParser(sink, ins.TableBase(), ins.TableSuffix(), nil)
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
	metrics.TestCount.WithLabelValues("table", "null", "ok").Inc()
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
