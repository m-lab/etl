package parser

import (
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	v2as "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/etl/schema"
)

//=====================================================================================
//                       PCAP Parser
//=====================================================================================

// PCAPParser parses the PCAP datatype from the packet-headers process.
type PCAPParser struct {
	*row.Base
	table  string
	suffix string
}

// NewPCAPParser returns a new parser for PCAP archives.
func NewPCAPParser(sink row.Sink, table, suffix string, ann v2as.Annotator) etl.Parser {
	bufSize := etl.PCAP.BQBufferSize()
	if ann == nil {
		ann = v2as.GetAnnotator(etl.BatchAnnotatorURL)
	}

	return &PCAPParser{
		Base:   row.NewBase(table, sink, bufSize, ann),
		table:  table,
		suffix: suffix,
	}

}

// IsParsable returns the canonical test type and whether to parse data.
func (parser *PCAPParser) IsParsable(testName string, data []byte) (string, bool) {
	// Files look like (.*).pcap.gz .
	if strings.HasSuffix(testName, "pcap.gz") {
		return "pcap", true
	}
	return "", false
}

// ParseAndInsert decodes the PCAP data and inserts it into BQ.
func (parser *PCAPParser) ParseAndInsert(fileMetadata map[string]bigquery.Value, testName string, rawContent []byte) error {
	metrics.WorkerState.WithLabelValues(parser.TableName(), "pcap").Inc()
	defer metrics.WorkerState.WithLabelValues(parser.TableName(), "pcap").Dec()

	row := schema.PCAPRow{
		Parser: schema.ParseInfo{
			Version:    Version(),
			Time:       time.Now(),
			ArchiveURL: fileMetadata["filename"].(string),
			Filename:   testName,
			GitCommit:  GitCommit(),
		},
	}

	// NOTE: Civil is not TZ adjusted. It takes the year, month, and date from
	// the given timestamp, regardless of the timestamp's timezone. Since we
	// run our systems in UTC, all timestamps will be relative to UTC and as
	// will these dates.
	row.Date = fileMetadata["date"].(civil.Date)
	row.ID = parser.GetUUID(testName)

	// Insert the row.
	if err := parser.Put(&row); err != nil {
		return err
	}

	// Count successful inserts.
	metrics.TestCount.WithLabelValues(parser.TableName(), "pcap", "ok").Inc()

	return nil
}

// GetUUID extracts the UUID from the filename.
// For example, for filename 2021/07/22/ndt-4c6fb_1625899199_00000000013A4623.pcap.gz,
// it returns ndt-4c6fb_1625899199_00000000013A4623.
func (parser *PCAPParser) GetUUID(filename string) string {
	regex := regexp.MustCompile(`\d{4}/\d{2}/\d{2}/`)
	id := regex.ReplaceAllString(filename, "")
	if len(id) >= 8 {
		return id[:len(id)-8]
	}
	return id
}

// NB: These functions are also required to complete the etl.Parser interface
// For PCAP, we just forward the calls to the Inserter.

func (parser *PCAPParser) Flush() error {
	return parser.Base.Flush()
}

func (parser *PCAPParser) TableName() string {
	return parser.table
}

func (parser *PCAPParser) FullTableName() string {
	return parser.table + parser.suffix
}

// RowsInBuffer returns the count of rows currently in the buffer.
func (parser *PCAPParser) RowsInBuffer() int {
	return parser.GetStats().Pending
}

// Committed returns the count of rows successfully committed to BQ.
func (parser *PCAPParser) Committed() int {
	return parser.GetStats().Committed
}

// Accepted returns the count of all rows received through InsertRow(s).
func (parser *PCAPParser) Accepted() int {
	return parser.GetStats().Total()
}

// Failed returns the count of all rows that could not be committed.
func (parser *PCAPParser) Failed() int {
	return parser.GetStats().Failed
}
