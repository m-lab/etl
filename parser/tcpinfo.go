package parser

/* CPU profile highlights (from TestTCPParser -count=100) with annotation disabled.
         0     0%     0%     10.03s 44.92%  encoding/json.Marshal
         0     0%  7.03%      9.30s 41.65%  encoding/json.sliceEncoder.encode

     5.31s 23.78% 30.86%      5.31s 23.78%  runtime.pthread_cond_signal

         0     0% 30.90%      3.29s 14.73%  encoding/json.Unmarshal

Notes:
  1. Almost half of time is spent in JSON marshaling.  This is good!
  2. Decoding the JSON from the ArchivalRecords takes 15% of the time.
*/

import (
	"bytes"
	"io"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/valyala/gozstd"

	v2as "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/tcp-info/netlink"
	"github.com/m-lab/tcp-info/snapshot"

	"github.com/m-lab/etl/annotation"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/etl/schema"
)

// TCPInfoParser handles parsing for TCPINFO datatype.
type TCPInfoParser struct {
	*row.Base
	table  string
	suffix string
}

// RowsInBuffer returns the count of rows currently in the buffer.
func (p *TCPInfoParser) RowsInBuffer() int {
	return p.GetStats().Pending
}

// Committed returns the count of rows successfully committed to BQ.
func (p *TCPInfoParser) Committed() int {
	return p.GetStats().Committed
}

// Accepted returns the count of all rows received through InsertRow(s)
func (p *TCPInfoParser) Accepted() int {
	return p.GetStats().Total
}

// Failed returns the count of all rows that could not be committed.
func (p *TCPInfoParser) Failed() int {
	return p.GetStats().Failed
}

// FullTableName implements etl.Parser.FullTableName
func (p *TCPInfoParser) FullTableName() string {
	return p.table + p.suffix
}

// TableName implements etl.Parser.TableName
func (p *TCPInfoParser) TableName() string {
	return p.table
}

// TaskError return the task level error, based on failed rows, or any other criteria.
// TaskError returns non-nil if more than 10% of row inserts failed.
func (p *TCPInfoParser) TaskError() error {
	stats := p.GetStats()
	if stats.Total < 10*stats.Failed {
		log.Printf("Warning: high row insert errors (more than 10%%): %d failed of %d accepted\n",
			stats.Failed, stats.Total)
		return etl.ErrHighInsertionFailureRate
	}
	return nil
}

// Flush synchronously flushes any pending rows.
func (p *TCPInfoParser) Flush() error {
	return p.Base.Flush()
}

// IsParsable returns the canonical test type and whether to parse data.
func (p *TCPInfoParser) IsParsable(testName string, data []byte) (string, bool) {
	if strings.HasSuffix(testName, "jsonl.zst") {
		return "tcpinfo", true
	}
	return "", false
}

func thinSnaps(orig []*snapshot.Snapshot) []*snapshot.Snapshot {
	n := len(orig)
	out := make([]*snapshot.Snapshot, 0, 1+n/10)
	for i := 0; i < n; i += 10 {
		out = append(out, orig[i])
	}
	if n%10 != 0 {
		out = append(out, orig[n-1])
	}
	return out
}

// ParseAndInsert extracts all ArchivalRecords from the rawContent and inserts into a single row.
// Approximately 15 usec/snapshot.
func (p *TCPInfoParser) ParseAndInsert(fileMetadata map[string]bigquery.Value, testName string, rawContent []byte) error {
	tableName := p.FullTableName()
	metrics.WorkerState.WithLabelValues(tableName, "tcpinfo").Inc()
	defer metrics.WorkerState.WithLabelValues(tableName, "tcpinfo").Dec()

	if strings.HasSuffix(testName, "zst") {
		var err error
		rawContent, err = gozstd.Decompress(nil, rawContent)
		if err != nil {
			metrics.TestCount.WithLabelValues(p.TableName(), "", "zstd error").Inc()
			return err
		}
	}

	// This contains metadata and all snapshots from a single connection.
	rdr := bytes.NewReader(rawContent)
	ar := netlink.NewArchiveReader(rdr)

	metrics.WorkerState.WithLabelValues(tableName, "tcpinfo-parse").Inc()
	// This will include the annotation when the buffer flushes, which is unfortunate.
	defer metrics.WorkerState.WithLabelValues(tableName, "tcpinfo-parse").Dec()

	var err error
	var rec *netlink.ArchivalRecord
	snaps := make([]*snapshot.Snapshot, 0, 2000)
	testMetadata := netlink.Metadata{}
	for rec, err = ar.Next(); err != io.EOF; rec, err = ar.Next() {
		if err != nil {
			break
		}
		snapMetadata, snap, err := snapshot.Decode(rec)
		if err != nil {
			break
		}
		// meta data generally appears only once, so we have to save it.
		if snapMetadata != nil {
			testMetadata = *snapMetadata
		}
		if snap.Observed != 0 {
			snaps = append(snaps, snap)
		}
	}

	if err != io.EOF {
		log.Println(err)
		log.Println(string(rawContent))
		metrics.TestCount.WithLabelValues(p.TableName(), "", "decode error").Inc()
		return err
	}

	if len(snaps) < 1 {
		// For now, we don't save rows with no snapshots.
		metrics.TestCount.WithLabelValues(p.TableName(), "", "no-snaps").Inc()
		return nil
	}

	row := schema.TCPRow{}
	// TODO - restore full snapshots, or implement smarter filtering.
	row.Snapshots = thinSnaps(snaps)
	row.FinalSnapshot = snaps[len(snaps)-1]
	if row.FinalSnapshot.InetDiagMsg != nil {
		row.SockID = row.FinalSnapshot.InetDiagMsg.ID.GetSockID()
	}
	row.CopySocketInfo()

	row.UUID = testMetadata.UUID
	row.TestTime = testMetadata.StartTime

	row.ParseInfo = &schema.ParseInfo{ParseTime: time.Now(), ParserVersion: Version()}

	if fileMetadata["filename"] != nil {
		fn, ok := fileMetadata["filename"].(string)
		if ok {
			row.ParseInfo.TaskFileName = fn
			row.Server.IATA = etl.GetIATACode(fn)
			// TODO - should populate other ServerInfo fields from siteinfo API.
		}
	}

	// TODO - handle metrics differently for error?
	metrics.TestCount.WithLabelValues(p.TableName(), "", "ok").Inc()
	return p.Put(&row)
}

// HasParams defines interface with Params()
type HasParams interface {
	Params() etl.InserterParams
}

// NewTCPInfoParser creates a new TCPInfoParser.  Duh.
// Single annotator may be optionally passed in.
// TODO change to required parameter.
func NewTCPInfoParser(ins etl.Inserter, ann ...v2as.Annotator) *TCPInfoParser {
	bufSize := etl.TCPINFO.BQBufferSize()
	var annotator v2as.Annotator
	if len(ann) > 0 && ann[0] != nil {
		annotator = ann[0]
	} else {
		annotator = v2as.GetAnnotator(annotation.BatchURL)
	}
	sink, ok := ins.(row.Sink)
	if !ok {
		log.Printf("%v is not a Sink\n", ins)
		panic("")
	}

	return &TCPInfoParser{
		Base:   row.NewBase("foobar", sink, bufSize, annotator),
		table:  ins.TableBase(),
		suffix: ins.TableSuffix()}
}
