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
	"cloud.google.com/go/civil"

	"github.com/valyala/gozstd"

	v2as "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/tcp-info/netlink"
	"github.com/m-lab/tcp-info/snapshot"

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
	return p.GetStats().Total()
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
// TaskError returns non-nil if more than 10% of row commits failed.
func (p *TCPInfoParser) TaskError() error {
	stats := p.GetStats()
	if stats.Total() < 10*stats.Failed {
		log.Printf("Warning: high row commit errors (more than 10%%): %d failed of %d accepted\n",
			stats.Failed, stats.Total())
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
func (p *TCPInfoParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, rawContent []byte) error {
	tableName := p.FullTableName()
	metrics.WorkerState.WithLabelValues(tableName, "tcpinfo").Inc()
	defer metrics.WorkerState.WithLabelValues(tableName, "tcpinfo").Dec()

	var err error
	if strings.HasSuffix(testName, "zst") {
		rawContent, err = gozstd.Decompress(nil, rawContent)
		if err != nil {
			metrics.TestTotal.WithLabelValues(p.TableName(), "tcpinfo", "zstd error").Inc()
			return err
		}
	}

	// This contains metadata and all snapshots from a single connection.
	rdr := bytes.NewReader(rawContent)
	ar := netlink.NewArchiveReader(rdr)

	metrics.WorkerState.WithLabelValues(tableName, "tcpinfo-parse").Inc()
	// This will include the annotation when the buffer flushes, which is unfortunate.
	defer metrics.WorkerState.WithLabelValues(tableName, "tcpinfo-parse").Dec()

	var rec *netlink.ArchivalRecord
	snaps := make([]*snapshot.Snapshot, 0, 2000)
	tcpMeta := netlink.Metadata{}
	for {
		rec, err = ar.Next()
		if err != nil {
			break
		}
		snapMeta, snap, err := snapshot.Decode(rec)
		if err != nil {
			break
		}
		// meta data generally appears only once, so we have to save it.
		if snapMeta != nil {
			tcpMeta = *snapMeta
		}
		if snap.Observed != 0 {
			snaps = append(snaps, snap)
		}
	}

	if err != io.EOF {
		log.Println(err)
		metrics.TestTotal.WithLabelValues(p.TableName(), "tcpinfo", "decode error").Inc()
		metrics.ErrorCount.WithLabelValues(p.TableName(), "tcpinfo", "decode error").Inc()
		return err
	}

	if len(snaps) < 1 {
		// For now, we don't save rows with no snapshots.
		metrics.TestTotal.WithLabelValues(p.TableName(), "tcpinfo", "no-snaps").Inc()
		metrics.WarningCount.WithLabelValues(p.TableName(), "tcpinfo", "no-snaps").Inc()
		return nil
	}
	if snaps[len(snaps)-1].InetDiagMsg == nil {
		// For now, we don't save rows with nil inetdiagmsg.
		metrics.TestTotal.WithLabelValues(p.TableName(), "tcpinfo", "nil-inetdiagmsg").Inc()
		metrics.WarningCount.WithLabelValues(p.TableName(), "tcpinfo", "nil-inetdiagmsg").Inc()
		return nil
	}

	row := schema.TCPInfoRow{
		ID: tcpMeta.UUID,
		A: &schema.TCPInfoSummary{
			SockID:        snaps[len(snaps)-1].InetDiagMsg.ID.GetSockID(),
			FinalSnapshot: snaps[len(snaps)-1],
		},
		Parser: schema.ParseInfo{
			Version:    Version(),
			Time:       time.Now(),
			ArchiveURL: meta["filename"].(string),
			Filename:   testName,
			GitCommit:  GitCommit(),
		},
		Date: meta["date"].(civil.Date),
		Raw: &schema.TCPInfoRawRecord{
			Metadata: tcpMeta,
			// TODO - restore full snapshots, or implement smarter filtering.
			Snapshots: thinSnaps(snaps),
		},
	}

	if err := p.Put(&row); err != nil {
		metrics.TestTotal.WithLabelValues(p.TableName(), "tcpinfo", "put error").Inc()
		metrics.ErrorCount.WithLabelValues(p.TableName(), "tcpinfo", "put error").Inc()
		return err
	}
	metrics.TestTotal.WithLabelValues(p.TableName(), "tcpinfo", "ok").Inc()
	return nil
}

// NewTCPInfoParser creates a new parser for the TCPInfo datatype.
func NewTCPInfoParser(sink row.Sink, table, suffix string, ann v2as.Annotator) *TCPInfoParser {
	bufSize := etl.TCPINFO.BQBufferSize()
	if ann == nil {
		ann = &nullAnnotator{}
	}

	return &TCPInfoParser{
		Base:   row.NewBase("tcpinfo", sink, bufSize, ann),
		table:  table,
		suffix: suffix,
	}
}
