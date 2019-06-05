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
	"errors"
	"io"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/valyala/gozstd"

	v2as "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/etl/annotation"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/tcp-info/netlink"
	"github.com/m-lab/tcp-info/snapshot"
)

// TCPInfoParser handles parsing for TCPINFO datatype.
type TCPInfoParser struct {
	Base
}

// TaskError return the task level error, based on failed rows, or any other criteria.
// TaskError returns non-nil if more than 10% of row inserts failed.
func (p *TCPInfoParser) TaskError() error {
	if p.Committed() < 10*p.Failed() {
		log.Printf("Warning: high row insert errors: %d / %d\n",
			p.Accepted(), p.Failed())
		return errors.New("too many insertion failures")
	}
	return nil
}

// TableName of the table that this Parser inserts into.
func (p *TCPInfoParser) TableName() string {
	return p.TableBase()
}

// Flush synchronously flushes any pending rows.
func (p *TCPInfoParser) Flush() error {
	p.Put(p.TakeRows())
	return p.Inserter.Flush()
}

// IsParsable returns the canonical test type and whether to parse data.
func (p *TCPInfoParser) IsParsable(testName string, data []byte) (string, bool) {
	return "tcpinfo", true
}

// ParseAndInsert extracts all ArchivalRecords from the rawContent and inserts into a single row.
// Approximately 15 usec/snapshot.
func (p *TCPInfoParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, rawContent []byte) error {
	if strings.HasSuffix(testName, "zst") {
		var err error
		rawContent, err = gozstd.Decompress(nil, rawContent)
		if err != nil {
			return err
		}
	}

	// This contains metadata and all snapshots from a single connection.
	rdr := bytes.NewReader(rawContent)
	ar := netlink.NewArchiveReader(rdr)

	var err error
	var rec *netlink.ArchivalRecord
	snaps := make([]*snapshot.Snapshot, 0, 2000)
	snapMeta := netlink.Metadata{}
	for rec, err = ar.Next(); err != io.EOF; rec, err = ar.Next() {
		if err != nil {
			break
		}
		meta, snap, err := snapshot.Decode(rec)
		if err != nil {
			break
		}
		// meta data generally appears only once, so we have to save it.
		if meta != nil {
			snapMeta = *meta
		}
		if snap.Observed != 0 {
			snaps = append(snaps, snap)
		}
	}

	if err != io.EOF {
		log.Println(err)
		log.Println(string(rawContent))
		return err
	}

	if len(snaps) < 1 {
		// For now, we don't save rows with no snapshots.
		return nil
	}

	row := schema.TCPRow{}
	row.Snapshots = snaps
	row.FinalSnapshot = snaps[len(snaps)-1]
	if row.FinalSnapshot.InetDiagMsg != nil {
		row.SockID = row.FinalSnapshot.InetDiagMsg.ID.GetSockID()
	}
	row.UUID = snapMeta.UUID
	row.TestTime = snapMeta.StartTime

	row.ParseInfo = &schema.ParseInfo{ParseTime: time.Now(), ParserVersion: Version()}

	if meta["filename"] != nil {
		row.ParseInfo.TaskFileName = meta["filename"].(string)
	}

	err = p.AddRow(&row)
	if err == etl.ErrBufferFull {
		// Flush asynchronously, to improve throughput.
		p.Annotate(p.TableName())
		p.PutAsync(p.TakeRows())
		err = p.AddRow(&row)
	}

	return err
}

// NewTCPInfoParser creates a new TCPInfoParser.  Duh.
// TODO - pass in the annotator.
func NewTCPInfoParser(ins etl.Inserter) *TCPInfoParser {
	bufSize := etl.TCPINFO.BQBufferSize()
	return &TCPInfoParser{*NewBase(ins, bufSize, v2as.GetAnnotator(annotation.BatchURL))}
}
