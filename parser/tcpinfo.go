package parser

/* CPU profile highlights (from TestTCPParser -count=100)
0.17s  0.38%  0.38%     23.24s 51.82%  encoding/json.Marshal
0.01s 0.022%  7.36%     22.18s 49.45%  encoding/json.sliceEncoder.encode
0.02s 0.045% 48.12%      1.87s  4.17%  github.com/m-lab/tcp-info/inetdiag.(*ipType).MarshalJSON

0.02s 0.045%  7.42%     13.06s 29.12%  encoding/json.Unmarshal

0.02s 0.045%  9.01%      5.18s 11.55%  runtime.systemstack
1.07s  2.39% 11.39%      4.50s 10.03%  runtime.mallocgc

Notes:
  1. Majority of time is spent in JSON marshaling.  This is good!
  2. ipType marshalling takes a disproportionate fraction of the time, as do other custom marshalers.
  3. Decoding the JSON from the ArchivalRecords takes 30% of the time.
  4. The zstd decoding is likely outside the profiling.
*/

import (
	"bytes"
	"errors"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/valyala/gozstd"

	"github.com/m-lab/annotation-service/api"
	v2as "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/etl/annotation"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/go/bqx"
	"github.com/m-lab/tcp-info/netlink"
	"github.com/m-lab/tcp-info/snapshot"
)

// TODO move to schema/tcpinfo.go
type ServerInfo struct {
	IP   string
	Port uint16
	IATA string

	Geo     *api.GeolocationIP
	Network *api.ASData // NOTE: dominant ASN is available at top level.
}

type ClientInfo struct {
	IP   string
	Port uint16

	Geo     *api.GeolocationIP
	Network *api.ASData // NOTE: dominant ASN is available at top level.
}

type ParseInfo struct {
	TaskFileName  string // The tar file containing this test.
	ParseTime     time.Time
	ParserVersion string
}

type TCPRow struct {
	UUID     string    // Top level just because
	TestTime time.Time // Must be top level for partitioning

	ClientASN uint32 // Top level for clustering
	ServerASN uint32 // Top level for clustering

	ParseInfo *ParseInfo

	Server *ServerInfo
	Client *ClientInfo

	FinalSnapshot *snapshot.Snapshot

	Snapshots []*snapshot.Snapshot
}

// Implement parser.Annotatable

// GetLogTime returns the timestamp that should be used for annotation.
func (row *TCPRow) GetLogTime() time.Time {
	return row.TestTime
}

// GetClientIPs returns the client (remote) IP for annotation.  See parser.Annotatable
func (row *TCPRow) GetClientIPs() []string {
	if row.Client == nil {
		snap := row.FinalSnapshot
		if snap == nil || snap.InetDiagMsg == nil {
			log.Println("No Snapshot")
			return []string{}
		}
		id := snap.InetDiagMsg.ID
		row.Client = &ClientInfo{IP: id.DstIP().String(), Port: id.DPort()}
	}

	return []string{row.Client.IP}
}

// GetServerIP returns the server (local) IP for annotation.  See parser.Annotatable
func (row *TCPRow) GetServerIP() string {
	if row.Server == nil {
		snap := row.FinalSnapshot
		if snap == nil || snap.InetDiagMsg == nil {
			log.Println("No Snapshot")
			return ""
		}
		id := snap.InetDiagMsg.ID
		row.Server = &ServerInfo{IP: id.SrcIP().String(), Port: id.SPort()}
	}

	if row.Server != nil {
		return row.Server.IP
	}
	log.Println("nil server for row")
	return row.Server.IP
}

// AnnotateClients adds the client annotations. See parser.Annotatable
func (row *TCPRow) AnnotateClients(annMap map[string]*api.Annotations) error {
	if annMap != nil {
		ann, ok := annMap[row.Client.IP]
		if ok && ann.Geo != nil {
			row.Client.Geo = ann.Geo
		}
		if ann.Network != nil {
			row.Client.Network = ann.Network
			asn, err := ann.Network.BestASN()
			if err != nil {
				log.Println(err)
			} else {

				row.ClientASN = uint32(asn)
			}
		}
	}
	return nil
}

// AnnotateServer adds the server annotations. See parser.Annotatable
func (row *TCPRow) AnnotateServer(local *api.Annotations) error {
	row.Client.Geo = local.Geo
	if local.Network != nil {
		row.Server.Network = local.Network
		asn, err := local.Network.BestASN()
		if err != nil {
			log.Println(err)
		} else {

			row.ServerASN = uint32(asn)
		}
	}
	return nil
}

// Schema returns the Bigquery schema for TCPRow.
func (row *TCPRow) Schema() (bigquery.Schema, error) {
	sch, err := bigquery.InferSchema(row)
	if err != nil {
		return bigquery.Schema{}, err
	}
	subs := map[string]bigquery.FieldSchema{
		"IDiagSPort":  bigquery.FieldSchema{Name: "IDiagSPort", Description: "", Type: "INTEGER"},
		"IDiagDPort":  bigquery.FieldSchema{Name: "IDiagDPort", Description: "", Type: "INTEGER"},
		"IDiagSrc":    bigquery.FieldSchema{Name: "IDiagSrc", Description: "", Type: "STRING"},
		"IDiagDst":    bigquery.FieldSchema{Name: "IDiagDst", Description: "", Type: "STRING"},
		"IDiagIf":     bigquery.FieldSchema{Name: "IDiagIf", Description: "", Type: "INTEGER"},
		"IDiagCookie": bigquery.FieldSchema{Name: "IDiagCookie", Description: "", Type: "INTEGER"},
	}

	c := bqx.Customize(sch, subs)
	rr := bqx.RemoveRequired(c)
	return rr, nil
}

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
		snap, decodeErr := snapshot.Decode(rec)
		if decodeErr != nil {
			err = decodeErr
			break
		}
		if snap.Metadata != nil {
			// TODO - do something with this.
			snapMeta = *snap.Metadata

		}
		if snap.Observed != 0 {
			snaps = append(snaps, snap)
		}
	}

	if err != io.EOF {
		log.Println(err)
		log.Println(string(rawContent))
		os.Exit(1)
		return err
		// TODO
	}

	if len(snaps) < 1 {
		return nil // no rows
	}

	row := TCPRow{}
	row.Snapshots = snaps
	row.FinalSnapshot = snaps[len(snaps)-1]
	row.UUID = snapMeta.UUID
	row.TestTime = snapMeta.StartTime

	row.ParseInfo = &ParseInfo{ParseTime: time.Now(), ParserVersion: Version()}

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

func NewTCPInfoParser(ins etl.Inserter) *TCPInfoParser {
	bufSize := etl.TCPINFO.BQBufferSize()
	// TODO fix * hack.
	return &TCPInfoParser{*NewBase(ins, bufSize, v2as.GetAnnotator(annotation.BatchURL))}
}
