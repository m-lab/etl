package schema

import (
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/go/bqx"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/tcp-info/inetdiag"
	"github.com/m-lab/tcp-info/snapshot"
)

// ServerInfo details various information about the server.
type ServerInfo struct {
	IP   string
	Port uint16
	IATA string

	Geo     *api.GeolocationIP
	Network *api.ASData // NOTE: dominant ASN is available at top level.
}

// ClientInfo details various information about the client.
type ClientInfo struct {
	IP   string
	Port uint16

	Geo     *api.GeolocationIP
	Network *api.ASData // NOTE: dominant ASN is available at top level.
}

// ParseInfo provides details about the parsing of this row.
type ParseInfo struct {
	TaskFileName  string // The tar file containing this test.
	ParseTime     time.Time
	ParserVersion string
}

// TCPRow describes a single BQ row of TCPInfo data.
type TCPRow struct {
	UUID     string    // Top level just because
	TestTime time.Time // Must be top level for partitioning

	ClientASN uint32 // Top level for clustering
	ServerASN uint32 // Top level for clustering

	ParseInfo *ParseInfo

	SockID inetdiag.SockID

	Server *ServerInfo
	Client *ClientInfo

	FinalSnapshot *snapshot.Snapshot

	Snapshots []*snapshot.Snapshot
}

func assertTCPRowIsValueSaver(r *TCPRow) {
	func(bigquery.ValueSaver) {}(r)
}

func init() {
	var err error
	tcpSchema, err = (&TCPRow{}).Schema()
	rtx.Must(err, "Error generating tcp schema")
}

var tcpSchema bigquery.Schema

// Save implements bigquery.ValueSaver
func (row *TCPRow) Save() (map[string]bigquery.Value, string, error) {
	ss := bigquery.StructSaver{Schema: tcpSchema, InsertID: row.UUID, Struct: row}
	return ss.Save()
}

// Schema returns the Bigquery schema for TCPRow.
func (row *TCPRow) Schema() (bigquery.Schema, error) {
	sch, err := bigquery.InferSchema(row)
	if err != nil {
		return bigquery.Schema{}, err
	}
	rr := bqx.RemoveRequired(sch)
	return rr, nil
}

// Implement parser.Annotatable

// GetLogTime returns the timestamp that should be used for annotation.
func (row *TCPRow) GetLogTime() time.Time {
	return row.TestTime
}

// GetClientIPs returns the client (remote) IP for annotation.  See parser.Annotatable
func (row *TCPRow) GetClientIPs() []string {
	if row.Client == nil {
		return []string{row.SockID.DstIP}
	}
	return []string{row.Client.IP}
}

// GetServerIP returns the server (local) IP for annotation.  See parser.Annotatable
func (row *TCPRow) GetServerIP() string {
	if row.Server == nil {
		metrics.AnnotationMissingCount.WithLabelValues("missing ServerIP").Inc()
		return row.SockID.SrcIP
	}
	return row.Server.IP
}

// AnnotateClients adds the client annotations. See parser.Annotatable
// annMap must not be null
func (row *TCPRow) AnnotateClients(annMap map[string]*api.Annotations) error {
	ann, ok := annMap[row.Client.IP]
	if !ok {
		metrics.AnnotationMissingCount.WithLabelValues("No annotation for IP").Inc()
		return nil
	}
	if ann.Geo == nil {
		metrics.AnnotationMissingCount.WithLabelValues("Empty ann.Geo").Inc()
	} else {
		row.Client.Geo = ann.Geo
	}
	if ann.Network == nil {
		metrics.AnnotationMissingCount.WithLabelValues("Empty ann.Network").Inc()
	} else {
		row.Client.Network = ann.Network
		asn, err := ann.Network.BestASN()
		if err != nil {
			metrics.AnnotationMissingCount.WithLabelValues("BestASN failed").Inc()
			return nil
		}
		row.ClientASN = uint32(asn)
	}
	return nil
}

// AnnotateServer adds the server annotations. See parser.Annotatable
// local must not be null
func (row *TCPRow) AnnotateServer(local *api.Annotations) error {
	row.Client.Geo = local.Geo
	if local.Network != nil {
		row.Server.Network = local.Network
		asn, err := local.Network.BestASN()
		if err != nil {
			metrics.AnnotationMissingCount.WithLabelValues("BestASN failed").Inc()
		}
		row.ServerASN = uint32(asn)
	}
	return nil
}
