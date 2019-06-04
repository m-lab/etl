package schema

import (
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/annotation-service/api"
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
func (r *TCPRow) Save() (map[string]bigquery.Value, string, error) {
	ss := bigquery.StructSaver{Schema: tcpSchema, InsertID: r.UUID, Struct: r}
	return ss.Save()
}

// Schema returns the Bigquery schema for TCPRow.
func (r *TCPRow) Schema() (bigquery.Schema, error) {
	sch, err := bigquery.InferSchema(r)
	if err != nil {
		return bigquery.Schema{}, err
	}
	rr := bqx.RemoveRequired(sch)
	return rr, nil
}
