package schema

import (
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/go/bqx"
	"github.com/m-lab/go/rtx"
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
// This is a rather messy work around the fact that the snapshot SockID
// has byte slice fields that have to be converted to the appropriate type.
// Tried writing Save() function just for the SockID, but that didn't work as expected.
// TODO - consider substituting the InetDiagMsg with a struct containing the desired SockID format.
func (r *TCPRow) Save() (map[string]bigquery.Value, string, error) {
	ss := bigquery.StructSaver{Schema: tcpSchema, InsertID: "", Struct: r}
	m, insertID, err := ss.Save()
	w := Web100ValueMap(m)

	if w["FinalSnapshot"] != nil {
		idm := w.GetMap([]string{"FinalSnapshot", "InetDiagMsg"})
		idSrc := r.FinalSnapshot.InetDiagMsg.ID
		idm["ID"], _, _ = idSrc.Save()
	}

	if r.Snapshots != nil {
		snapMaps := m["Snapshots"].([]bigquery.Value)
		for i := range r.Snapshots {
			snap := r.Snapshots[i]
			if i >= len(snapMaps) || snap == nil || snap.InetDiagMsg == nil {
				continue
			}
			idSrc := snap.InetDiagMsg.ID
			sm := snapMaps[i]
			snapMap, ok := sm.(map[string]bigquery.Value)
			if !ok {
				continue
			}
			idm, ok := snapMap["InetDiagMsg"]
			if !ok {
				continue
			}
			idmMap, ok := idm.(map[string]bigquery.Value)
			if ok {
				idmMap["ID"], _, _ = idSrc.Save()
			}
		}
	}
	return m, insertID, err
}

// Schema returns the Bigquery schema for TCPRow.
func (r *TCPRow) Schema() (bigquery.Schema, error) {
	sch, err := bigquery.InferSchema(r)
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
