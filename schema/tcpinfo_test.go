package schema_test

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/tcp-info/inetdiag"

	"github.com/davecgh/go-spew/spew"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/etl/storage"
	"github.com/m-lab/tcp-info/snapshot"
)

func localETLSource(fn string) (*storage.ETLSource, error) {
	if !(strings.HasSuffix(fn, ".tgz") || strings.HasSuffix(fn, ".tar") ||
		strings.HasSuffix(fn, ".tar.gz")) {
		return nil, errors.New("not tar or tgz: " + fn)
	}

	var rdr io.ReadCloser
	var raw io.ReadCloser
	raw, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	// Handle .tar.gz, .tgz files.
	if strings.HasSuffix(strings.ToLower(fn), "gz") {
		rdr, err = gzip.NewReader(raw)
		if err != nil {
			raw.Close()
			return nil, err
		}
	} else {
		rdr = raw
	}
	tarReader := tar.NewReader(rdr)

	timeout := 16 * time.Millisecond
	return &storage.ETLSource{TarReader: tarReader, Closer: raw, RetryBaseTime: timeout, TableBase: "test"}, nil
}

func TestBQSaver(t *testing.T) {
	row := schema.TCPRow{UUID: "foobar"}
	row.FinalSnapshot = &snapshot.Snapshot{InetDiagMsg: &inetdiag.InetDiagMsg{}}
	row.FinalSnapshot.InetDiagMsg.ID.IDiagSPort[1] = 123
	row.Snapshots = make([]*snapshot.Snapshot, 2)
	row.Snapshots[0] = &snapshot.Snapshot{InetDiagMsg: &inetdiag.InetDiagMsg{}}
	row.Snapshots[1] = &snapshot.Snapshot{} // Leave this without InetDiagMsg to test nil handling

	rowMap, _, _ := row.Save()
	if rowMap["UUID"] != "foobar" {
		t.Error(spew.Sdump(rowMap))
	}

	fs := rowMap["FinalSnapshot"].(map[string]bigquery.Value)
	if fs != nil {
		idm := fs["InetDiagMsg"].(map[string]bigquery.Value)
		id := idm["ID"].(map[string]bigquery.Value)
		if id["SPort"] != int64(123) {
			t.Error(id)
		}
	} else {
		t.Error("Nil FinalSnapshot")
	}
	snapMaps, ok := rowMap["Snapshots"].([]bigquery.Value)
	if !ok || snapMaps == nil {
		t.Fatal("Nil snapshots")
	}
	sm := snapMaps[0]
	snapMap, ok := sm.(map[string]bigquery.Value)
	if snapMap == nil || !ok {
		t.Fatal("Problem with first snapshot")
	}
	idm, ok := snapMap["InetDiagMsg"]
	if !ok {
		t.Fatal("problem with idm")
	}
	_, ok = idm.(map[string]bigquery.Value)
	if !ok {
		t.Fatal("problem with idm")
	}
}
