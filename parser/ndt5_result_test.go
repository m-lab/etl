package parser_test

import (
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/civil"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
)

func TestNDT5ResultParser_ParseAndInsert(t *testing.T) {
	tests := []struct {
		name           string
		testName       string
		expectMetadata bool
		emptySummary   bool
		expectTCPInfo  bool
		wantErr        bool
	}{
		{
			name:           "success-with-metadata",
			testName:       `ndt-5hkck_1566219987_000000000000017D.json`,
			expectMetadata: true,
		},
		{
			name:           "success-without-metadata",
			testName:       `ndt-vscqp_1565987984_000000000001A1C2.json`,
			expectMetadata: false,
		},
		{
			name:         "success-empty-s2c-and-c2s",
			testName:     `ndt-x5dms_1589313593_0000000000024063.json`,
			emptySummary: true,
		},
		{
			name:          "success-s2c-with-tcpinfo",
			testName:      `ndt-m9pcq_1652405655_000000000014FD22.json`,
			expectTCPInfo: true,
		},
		{
			name:     "success-remove-download-unsafe-uuid",
			testName: `ndt-rczlq_1666977535_unsafe_0000000000169592.json`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ins := newInMemorySink()
			n := parser.NewNDT5ResultParser(ins, "test", "_suffix")

			resultData, err := ioutil.ReadFile(`testdata/NDT5Result/` + tt.testName)
			if err != nil {
				t.Fatalf(err.Error())
			}
			d, err := civil.ParseDate("2019-08-22")
			if err != nil {
				t.Fatalf(err.Error())
			}
			meta := etl.Metadata{
				ArchiveURL: "gs://mlab-test-bucket/ndt/ndt5/2019/08/22/ndt_ndt5_2019_08_22_20190822T194819.568936Z-ndt5-mlab1-lga0t-ndt.tgz",
				Date:       d,
				Version:    parser.Version(),
				GitCommit:  parser.GitCommit(),
			}

			if err := n.ParseAndInsert(meta, tt.testName, resultData); (err != nil) != tt.wantErr {
				t.Errorf("NDT5ResultParser.ParseAndInsert() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.emptySummary {
				if n.Accepted() != 1 {
					t.Fatal("Failed to insert measurement with no c2s or s2c data.", ins)
				}
				return
			}
			if n.Accepted() != 2 {
				t.Fatal("Failed to insert snaplog data.", ins)
			}
			n.Flush()

			// Should include download (0) and upload (1).
			download := ins.data[0].(*schema.NDT5ResultRowV2)
			if download.Raw.Control == nil {
				t.Fatal("Raw.Control is nil, expected value")
			}
			if download.Raw.Control.UUID != strings.TrimSuffix(tt.testName, ".json") {
				t.Fatalf("Raw.Control.UUID incorrect; got %q ; want %q", download.Raw.Control.UUID, strings.TrimSuffix(tt.testName, ".json"))
			}
			if tt.expectMetadata && len(download.Raw.Control.ClientMetadata) != 1 {
				t.Fatalf("Raw.Control.ClientMetadata length != 1; got %d, want 1", len(download.Raw.Control.ClientMetadata))
			}
			if tt.expectMetadata && (download.Raw.Control.ClientMetadata[0].Name != "client.os.name" || download.Raw.Control.ClientMetadata[0].Value != "NDTjs") {
				t.Fatalf("Raw.Control.ClientMetadata has wrong value; got %q=%q, want client.os.name=NDTjs",
					download.Raw.Control.ClientMetadata[0].Name,
					download.Raw.Control.ClientMetadata[0].Value)
			}
			if strings.Contains(download.ID, "_unsafe") || strings.Contains(download.A.UUID, "_unsafe") {
				t.Fatalf("ID or A.UUID contain the string '_unsafe'")
			}
			if download.Raw.S2C == nil {
				t.Fatalf("Raw.S2C is nil")
			}
			if download.Raw.S2C.UUID != download.A.UUID {
				t.Fatalf("Raw.S2C.UUID does not match A.UUID; got %s, want %s",
					download.Raw.S2C.UUID, download.A.UUID)
			}
			// Verify a.MinRTT when S2C.TCPInfo is present.
			if tt.expectTCPInfo && download.A.MinRTT != float64(download.Raw.S2C.TCPInfo.MinRTT)/1000.0/1000.0 {
				t.Fatalf("A.MinRTT does not match Raw.S2C.TCPInfo.MinRTT; got %f, want %f",
					download.A.MinRTT, float64(download.Raw.S2C.TCPInfo.MinRTT)/1000.0/1000.0)
			}
			// Verify a.MinRTT when S2C.TCPInfo is not present.
			if tt.expectMetadata && download.A.MinRTT != float64(download.Raw.S2C.MinRTT)/float64(time.Millisecond) {
				t.Fatalf("A.MinRTT does not match Raw.S2C.MinRTT; got %f, want %f",
					download.A.MinRTT, float64(download.Raw.S2C.MinRTT)/float64(time.Millisecond))
			}
			upload := ins.data[1].(*schema.NDT5ResultRowV2)
			if upload.Raw.Control == nil {
				t.Fatal("Raw.Control is nil, expected value")
			}
			if upload.Raw.Control.UUID != strings.TrimSuffix(tt.testName, ".json") {
				t.Fatalf("Raw.Control.UUID incorrect; got %q ; want %q", upload.Raw.Control.UUID, strings.TrimSuffix(tt.testName, ".json"))
			}
			if tt.expectMetadata && len(upload.Raw.Control.ClientMetadata) != 1 {
				t.Fatalf("Raw.Control.ClientMetadata length != 1; got %d, want 1", len(upload.Raw.Control.ClientMetadata))
			}
			if tt.expectMetadata && (upload.Raw.Control.ClientMetadata[0].Name != "client.os.name" || upload.Raw.Control.ClientMetadata[0].Value != "NDTjs") {
				t.Fatalf("Raw.Control.ClientMetadata has wrong value; got %q=%q, want client.os.name=NDTjs",
					upload.Raw.Control.ClientMetadata[0].Name,
					upload.Raw.Control.ClientMetadata[0].Value)
			}
			if strings.Contains(upload.ID, "_unsafe") || strings.Contains(upload.A.UUID, "_unsafe") {
				t.Fatalf("ID or A.UUID contain the string '_unsafe'")
			}
			if upload.Raw.C2S == nil {
				t.Fatalf("Raw.C2S is nil")
			}
			if upload.Raw.C2S.UUID != upload.A.UUID {
				t.Fatalf("Raw.C2S.UUID does not match A.UUID; got %s, want %s",
					upload.Raw.C2S.UUID, upload.A.UUID)
			}
		})
	}
}

func TestNDT5ResultParser_IsParsable(t *testing.T) {
	tests := []struct {
		name     string
		testName string
		want     bool
	}{
		{
			name:     "success-new-client-metadata",
			testName: `ndt-5hkck_1566219987_000000000000017D.json`,
			want:     true,
		},
		{
			name:     "error-bad-extension",
			testName: `badfile.badextension`,
			want:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := ioutil.ReadFile(`testdata/NDT5Result/` + tt.testName)
			if err != nil {
				t.Fatalf(err.Error())
			}
			p := &parser.NDT5ResultParser{}
			_, got := p.IsParsable(tt.testName, data)
			if got != tt.want {
				t.Errorf("NDT5ResultParser.IsParsable() got1 = %v, want %v", got, tt.want)
			}
		})
	}
}
