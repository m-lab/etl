package parser_test

import (
	"io/ioutil"
	"strings"
	"testing"

	"cloud.google.com/go/civil"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
)

func TestNDT5ResultParser_ParseAndInsert(t *testing.T) {
	tests := []struct {
		name           string
		testName       string
		expectMetadata bool
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ins := newInMemorySink()
			n := parser.NewNDT5ResultParser(ins, "test", "_suffix", &fakeAnnotator{})

			resultData, err := ioutil.ReadFile(`testdata/NDT5Result/` + tt.testName)
			if err != nil {
				t.Fatalf(err.Error())
			}
			d, err := civil.ParseDate("2019-08-22")
			if err != nil {
				t.Fatalf(err.Error())
			}
			meta := map[string]bigquery.Value{
				"filename": "gs://mlab-test-bucket/ndt/ndt5/2019/08/22/ndt_ndt5_2019_08_22_20190822T194819.568936Z-ndt5-mlab1-lga0t-ndt.tgz",
				"date":     d,
			}

			if err := n.ParseAndInsert(meta, tt.testName, resultData); (err != nil) != tt.wantErr {
				t.Errorf("NDT5ResultParser.ParseAndInsert() error = %v, wantErr %v", err, tt.wantErr)
			}
			if n.Accepted() != 1 {
				t.Fatal("Failed to insert snaplog data.", ins)
			}
			n.Flush()
			actualValues := ins.data[0].(*schema.NDT5ResultRowV2)
			if actualValues.Raw.Control == nil {
				t.Fatal("Raw.Control is nil, expected value")
			}
			if actualValues.Raw.Control.UUID != strings.TrimSuffix(tt.testName, ".json") {
				t.Fatalf("Raw.Control.UUID incorrect; got %q ; want %q", actualValues.Raw.Control.UUID, strings.TrimSuffix(tt.testName, ".json"))
			}
			if tt.expectMetadata && len(actualValues.Raw.Control.ClientMetadata) != 1 {
				t.Fatalf("Raw.Control.ClientMetadata length != 1; got %d, want 1", len(actualValues.Raw.Control.ClientMetadata))
			}
			if tt.expectMetadata && (actualValues.Raw.Control.ClientMetadata[0].Name != "client.os.name" || actualValues.Raw.Control.ClientMetadata[0].Value != "NDTjs") {
				t.Fatalf("Raw.Control.ClientMetadata has wrong value; got %q=%q, want client.os.name=NDTjs",
					actualValues.Raw.Control.ClientMetadata[0].Name,
					actualValues.Raw.Control.ClientMetadata[0].Value)
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
