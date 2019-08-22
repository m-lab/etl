package parser_test

import (
	"io/ioutil"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
)

func TestNDTResultParser(t *testing.T) {
	ins := newInMemoryInserter()

	n := parser.NewNDTResultParser(ins)

	resultName := `ndt-5hkck_1566219987_000000000000017D.json`
	resultData, err := ioutil.ReadFile(`testdata/NDTResult/` + resultName)
	if err != nil {
		t.Fatalf(err.Error())
	}

	meta := map[string]bigquery.Value{
		"filename": "gs://mlab-test-bucket/ndt/ndt5/2019/08/22/ndt_ndt5_2019_08_22_20190822T194819.568936Z-ndt5-mlab1-lga0t-ndt.tgz",
	}
	err = n.ParseAndInsert(meta, resultName, resultData)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if ins.Accepted() != 1 {
		t.Fatalf("Failed to insert snaplog data.")
	}

	actualValues := ins.data[0].(schema.NDTResult)
	// pretty.Print(actualValues)
	if actualValues.Result.Control == nil {
		t.Fatal("Result.Control is nil, expected value")
	}
	if actualValues.Result.Control.UUID != "ndt-5hkck_1566219987_000000000000017D" {
		t.Fatalf("Result.Control.UUID incorrect; got %q ; want %q", actualValues.Result.Control.UUID, "ndt-5hkck_1566219987_000000000000017D")
	}
	if len(actualValues.Result.Control.ClientMetadata) != 1 {
		t.Fatalf("Result.Control.ClientMetadata length != 1; got %d, want 1", len(actualValues.Result.Control.ClientMetadata))
	}
	if actualValues.Result.Control.ClientMetadata[0].Name != "client.os.name" || actualValues.Result.Control.ClientMetadata[0].Value != "NDTjs" {
		t.Fatalf("Result.Control.ClientMetadata has wrong value; got %q=%q, want client.os.name=NDTjs",
			actualValues.Result.Control.ClientMetadata[0].Name,
			actualValues.Result.Control.ClientMetadata[0].Value)
	}
}

func xyzTestNDTResultParser_IsParsable(t *testing.T) {

	unparsable := `ndt-vscqp_1565987984_000000000001A1C2.json`
	unparsableData, err := ioutil.ReadFile(`testdata/NDTResult/` + unparsable)
	if err != nil {
		t.Fatalf(err.Error())
	}
	p := &parser.NDTResultParser{}
	_, parsable := p.IsParsable("ndt-vscqp_1565987984_000000000001A1C2.json", unparsableData)
	if parsable {
		t.Errorf("NDTResultParser.IsParsable() got = %v, want false", parsable)
	}
}

func TestNDTResultParser_IsParsable(t *testing.T) {
	tests := []struct {
		name     string
		testName string
		want     bool
	}{
		{
			name:     "error-old-client-metadata",
			testName: `ndt-vscqp_1565987984_000000000001A1C2.json`,
			want:     false,
		},
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
			data, err := ioutil.ReadFile(`testdata/NDTResult/` + tt.testName)
			if err != nil {
				t.Fatalf(err.Error())
			}
			p := &parser.NDTResultParser{}
			_, got := p.IsParsable(tt.testName, data)
			if got != tt.want {
				t.Errorf("NDTResultParser.IsParsable() got1 = %v, want %v", got, tt.want)
			}
		})
	}
}
