package parser_test

import (
	"io/ioutil"
	"path"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/go-test/deep"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/uuid-annotator/annotator"
)

const (
	hopAnnotation1Filename = "20210818T174432Z_1e0b318cf3c2_91.189.88.152.json"
	hopAnnotation1GCSPath  = "gs://archive-measurement-lab/ndt/hopannotation1/2021/07/30/"
)

func TestHopAnnotation1Parser_ParseAndInsert(t *testing.T) {
	ins := newInMemorySink()
	n := parser.NewHopAnnotation1Parser(ins, "test", "_suffix", &fakeAnnotator{})

	data, err := ioutil.ReadFile(path.Join("testdata/HopAnnotation1/", hopAnnotation1Filename))
	rtx.Must(err, "failed to load test file")

	date := civil.Date{Year: 2021, Month: 07, Day: 30}

	meta := map[string]bigquery.Value{
		"filename": path.Join(hopAnnotation1GCSPath, hopAnnotation1Filename),
		"date":     date,
	}

	if err := n.ParseAndInsert(meta, hopAnnotation1Filename, data); err != nil {
		t.Errorf("HopAnnotation1Parser.ParseAndInsert() error = %v, wantErr %v", err, true)
	}

	if n.Accepted() != 1 {
		t.Fatal("Failed to insert snaplog data", ins)
	}
	n.Flush()

	row := ins.data[0].(*schema.HopAnnotation1Row)

	expectedParseInfo := schema.ParseInfo{
		Version:    "https://github.com/m-lab/etl/tree/foobar",
		Time:       row.Parser.Time,
		ArchiveURL: path.Join(hopAnnotation1GCSPath, hopAnnotation1Filename),
		Filename:   hopAnnotation1Filename,
		Priority:   0,
		GitCommit:  "12345678",
	}

	expectedGeolocation := annotator.Geolocation{
		ContinentCode:       "EU",
		CountryCode:         "GB",
		CountryName:         "United Kingdom",
		Subdivision1ISOCode: "ENG",
		Subdivision1Name:    "England",
		City:                "London",
		PostalCode:          "EC2V",
		Latitude:            51.5095,
		Longitude:           -0.0955,
		AccuracyRadiusKm:    200,
	}

	expectedNetwork := annotator.Network{
		CIDR:     "91.189.88.0/21",
		ASNumber: 41231,
		ASName:   "Canonical Group Limited",
		Systems: []annotator.System{
			{ASNs: []uint32{41231}},
		},
	}

	expectedAnnotations := annotator.ClientAnnotations{
		Geo:     &expectedGeolocation,
		Network: &expectedNetwork,
	}

	expectedRaw := schema.HopAnnotation1{
		ID:          "20210818_1e0b318cf3c2_91.189.88.152",
		Timestamp:   time.Date(2021, 8, 18, 17, 44, 32, 0, time.UTC),
		Annotations: &expectedAnnotations,
	}

	expectedHopAnnotation1Row := schema.HopAnnotation1Row{
		ID:     "20210818_1e0b318cf3c2_91.189.88.152",
		Parser: expectedParseInfo,
		Date:   date,
		Raw:    &expectedRaw,
	}

	if diff := deep.Equal(row, &expectedHopAnnotation1Row); diff != nil {
		t.Errorf("HopAnnotation1Parser.ParseAndInsert() different row: %s", strings.Join(diff, "\n"))
	}

}

func TestHopAnnotation1_IsParsable(t *testing.T) {
	tests := []struct {
		name     string
		testName string
		want     bool
	}{
		{
			name:     "success-hopannotation1",
			testName: hopAnnotation1Filename,
			want:     true,
		},
		{
			name:     "error-bad-extension",
			testName: "badfile.badextension",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := ioutil.ReadFile(path.Join(`testdata/HopAnnotation1/`, tt.testName))
			if err != nil {
				t.Fatalf(err.Error())
			}
			p := &parser.HopAnnotation1Parser{}
			_, got := p.IsParsable(tt.testName, data)
			if got != tt.want {
				t.Errorf("HopAnnotation1Parser.IsParsable() got = %v, want %v", got, tt.want)
			}
		})
	}
}
