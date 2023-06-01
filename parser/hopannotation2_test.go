package parser_test

import (
	"io/ioutil"
	"path"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/go-test/deep"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/traceroute-caller/hopannotation"
	"github.com/m-lab/uuid-annotator/annotator"
)

const (
	hopAnnotation2Filename = "20210818T174432Z_1e0b318cf3c2_91.189.88.152.json"
	hopAnnotation2GCSPath  = "gs://archive-measurement-lab/ndt/hopannotation2/2021/07/30/"
)

func TestHopAnnotation2Parser_ParseAndInsert(t *testing.T) {
	ins := newInMemorySink()
	n := parser.NewHopAnnotation2Parser(ins, "test", "_suffix")

	data, err := ioutil.ReadFile(path.Join("testdata/HopAnnotation2/", hopAnnotation2Filename))
	rtx.Must(err, "failed to load test file")

	date := civil.Date{Year: 2021, Month: 07, Day: 30}

	meta := etl.ParserMetadata{
		ArchiveURL: path.Join(hopAnnotation2GCSPath, hopAnnotation2Filename),
		Date:       date,
		Version:    parser.Version(),
		GitCommit:  parser.GitCommit(),
	}

	if err := n.ParseAndInsert(meta, hopAnnotation2Filename, data); err != nil {
		t.Errorf("HopAnnotation2Parser.ParseAndInsert() error = %v, wantErr %v", err, true)
	}

	if n.Accepted() != 1 {
		t.Fatal("Failed to insert snaplog data", ins)
	}
	n.Flush()

	row := ins.data[0].(*schema.HopAnnotation2Row)

	expectedParseInfo := schema.ParseInfo{
		Version:    "https://github.com/m-lab/etl/tree/foobar",
		Time:       row.Parser.Time,
		ArchiveURL: path.Join(hopAnnotation2GCSPath, hopAnnotation2Filename),
		Filename:   hopAnnotation2Filename,
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

	// TODO(soltesz): update traceroute-caller type.
	expectedRaw := hopannotation.HopAnnotation1{
		ID:          "20210818_1e0b318cf3c2_91.189.88.152",
		Timestamp:   time.Date(2021, 8, 18, 17, 44, 32, 0, time.UTC),
		Annotations: &expectedAnnotations,
	}

	expectedHopAnnotation2Row := schema.HopAnnotation2Row{
		ID:     "20210818_1e0b318cf3c2_91.189.88.152",
		Parser: expectedParseInfo,
		Date:   date,
		Raw:    &expectedRaw,
	}

	if diff := deep.Equal(row, &expectedHopAnnotation2Row); diff != nil {
		t.Errorf("HopAnnotation2Parser.ParseAndInsert() different row: %s", strings.Join(diff, "\n"))
	}

}

func TestHopAnnotation2_IsParsable(t *testing.T) {
	tests := []struct {
		name     string
		testName string
		want     bool
	}{
		{
			name:     "success-hopannotation1",
			testName: hopAnnotation2Filename,
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
			data, err := ioutil.ReadFile(path.Join(`testdata/HopAnnotation2/`, tt.testName))
			if err != nil {
				t.Fatalf(err.Error())
			}
			p := &parser.HopAnnotation2Parser{}
			_, got := p.IsParsable(tt.testName, data)
			if got != tt.want {
				t.Errorf("HopAnnotation2Parser.IsParsable() got = %v, want %v", got, tt.want)
			}
		})
	}
}
