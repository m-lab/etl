package parser_test

import (
	"io/ioutil"
	"path"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/go-test/deep"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/go/rtx"
)

const (
	pcapFilename = "ndt-4c6fb_1625899199_000000000121C1A0.pcap.gz"
	pcapGCSPath  = "gs://archive-measurement-lab/ndt/pcap/2021/07/22/"
)

func TestPCAPParser_ParseAndInsert(t *testing.T) {
	ins := newInMemorySink()
	n := parser.NewPCAPParser(ins, "test", "_suffix", &fakeAnnotator{})

	data, err := ioutil.ReadFile(path.Join("testdata/PCAP/", pcapFilename))
	rtx.Must(err, "failed to load test file")

	date := civil.Date{Year: 2021, Month: 07, Day: 22}

	meta := map[string]bigquery.Value{
		"filename": path.Join(pcapGCSPath, pcapFilename),
		"date":     date,
	}

	if err := n.ParseAndInsert(meta, pcapFilename, data); err != nil {
		t.Errorf("PCAPParser.ParseAndInsert() error = %v, wantErr %v", err, true)
	}

	if n.Accepted() != 1 {
		t.Fatal("Failed to insert snaplog data", ins)
	}
	n.Flush()

	row := ins.data[0].(*schema.PCAPRow)

	expectedParseInfo := schema.ParseInfo{
		Version:    "https://github.com/m-lab/etl/tree/foobar",
		Time:       row.Parser.Time,
		ArchiveURL: path.Join(pcapGCSPath, pcapFilename),
		Filename:   pcapFilename,
		Priority:   0,
		GitCommit:  "12345678",
	}

	expectedPCAPRow := schema.PCAPRow{
		ID:     "ndt-4c6fb_1625899199_000000000121C1A0",
		Parser: expectedParseInfo,
		Date:   date,
	}

	if diff := deep.Equal(row, &expectedPCAPRow); diff != nil {
		t.Errorf("PCAPParser.ParseAndInsert() different row: %s", strings.Join(diff, "\n"))
	}

}

func TestPCAPParser_IsParsable(t *testing.T) {
	tests := []struct {
		name     string
		testName string
		want     bool
	}{
		{
			name:     "success-pcap",
			testName: pcapFilename,
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
			data, err := ioutil.ReadFile(path.Join(`testdata/PCAP/`, tt.testName))
			if err != nil {
				t.Fatalf(err.Error())
			}
			p := &parser.PCAPParser{}
			_, got := p.IsParsable(tt.testName, data)
			if got != tt.want {
				t.Errorf("PCAPParser.IsParsable() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPCAPParser_GetUUID(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		want     string
	}{
		{
			name:     "filename-expected-format",
			filename: "2021/07/22/ndt-4c6fb_1625899199_00000000013A4623.pcap.gz",
			want:     "ndt-4c6fb_1625899199_00000000013A4623",
		},
		{
			name:     "filename-without-date-prefix",
			filename: "ndt-4c6fb_1625899199_00000000013A4623.pcap.gz",
			want:     "ndt-4c6fb_1625899199_00000000013A4623",
		},
		{
			name:     "empty-string",
			filename: "",
			want:     ".",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &parser.PCAPParser{}
			got := p.GetUUID(tt.filename)
			if got != tt.want {
				t.Errorf("PCAPParser.GetUUID() got = %v, want %v", got, tt.want)
			}
		})
	}
}