package parser_test

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"path"
	"testing"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/go/rtx"
)

const (
	switchDISCOv1Filename = "discov1-switch.json.gz"
	switchDISCOv2Filename = "discov2-switch.jsonl"
	switchGCSPath         = "gs://archive-measurement-lab/utilization/switch/2021/12/14/"
)

func TestSwitchParser_ParseAndInsert(t *testing.T) {
	sink := newInMemorySink()
	n := parser.NewSwitchParser(sink, "switch", "_suffix", &fakeAnnotator{})

	// Test DISCOv2 format.
	data, err := ioutil.ReadFile(path.Join("testdata/Switch/", switchDISCOv2Filename))
	rtx.Must(err, "failed to load DISCOv2 test file")

	date := civil.Date{Year: 2021, Month: 12, Day: 14}

	meta := map[string]bigquery.Value{
		"filename": path.Join(switchGCSPath, switchDISCOv2Filename),
		"date":     date,
	}

	if err := n.ParseAndInsert(meta, switchDISCOv2Filename, data); err != nil {
		t.Errorf("SwitchParser.ParseAndInsert() error = %v, wantErr %v", err, true)
	}

	if n.Accepted() != 30 {
		t.Fatal("Expected 30 accepted rows, got", n.Accepted())
	}
	n.Flush()

	// Test DISCOv1 format.
	// This is a gzip-compressed JSONL file.
	gzipData, err := ioutil.ReadFile(path.Join("testdata/Switch/", switchDISCOv1Filename))
	rtx.Must(err, "failed to load DISCOv1 test file")

	reader, err := gzip.NewReader(bytes.NewReader(gzipData))
	rtx.Must(err, "failed to create gzip reader")

	data, err = ioutil.ReadAll(reader)
	rtx.Must(err, "failed to read from gzip stream")

	date = civil.Date{Year: 2016, Month: 05, Day: 12}

	meta = map[string]bigquery.Value{
		"filename": path.Join(switchGCSPath, switchDISCOv1Filename),
		"date":     date,
	}

	if err := n.ParseAndInsert(meta, switchDISCOv1Filename, data); err != nil {
		t.Errorf("SwitchParser.ParseAndInsert() error = %v, wantErr %v", err, true)
	}

	if n.Accepted() != 390 {
		t.Fatal("Expected 390 accepted rows, got", n.Accepted())
	}
	n.Flush()
}
