package parser_test

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"path"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
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
	if n.Failed() != 0 {
		t.Fatal("Expected 0 failed rows, got", n.Failed())
	}
	n.Flush()

	// Verify that the data was parsed correctly.
	firstRow := sink.data[0].(*schema.SwitchRow)
	expected := civil.Date{Year: 2021, Month: 12, Day: 14}
	if firstRow.Date != expected {
		t.Errorf("Expected row to have date %v, got %v", expected,
			firstRow.Date)
	}
	// Check that the ID has the right prefix. Since the order of the rows
	// isn't predictable, we can't verify the timestamp.
	if !strings.HasPrefix(firstRow.ID, "mlab2-dfw07-") {
		t.Errorf("Expected row ID to start with %s, got %s",
			"mlab2-dfw07-", firstRow.ID)
	}
	// Check that there are 16 metrics in the row's raw.metrics field.
	if len(firstRow.Raw.Metrics) != 16 {
		t.Errorf("Expected 16 metrics, got %d", len(firstRow.Raw.Metrics))
	}
	// Check that local octets are correctly set to zero for this archive.
	if firstRow.A.SwitchOctetsLocalRx != 0 ||
		firstRow.A.SwitchOctetsLocalRxCounter != 0 ||
		firstRow.A.SwitchOctetsLocalTx != 0 ||
		firstRow.A.SwitchOctetsLocalTxCounter != 0 {
		t.Errorf("Expected local octets to be zero, got %v", firstRow.A)
	}

	// Test DISCOv1 format.
	sink = newInMemorySink()
	n = parser.NewSwitchParser(sink, "switch", "_suffix", &fakeAnnotator{})
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

	if n.Accepted() != 360 {
		t.Fatal("Expected 360 accepted rows, got", n.Accepted())
	}
	if n.Failed() != 0 {
		t.Fatal("Expected 0 failed rows, got", n.Failed())
	}
	n.Flush()

	// Verify that the data was parsed correctly.
	firstRow = sink.data[0].(*schema.SwitchRow)
	expected = civil.Date{Year: 2016, Month: 05, Day: 12}
	if firstRow.Date != expected {
		t.Errorf("Expected row to have date %v, got %v", expected,
			firstRow.Date)
	}
	// Check that the ID has the right prefix. Since the order of the rows
	// isn't predictable, we can't verify the timestamp.
	if !strings.HasPrefix(firstRow.ID, "mlab3-svg01-") {
		t.Errorf("Expected row ID to start with %s, got %s",
			"mlab3-svg01-", firstRow.ID)
	}
	// Check that there are 24 metrics in the row's raw.metrics field.
	if len(firstRow.Raw.Metrics) != 24 {
		t.Errorf("Expected 24 metrics, got %d", len(firstRow.Raw.Metrics))
	}
	// Check that local octets are non-zero for this archive. DISCOv1 did not
	// include counters.
	if firstRow.A.SwitchOctetsLocalRx == 0 ||
		firstRow.A.SwitchOctetsLocalTx == 0 {
		t.Errorf("Expected local octets to be non-zero, got %d %d",
			firstRow.A.SwitchOctetsLocalRx, firstRow.A.SwitchOctetsLocalTx)
	}
}
