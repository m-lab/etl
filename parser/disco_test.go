package parser_test

import (
	"log"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/fake"
	"github.com/m-lab/etl/parser"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

var test_data []byte = []byte(
	`{
	"sample": [{"timestamp": 69850, "value": 0.0}, {"timestamp": 69860, "value": 0.0}],
	"metric": "switch.multicast.local.rx",
	"hostname": "mlab4.sea05.measurement-lab.org",
	"experiment": "s1.sea05.measurement-lab.org"}
	{"sample": [],
	"metric": "switch.multicast.local.rx",
	"hostname": "mlab1.sea05.measurement-lab.org",
	"experiment": "s1.sea05.measurement-lab.org"}`)

// This tests the parser, using a fake inserter, so that it runs entirely locally.
func TestJSONParsing(t *testing.T) {
	// This creates a real inserter, with a fake uploader, for local testing.
	uploader := fake.FakeUploader{}
	ins, err := bq.NewBQInserter(etl.InserterParams{
		"mlab-sandbox", "dataset", "disco_test", "", 3, 10 * time.Second, time.Second}, &uploader)
	if err != nil {
		t.Fatal(err)
	}

	var parser etl.Parser = parser.NewDiscoParser(ins)

	meta := map[string]bigquery.Value{"filename": "fake-filename.tar", "parse_time": time.Now()}
	// Should result in two tests sent to inserter, but no call to uploader.
	err = parser.ParseAndInsert(meta, "testName", test_data)
	if err != nil {
		t.Fatal(err)
	}
	if ins.Accepted() != 2 {
		t.Error("Accepted = ", ins.Accepted())
		t.Fail()
	}

	// Adds two more rows, triggering an upload of 3 rows.
	err = parser.ParseAndInsert(meta, "testName", test_data)
	if err != nil {
		t.Fatal(err)
	}
	if len(uploader.Rows) != 3 {
		t.Error("Expected 3, got", len(uploader.Rows))
	}

	// Adds two more rows, triggering an upload of 3 rows.
	err = parser.ParseAndInsert(meta, "testName", test_data)
	if err != nil {
		t.Fatal(err)
	}

	if ins.Accepted() != 6 {
		t.Error("Accepted = ", ins.Accepted())
	}
	if ins.RowsInBuffer() != 0 {
		t.Error("RowsInBuffer = ", ins.RowsInBuffer())
	}
	if len(uploader.Rows) != 3 {
		t.Error("Expected 3, got", len(uploader.Rows))
	}

	if uploader.Rows[0].Row["sample"] != nil && len(uploader.Rows[0].Row["sample"].([]bigquery.Value)) != 1 {
		t.Error("Expected 1, got", len(uploader.Rows[0].Row["sample"].([]bigquery.Value)))
	}
	if uploader.Rows[0].Row["task_filename"].(string) != "fake-filename.tar" {
		t.Error("task_filename incorrect: Expected 'fake-filename.tar', got",
			uploader.Rows[0].Row["task_filename"].(string))
	}
	if uploader.Rows[0].Row["test_id"].(string) != "testName" {
		t.Error("task_filename incorrect: Expected 'testName', got",
			uploader.Rows[0].Row["test_id"].(string))
	}
	if uploader.Rows[0].Row["metric"].(string) != "switch.multicast.local.rx" {
		t.Error("task_filename incorrect: Expected 'switch.multicast.local.rx', got",
			uploader.Rows[0].Row["metric"].(string))
	}
	if uploader.Rows[0].Row["hostname"].(string) != "mlab1.sea05.measurement-lab.org" {
		t.Error("task_filename incorrect: Expected 'mlab1.sea05.measuremet-lab.org', got",
			uploader.Rows[0].Row["hostname"].(string))
	}
	if uploader.Rows[0].Row["experiment"].(string) != "s1.sea05.measurement-lab.org" {
		t.Error("task_filename incorrect: Expected 's1.sea05.measuremet-lab.org', got",
			uploader.Rows[0].Row["experiment"].(string))
	}

	if err != nil {
		log.Printf("Request: %v\n", uploader.Request)
		log.Printf("Rows Len: %d\n", len(uploader.Rows))
		if len(uploader.Rows) > 0 {
			log.Printf("Rows[0]: %v\n", uploader.Rows[0])
			log.Printf("Rows[0]['sample']: %v\n", len(uploader.Rows[0].Row["sample"].([]bigquery.Value)))
		}
		t.Error(err)
	}
}

// DISABLED
// This tests insertion into a test table in the cloud.  Should not normally be executed.
func xTestRealBackend(t *testing.T) {
	ins, err := bq.NewInserter(etl.SW, time.Now())
	var parser etl.Parser = parser.NewDiscoParser(ins)

	meta := map[string]bigquery.Value{"filename": "filename", "parse_time": time.Now()}
	for i := 0; i < 3; i++ {
		// Iterations:
		// Add two rows, no upload.
		// Add two more rows, triggering an upload of 3 rows.
		// Add two more rows, triggering an upload of 3 rows.
		err = parser.ParseAndInsert(meta, "testName", test_data)
		if ins.Accepted() != 2 {
			t.Error("Accepted = ", ins.Accepted())
			t.Fail()
		}
	}

	if ins.Accepted() != 6 {
		t.Error("Accepted = ", ins.Accepted())
	}
	if ins.RowsInBuffer() != 0 {
		t.Error("RowsInBuffer = ", ins.RowsInBuffer())
	}

	if err != nil {
		t.Error(err)
	}
}
