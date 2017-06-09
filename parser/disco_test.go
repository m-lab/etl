package parser_test

import (
	"fmt"
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

// PrintingInserter prints out the items passed in.
// Inject into Parser for testing.
type PrintingInserter struct {
	bq.NullInserter
}

func (ti *PrintingInserter) InsertRow(data interface{}) error {
	fmt.Printf("%T: %v\n", data, data)
	return nil
}
func (ti *PrintingInserter) Flush() error {
	return nil
}

var test_data []byte = []byte(`{
	"sample": [{"timestamp": 69850, "value": 0.0}, {"timestamp": 69860, "value": 0.0}],
	"metric": "switch.multicast.local.rx",
	"hostname": "mlab4.sea05.measurement-lab.org",
	"experiment": "s1.sea05.measurement-lab.org"}
	{"sample": [{"timestamp": 69870, "value": 0.0}, {"timestamp": 69880, "value": 0.0}],
	"metric": "switch.multicast.local.rx",
	"hostname": "mlab1.sea05.measurement-lab.org",
	"experiment": "s1.sea05.measurement-lab.org"}`)

// This tests the parser, using a fake inserter, so that it runs entirely locally.
func TestJSONParsing(t *testing.T) {
	// This creates a real inserter, with a fake uploader, for local testing.
	uploader := fake.FakeUploader{}
	ins, err := bq.NewBQInserter(etl.InserterParams{
		"mlab_sandbox", "disco_test", "", 10 * time.Second, 3}, &uploader)

	var parser etl.Parser = parser.NewDiscoParser(ins)

	meta := map[string]bigquery.Value{"filename": "filename", "parsetime": time.Now()}
	// Should result in two tests sent to inserter, but no call to uploader.
	err = parser.ParseAndInsert(meta, "testName", test_data)
	if ins.Accepted() != 2 {
		t.Error("Accepted = ", ins.Accepted())
		t.Fail()
	}

	// Adds two more rows, triggering an upload of 3 rows.
	err = parser.ParseAndInsert(meta, "testName", test_data)
	if len(uploader.Rows) != 3 {
		t.Error("Uploader Row Count = ", len(uploader.Rows))
	}

	// Adds two more rows, triggering an upload of 3 rows.
	err = parser.ParseAndInsert(meta, "testName", test_data)

	if ins.Accepted() != 6 {
		t.Error("Accepted = ", ins.Accepted())
	}
	if ins.RowsInBuffer() != 0 {
		t.Error("RowsInBuffer = ", ins.RowsInBuffer())
	}
	if len(uploader.Rows) != 3 {
		t.Error("Uploader Row Count = ", len(uploader.Rows))
	}

	if err != nil {
		log.Printf("%v\n", uploader.Request)
		log.Printf("%d Rows\n", len(uploader.Rows))
		log.Printf("%v\n", uploader.Rows[0])
		t.Error(err)
	}
}

// DISABLED
// This tests insertion into a test table in the cloud.  Should not normally be executed.
func xTestRealBackend(t *testing.T) {
	ins, err := bq.NewInserter("mlab_sandbox", etl.SW, time.Now())
	var parser etl.Parser = parser.NewDiscoParser(ins)

	meta := map[string]bigquery.Value{"filename": "filename", "parsetime": time.Now()}
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
