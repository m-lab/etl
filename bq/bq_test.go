package bq_test

import (
	"errors"
	"log"
	"net/url"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"

	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/fake"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func assertInserter(in etl.Inserter) {
	func(in etl.Inserter) {}(&bq.BQInserter{})
}

func foobar(vs bigquery.ValueSaver) {
	_, _, _ = vs.Save()
}

func TestMapSaver(t *testing.T) {
	fns := bq.MapSaver{"filename": "foobar"}
	foobar(&fns)
}

// Item represents a row item.
type Item struct {
	Name   string
	Count  int
	Foobar int `json:"foobar"`
}

func standardInsertParams(bufferSize int) etl.InserterParams {
	return etl.InserterParams{
		Project: "mlab-testing", Dataset: "dataset", Table: "table",
		Suffix:        "",
		BufferSize:    bufferSize,
		PutTimeout:    10 * time.Second,
		MaxRetryDelay: 1 * time.Second,
	}
}

//==================================================================================
// These tests hit the backend, to verify expected behavior of table creation and
// access to partitions.  They deliberately have a leading "x" to prevent running
// them in Travis.  We need to find a better way to control whether they run or
// not.
//==================================================================================
func xTestRealPartitionInsert(t *testing.T) {
	tag := "new"
	items := []interface{}{
		Item{Name: tag + "_x0", Count: 17, Foobar: 44},
		Item{Name: tag + "_x1", Count: 12, Foobar: 44}}

	params := standardInsertParams(1)
	params.Suffix = "_20160201"
	in, err := bq.NewBQInserter(params, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err = in.InsertRow(Item{Name: tag + "_x0", Count: 17, Foobar: 44}); err != nil {
		t.Error(err)
	}
	if err = in.InsertRows(items); err != nil {
		t.Error(err)
	}

	if in.Accepted() != 3 {
		t.Error("Should have accepted three rows")
	}
	in.Flush()
}

//==================================================================================

func TestBasicInsert(t *testing.T) {
	tag := "new"
	items := []interface{}{
		Item{Name: tag + "_x0", Count: 17, Foobar: 44},
		Item{Name: tag + "_x1", Count: 12, Foobar: 44}}

	params := standardInsertParams(1)
	in, err := bq.NewBQInserter(params, fake.NewFakeUploader())
	if err != nil {
		t.Fatal(err)
	}

	if err = in.InsertRow(Item{Name: tag + "_x0", Count: 17, Foobar: 44}); err != nil {
		t.Error(err)
	}
	if err = in.InsertRows(items); err != nil {
		t.Error(err)
	}

	if in.Accepted() != 3 {
		t.Error("Should have accepted three rows")
	}
	in.Flush()
}

// This does not currently work with mlab-testing credentials, and we
// probably don't want it to.  So disabling it in travis.
func TestInsertConfig(t *testing.T) {
	_, isTravis := os.LookupEnv("TRAVIS")
	if isTravis {
		return
	}
	os.Setenv("GCLOUD_PROJECT", "mlab-oti")
	in, err := bq.NewInserter(etl.SS, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if in.Dataset() != "base_tables" {
		t.Errorf("Want base_tables, got %s", in.Dataset())
	}
	if in.Project() != "mlab-oti" {
		t.Errorf("Want mlab-oti, got %s", in.Project())
	}

	in, err = bq.NewInserter(etl.NDT, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if in.Dataset() != "base_tables" {
		t.Errorf("Want base_tables, got %s", in.Dataset())
	}
	if in.Project() != "mlab-oti" {
		t.Errorf("Want mlab-oti, got %s", in.Project())
	}
}

func TestBufferingAndFlushing(t *testing.T) {
	var items []interface{}
	items = append(items, Item{Name: "x1", Count: 17, Foobar: 44})
	items = append(items, Item{Name: "x2", Count: 12, Foobar: 44})

	// Set up an Inserter with a fake Uploader backend for testing.
	// Buffer 3 rows, so that we can test the buffering.
	in, err := bq.NewBQInserter(standardInsertParams(3), fake.NewFakeUploader())
	if err != nil {
		log.Printf("%v\n", err)
		t.Fatal()
	}

	// Try inserting a single row.  Should stay in the buffer.
	if err = in.InsertRow(Item{Name: "x0", Count: 17, Foobar: 44}); err != nil {
		t.Fatal()
	}
	if in.RowsInBuffer() != 1 {
		t.Error("RowsInBuffer = ", in.RowsInBuffer())
	}

	// Insert two more rows, which should trigger a flush.
	if err = in.InsertRows(items); err != nil {
		t.Fatal()
	}
	if in.RowsInBuffer() != 0 {
		t.Error("RowsInBuffer = ", in.RowsInBuffer())
	}
	if in.Accepted() != 3 {
		t.Error("Accepted = ", in.Accepted())
	}

	// Insert two more rows, which should NOT trigger a flush.
	if err = in.InsertRows(items); err != nil {
		t.Fatal()
	}
	if in.RowsInBuffer() != 2 {
		t.Error("RowsInBuffer = ", in.RowsInBuffer())
	}
	if in.Accepted() != 5 {
		t.Error("Accepted = ", in.Accepted())
	}

	// Insert two more rows, which should trigger a flush, and leave one
	// row in the buffer.
	if err = in.InsertRows(items); err != nil {
		t.Fatal()
	}
	if in.RowsInBuffer() != 1 {
		t.Error("RowsInBuffer = ", in.RowsInBuffer())
	}
	if in.Accepted() != 7 {
		t.Error("Accepted = ", in.Accepted())
	}

	// Flush the final row.
	in.Flush()
	if in.RowsInBuffer() != 0 {
		t.Error("RowsInBuffer = ", in.RowsInBuffer())
	}
	if in.Accepted() != 7 {
		t.Error("Count = ", in.Accepted())
	}

}

// RowBuffer for SS.
type RowBuffer struct {
	bufferSize int
	rows       []interface{}
}

// AddRow simply inserts a row into the buffer.  Returns error if buffer is full.
// Not threadsafe.  Should only be called by owning thread.
func (buf *RowBuffer) AddRow(row struct{}) error {
	for len(buf.rows) >= buf.bufferSize-1 {
		return etl.ErrBufferFull
	}
	buf.rows = append(buf.rows, &row)
	return nil
}

func (buf *RowBuffer) TakeRows() []interface{} {
	res := buf.rows
	buf.rows = make([]interface{}, 0, buf.bufferSize)
	return res
}

// Just manual testing for now - need to assert something useful.
func TestHandleInsertErrors(t *testing.T) {
	fakeUploader := fake.NewFakeUploader()
	buf := RowBuffer{5, make([]interface{}, 0, 5)}
	in, e := bq.NewBQInserter(standardInsertParams(5), fakeUploader)
	if e != nil {
		log.Printf("%v\n", e)
		t.Fatal()
	}

	rie := bigquery.RowInsertionError{InsertID: "1234", RowIndex: 123}
	var bqe bigquery.Error
	bqe.Location = "location"
	bqe.Message = "message"
	bqe.Reason = "invalid"
	// This is a little wierd.  MultiError we receive from insert contain
	// *bigquery.Error.  So that is what we test here.
	rie.Errors = append(rie.Errors, &bqe)
	fakeUploader.SetErr(&rie)
	buf.AddRow(struct{}{})
	in.Put(buf.TakeRows())
	if in.Failed() != 1 {
		t.Error()
	}

	var pme bigquery.PutMultiError
	pme = append(pme, rie)
	fakeUploader.SetErr(&pme)
	buf.AddRow(struct{}{})
	buf.AddRow(struct{}{})
	in.Put(buf.TakeRows())

	if fakeUploader.CallCount != 2 {
		t.Errorf("Expected %d calls, got %d\n", 2, fakeUploader.CallCount)
	}
	if len(fakeUploader.Rows) != 0 {
		t.Errorf("Expected %d rows, got %d\n", 0, len(fakeUploader.Rows))
	}
	if in.Failed() != 3 {
		t.Error()
	}
}

func TestHandleRequestTooLarge(t *testing.T) {
	fakeUploader := fake.NewFakeUploader()
	fakeUploader.RejectIfMoreThan = 2
	bqi, e := bq.NewBQInserter(standardInsertParams(5), fakeUploader)
	if e != nil {
		log.Printf("%v\n", e)
		t.Fatal()
	}

	// These don't have to implement saver as long as Err is being set,
	// and flush is not autotriggering more than once.
	var items []interface{}
	items = append(items, Item{Name: "x1", Count: 17, Foobar: 44})
	items = append(items, Item{Name: "x2", Count: 12, Foobar: 44})
	items = append(items, Item{Name: "x3", Count: 12, Foobar: 44})
	items = append(items, Item{Name: "x4", Count: 12, Foobar: 44})
	items = append(items, Item{Name: "x5", Count: 12, Foobar: 44})

	bqi.InsertRows(items)
	bqi.Flush()
	if bqi.Committed() != 5 {
		t.Error("Lost rows:", bqi.Committed())
	}
	if bqi.Failed() > 0 {
		t.Errorf("Lost rows: %+v", bqi)
	}
}

func TestQuotaError(t *testing.T) {

	// Set up an Inserter with a fake Uploader backend for testing.
	// Buffer 5 rows, so that we can test the buffering.
	fakeUploader := fake.NewFakeUploader()
	in, e := bq.NewBQInserter(standardInsertParams(5), fakeUploader)
	if e != nil {
		log.Printf("%v\n", e)
		t.Fatal()
	}

	var items []interface{}
	items = append(items, Item{Name: "x1", Count: 17, Foobar: 44})
	items = append(items, Item{Name: "x2", Count: 12, Foobar: 44})

	// Insert two rows.
	if err := in.InsertRows(items); err != nil {
		t.Fatal()
	}
	if in.Accepted() != 2 {
		t.Error("Accepted = ", in.Accepted())
	}

	// Set up an arbitrary error and ensure it causes a failure.
	fakeUploader.SetErr(errors.New("Foobar"))
	err := in.Flush()

	if err != nil {
		t.Error("Error should have been consumed.")
	}
	if fakeUploader.CallCount != 1 {
		t.Error("Call count should be 1: ", fakeUploader.CallCount)
	}
	if in.Failed() != 2 {
		// Should have failed two rows.
		t.Error("Should have increased Failed to 2: ", in.Failed())
	}

	// Insert the rows again, since they were lost in previous Flush call.
	if err = in.InsertRows(items); err != nil {
		t.Fatal()
	}
	if in.Accepted() != 4 {
		t.Error("Accepted = ", in.Accepted())
	}
	// This should fail, because the uploader has a preloaded error.
	fakeUploader.SetErr(errors.New("Quota exceeded:"))
	err = in.Flush()

	if err != nil {
		t.Error("Error should have been consumed.")
	}
	// The Quota exceeded error should have caused a retry, resulting in 2 additional calls.
	if fakeUploader.CallCount != 3 {
		t.Error("Call count should be 3: ", fakeUploader.CallCount)
	}
	// The second call should have succeeded, increasing the Committed count.
	if in.Committed() != 2 {
		t.Error("Should have increased Committed to 2: ", in.Committed())
	}

	if fakeUploader.CallCount != 3 {
		t.Errorf("Expected %d calls, got %d\n", 3, fakeUploader.CallCount)
	}
	if len(fakeUploader.Rows) != 2 {
		t.Errorf("Expected %d rows, got %d\n", 2, len(fakeUploader.Rows))
	}
}

// This isn't particularly thorough, but it exercises the various error handling paths
// to ensure there aren't any panics.
func TestUpdateMetrics(t *testing.T) {
	fakeUploader := fake.NewFakeUploader()
	in, e := bq.NewBQInserter(standardInsertParams(20), fakeUploader)
	if e != nil {
		log.Printf("%v\n", e)
		t.Fatal()
	}
	bqi := in.(*bq.BQInserter)

	// These don't have to implement saver as long as Err is being set,
	// and flush is not autotriggering more than once.
	var items []interface{}
	items = append(items, Item{Name: "x1", Count: 17, Foobar: 44})
	items = append(items, Item{Name: "x2", Count: 12, Foobar: 44})

	fakeUploader.SetErr(make(bigquery.PutMultiError, 2))
	bqi.InsertRows(items)
	bqi.Flush()
	if bqi.Failed() != 2 {
		t.Error(in)
	}

	// Try adding 11 rows, with a PutMultiError on all rows.
	fakeUploader.SetErr(make(bigquery.PutMultiError, 11))
	bqi.InsertRows(make([]interface{}, 11))
	bqi.Flush()
	// There should now be 13 failed rows.
	if bqi.Failed() != 13 {
		t.Error(in)
	}

	// Try adding 1 row with a simple error.
	fakeUploader.SetErr(&url.Error{Err: errors.New("random error")})
	bqi.InsertRows(make([]interface{}, 1))
	bqi.Flush()
	// There should now be 14 failures.
	if bqi.Failed() != 14 {
		t.Error(in)
	}

	// Try adding 1 row with a googleapi.Error.
	fakeUploader.SetErr(&googleapi.Error{Code: 404})
	bqi.InsertRows(make([]interface{}, 1))
	bqi.Flush()
	// Should now be 15 failures.
	if bqi.Failed() != 15 || bqi.Committed() != 0 {
		t.Error(in)
	}

}
