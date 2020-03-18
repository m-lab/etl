package bq_test

import (
	"errors"
	"log"
	"net/url"
	"testing"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"

	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/fake"
	"github.com/m-lab/etl/row"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func assertSink(in row.Sink) {
	func(in row.Sink) {}(&bq.BQInserter{})
}

//==================================================================================

func TestGKEBasicInsert(t *testing.T) {
	tag := "new"
	items := []interface{}{
		Item{Name: tag + "_x0", Count: 17, Foobar: 44},
		Item{Name: tag + "_x1", Count: 12, Foobar: 44}}

	u := fake.NewFakeUploader()
	in, err := bq.NewColumnPartitionedInserter(etl.NDT5, u)
	if err != nil {
		t.Fatal(err)
	}

	if _, err = in.Commit([]interface{}{Item{Name: tag + "_x0", Count: 17, Foobar: 44}}, "basic"); err != nil {
		t.Error(err)
	}
	if _, err = in.Commit(items, "Basic"); err != nil {
		t.Error(err)
	}

	if u.CallCount != 2 {
		t.Error("Should have called inserter twice", u.CallCount)
	}
	if u.Total != 3 {
		t.Error("Should have added 3 rows", u.Total)
	}
}

// Just manual testing for now - need to assert something useful.
func TestGKEHandleInsertErrors(t *testing.T) {
	u := fake.NewFakeUploader()
	in, err := bq.NewColumnPartitionedInserter(etl.NDT5, u)
	if err != nil {
		t.Fatal(err)
	}
	tag := "new"
	items := []interface{}{
		Item{Name: tag + "_x0", Count: 17, Foobar: 44},
		Item{Name: tag + "_x1", Count: 12, Foobar: 44}}

	rie := bigquery.RowInsertionError{InsertID: "1234", RowIndex: 123}
	var bqe bigquery.Error
	bqe.Location = "location"
	bqe.Message = "message"
	bqe.Reason = "invalid"
	// This is a little wierd.  MultiError we receive from insert contain
	// *bigquery.Error.  So that is what we test here.
	rie.Errors = append(rie.Errors, &bqe)
	u.SetErr(&rie)
	n, err := in.Commit(items, "test")
	if n != 0 {
		t.Fatal(err, n)
	}
	if err != nil {
		t.Error(err)
	}

	var pme bigquery.PutMultiError
	pme = append(pme, rie)
	u.SetErr(&pme)
	n, err = in.Commit(items, "test")
	if n != 0 {
		t.Fatal(err, n)
	}
	if err != nil {
		// TODO for now Commit never returns error, but that may not be what we want.
		t.Error(err)
	}

	if u.CallCount != 2 {
		t.Errorf("Expected %d calls, got %d\n", 2, u.CallCount)
	}
	if len(u.Rows) != 0 {
		t.Errorf("Expected %d rows, got %d\n", 0, len(u.Rows))
	}
}

func xTestHandleRequestTooLarge(t *testing.T) {
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
	// Should see two fails, and three successes.
	if fakeUploader.CallCount != 5 {
		t.Errorf("Expected %d calls, got %d\n", 5, fakeUploader.CallCount)
	}
	if bqi.Committed() != 5 {
		t.Error("Lost rows:", bqi.Committed())
	}
	if bqi.Failed() > 0 {
		t.Errorf("Lost rows: %+v", bqi)
	}
}

func xTestQuotaError(t *testing.T) {

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
func xTestUpdateMetrics(t *testing.T) {
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
