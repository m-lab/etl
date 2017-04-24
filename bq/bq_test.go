package bq_test

import (
	"log"
	"testing"
	"time"

	"github.com/m-lab/etl/fake"
	"github.com/m-lab/etl/intf"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// Item represents a row item.
type Item struct {
	Name   string
	Count  int
	Foobar int `json:"foobar"`
}

func TestInsert(t *testing.T) {
	tag := "new"
	items := []interface{}{
		Item{Name: tag + "_x0", Count: 17, Foobar: 44},
		Item{Name: tag + "_x1", Count: 12, Foobar: 44}}

	in, err := fake.NewFakeInserter(
		intf.InserterParams{"mlab-sandbox", "mlab_sandbox", "test2", 10 * time.Second, 1})
	if err != nil {
		t.Fatal(err)
	}

	if err = in.InsertRow(Item{Name: tag + "_x0", Count: 17, Foobar: 44}); err != nil {
		t.Error(err)
	}
	if err = in.InsertRows(items); err != nil {
		t.Error(err)
	}
	// TODO - uncomment when this bug is resolved.
	//if in.Count() != 2 {
	//	t.Error("Should have inserted two rows")
	//}
	in.Flush()
}

func TestFlushing(t *testing.T) {
	var items []interface{}
	items = append(items, Item{Name: "x1", Count: 17, Foobar: 44})
	items = append(items, Item{Name: "x2", Count: 12, Foobar: 44})

	in, err := fake.NewFakeInserter(
		intf.InserterParams{"mlab-sandbox", "mlab_sandbox", "test2", 10 * time.Second, 3})
	if err != nil {
		log.Printf("%v\n", err)
		t.Fatal()
	}

	if err = in.InsertRow(Item{Name: "x0", Count: 17, Foobar: 44}); err != nil {
		t.Fatal()
	}
	if in.RowsInBuffer() != 1 {
		t.Error("RowsInBuffer = ", in.RowsInBuffer())
	}
	if err = in.InsertRows(items); err != nil {
		t.Fatal()
	}
	if in.RowsInBuffer() != 0 {
		t.Error("RowsInBuffer = ", in.RowsInBuffer())
	}
	if in.Count() != 3 {
		t.Error("Count = ", in.Count())
	}
	if err = in.InsertRows(items); err != nil {
		t.Fatal()
	}
	if in.RowsInBuffer() != 2 {
		t.Error("RowsInBuffer = ", in.RowsInBuffer())
	}
	if in.Count() != 5 {
		t.Error("Count = ", in.Count())
	}
	if err = in.InsertRows(items); err != nil {
		t.Fatal()
	}
	if in.RowsInBuffer() != 1 {
		t.Error("RowsInBuffer = ", in.RowsInBuffer())
	}
	if in.Count() != 7 {
		t.Error("Count = ", in.Count())
	}
	in.Flush()
	if in.RowsInBuffer() != 0 {
		t.Error("RowsInBuffer = ", in.RowsInBuffer())
	}
	if in.Count() != 7 {
		t.Error("Count = ", in.Count())
	}

}
