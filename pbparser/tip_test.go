package pbparser_test

import (
	"io/ioutil"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/pbparser"
)

func assertInserter(in etl.Inserter) {
	func(in etl.Inserter) {}(&inMemoryInserter{})
}

type inMemoryInserter struct {
	data      []interface{}
	committed int
	failed    int
}

func newInMemoryInserter() *inMemoryInserter {
	data := make([]interface{}, 0)
	return &inMemoryInserter{data, 0, 0}
}

func (in *inMemoryInserter) Put(data []interface{}) error {
	in.data = append(in.data, data...)
	in.committed = len(in.data)
	return nil
}

func (in *inMemoryInserter) PutAsync(data []interface{}) {
	in.data = append(in.data, data...)
	in.committed = len(in.data)
}

func (in *inMemoryInserter) InsertRow(data interface{}) error {
	in.data = append(in.data, data)
	return nil
}
func (in *inMemoryInserter) InsertRows(data []interface{}) error {
	in.data = append(in.data, data...)
	return nil
}
func (in *inMemoryInserter) Flush() error {
	in.committed = len(in.data)
	return nil
}
func (in *inMemoryInserter) TableBase() string {
	return "ndt_test"
}
func (in *inMemoryInserter) TableSuffix() string {
	return ""
}
func (in *inMemoryInserter) FullTableName() string {
	return "ndt_test"
}
func (in *inMemoryInserter) Dataset() string {
	return ""
}
func (in *inMemoryInserter) Project() string {
	return ""
}
func (in *inMemoryInserter) RowsInBuffer() int {
	return len(in.data) - in.committed
}
func (in *inMemoryInserter) Accepted() int {
	return len(in.data)
}
func (in *inMemoryInserter) Committed() int {
	return in.committed
}
func (in *inMemoryInserter) Failed() int {
	return in.failed
}

func TestInserter(t *testing.T) {
	ins := &inMemoryInserter{}
	n := pbparser.NewTCPInfoParser(ins)
	filename := "testdata/20180607Z153856.193U00000000L2620:0:1003:415:b33e:9d6a:81bf:87a1:36032R2607:f8b0:400d:c0d::81:5034_00000.zst"
	rawData, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatalf("cannot read testdata.")
	}

	meta := map[string]bigquery.Value{"filename": filename}
	err = n.ParseAndInsert(meta, filename, rawData)
	if err != nil {
		t.Fatalf(err.Error())
	}
	n.Flush()
	if ins.Committed() != 17 {
		t.Fatalf("Expected %d, Got %d.", 6, ins.Committed())
	}

	if len(ins.data) < 1 {
		t.Fatal("Should have at least one inserted row")
	}
	inserted := ins.data[0].(map[string]bigquery.Value)
	if time.Unix(0, inserted["parse_time"].(int64)).After(time.Now()) {
		t.Error("Should have inserted parse_time", inserted["parse_time"])
	}
	if inserted["task_filename"].(string) != filename {
		t.Error("Should have correct filename", filename, "!=", inserted["task_filename"])
	}
}
