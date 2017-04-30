package parser

// TODO - use parser_test, to force proper package isolation.

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/m-lab/etl/bq"

	"cloud.google.com/go/bigquery"
)

func TestNDTParser(t *testing.T) {
	// Load test data.
	rawData, err := ioutil.ReadFile("testdata/c2s_snaplog")
	if err != nil {
		t.Fatalf(err.Error())
	}

	ins := &inMemoryInserter{}
	n := &NDTParser{inserter: ins, tmpDir: "./", tableName: "ndt_table"}
	err = n.ParseAndInsert(nil, "filename.c2s_snaplog", rawData)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if ins.RowsInBuffer() != 1 {
		t.Fatalf("Failed to insert snaplog data.")
	}

	results := ins.data[0].(*bq.MapSaver).Values
	// TODO(dev): find a better way to verify the returned values are correct.
	expectedValues := map[string]bigquery.Value{
		"web100_log_entry_version":                    "2.5.27 201001301335 net100",
		"web100_log_entry_snap_RemAddress":            "45.56.98.222",
		"web100_log_entry_connection_spec_local_port": int64(43685),
	}
	for key, value := range expectedValues {
		// Raw bigquery.Value instances do not compare.
		if results[key] != value {
			t.Errorf("Wrong value for %q: got %q; want %q", key, str(results[key]), str(value))
		}
	}
	// TODO(dev): remove print of entire snaplog.
	// prettyPrint(results)
}

// TODO(dev): is there a better way to display these values?
func str(v bigquery.Value) string {
	switch t := v.(type) {
	case string:
		s := v.(string)
		return s
	case int64:
		i := v.(int64)
		return fmt.Sprintf("%d", i)
	default:
		return fmt.Sprintf("Unexpected<%T>", t)
	}
}

func prettyPrint(results map[string]bigquery.Value) {
	b, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Print(string(b))
}

type inMemoryInserter struct {
	data []interface{}
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
	return nil
}
func (in *inMemoryInserter) TableName() string {
	return ""
}
func (in *inMemoryInserter) Dataset() string {
	return ""
}
func (in *inMemoryInserter) RowsInBuffer() int {
	return len(in.data)
}
func (in *inMemoryInserter) Count() int {
	return len(in.data)
}
