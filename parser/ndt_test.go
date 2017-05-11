package parser_test

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/parser"

	"github.com/kr/pretty"

	"cloud.google.com/go/bigquery"
)

func TestNDTParser(t *testing.T) {
	// Load test data.
	rawData, err := ioutil.ReadFile("testdata/c2s_snaplog")
	if err != nil {
		t.Fatalf(err.Error())
	}

	ins := &inMemoryInserter{}
	parser.TmpDir = "./"
	n := parser.NewNDTParser(ins)
	err = n.ParseAndInsert(nil, "filename.c2s_snaplog", rawData)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if ins.RowsInBuffer() != 1 {
		t.Fatalf("Failed to insert snaplog data.")
	}

	// Extract the values saved to the inserter.
	actualValues := ins.data[0].(*bq.MapSaver).Values
	expectedValues := map[string]bigquery.Value{
		"web100_log_entry": map[string]bigquery.Value{
			"version": "2.5.27 201001301335 net100",
			"snap": map[string]bigquery.Value{
				"RemAddress": "45.56.98.222",
			},
			"connection_spec": map[string]bigquery.Value{
				"local_port": int64(43685),
			},
		},
	}
	if !compare(t, actualValues, expectedValues) {
		t.Errorf("Missing expected values:")
		t.Errorf(pretty.Sprint(expectedValues))
	}
}

// compare recursively checks whether actual values equal values in the expected values.
// The expected values may be a subset of the actual values, but not a superset.
func compare(t *testing.T, actual map[string]bigquery.Value, expected map[string]bigquery.Value) bool {
	match := true
	for key, value := range expected {
		switch v := value.(type) {
		case map[string]bigquery.Value:
			match = match && compare(t, actual[key].(map[string]bigquery.Value), v)
		case string:
			if actual[key].(string) != v {
				t.Logf("Wrong strings for key %q: got %q; want %q", key, v, actual[key].(string))
				match = false
			}
		case int64:
			if actual[key].(int64) != v {
				t.Logf("Wrong ints for key %q: got %d; want %d", key, v, actual[key].(int64))
				match = false
			}
		case int32:
			if actual[key].(int32) != v {
				t.Logf("Wrong ints for key %q: got %d; want %d", key, v, actual[key].(int32))
				match = false
			}
		case int:
			if actual[key].(int) != v {
				t.Logf("Wrong ints for key %q: got %d; want %d", key, v, actual[key].(int))
				match = false
			}
		case []float64:
			fmt.Println(v)
			if len(v) != len(actual[key].([]float64)) {
				t.Logf("Wrong floats for key %q: got %d; want %d", key, v, actual[key].([]float64))
				match = false
			}
			for i := range v {
				if v[i] != actual[key].([]float64)[i] {
					t.Logf("Wrong floats for key %q: got %d; want %d", key, v, actual[key].([]float64))
					match = false
				}
			}

		default:
			fmt.Printf("Unsupported type. %T\n", v)
			panic(nil)
		}
	}
	return match
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
	return "ndt_test"
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
