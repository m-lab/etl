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

// A handful of file names from a single ndt tar file.
var testFileNames []string = []string{
	`20170509T00:05:13.863119000Z_45.56.98.222.c2s_ndttrace`,
	`20170509T00:05:13.863119000Z_45.56.98.222.s2c_ndttrace`,
	`20170509T00:05:13.863119000Z_eb.measurementlab.net:40074.s2c_snaplog`,
	`20170509T00:05:13.863119000Z_eb.measurementlab.net:43628.c2s_snaplog`,
	`20170509T00:05:13.863119000Z_eb.measurementlab.net:56986.cputime`,
	`20170509T00:05:13.863119000Z_eb.measurementlab.net:56986.meta`,
	`20170509T00:14:43.498114000Z_77.95.64.13.c2s_ndttrace`,
	`20170509T00:14:43.498114000Z_77.95.64.13.s2c_ndttrace`,
	`20170509T00:14:43.498114000Z_vm-jcanat-measures.rezopole.net:37625.c2s_snaplog`,
	`20170509T00:14:43.498114000Z_vm-jcanat-measures.rezopole.net:43519.s2c_snaplog`,
	`20170509T00:14:43.498114000Z_vm-jcanat-measures.rezopole.net:55712.cputime`,
	`20170509T00:14:43.498114000Z_vm-jcanat-measures.rezopole.net:55712.meta`,
	`20170509T00:15:13.652804000Z_45.56.98.222.c2s_ndttrace`,
	`20170509T00:15:13.652804000Z_45.56.98.222.s2c_ndttrace`,
	`20170509T00:15:13.652804000Z_eb.measurementlab.net:54794.s2c_snaplog`,
	`20170509T00:15:13.652804000Z_eb.measurementlab.net:55544.cputime`,
	`20170509T00:15:13.652804000Z_eb.measurementlab.net:55544.meta`,
	`20170509T00:15:13.652804000Z_eb.measurementlab.net:56700.c2s_snaplog`,
	`20170509T00:25:13.399280000Z_45.56.98.222.c2s_ndttrace`,
	`20170509T00:25:13.399280000Z_45.56.98.222.s2c_ndttrace`,
	`20170509T00:25:13.399280000Z_eb.measurementlab.net:51680.cputime`,
	`20170509T00:25:13.399280000Z_eb.measurementlab.net:51680.meta`,
	`20170509T00:25:13.399280000Z_eb.measurementlab.net:53254.s2c_snaplog`,
	`20170509T00:25:13.399280000Z_eb.measurementlab.net:57528.c2s_snaplog`,
	`20170509T00:35:13.681547000Z_45.56.98.222.c2s_ndttrace`,
	`20170509T00:35:13.681547000Z_45.56.98.222.s2c_ndttrace`,
	`20170509T00:35:13.681547000Z_eb.measurementlab.net:38296.s2c_snaplog`}

func TestValidation(t *testing.T) {
	for _, test := range testFileNames {
		_, err := parser.ParseNDTFileName(test)
		if err != nil {
			t.Error(err)
		}
	}
}

func TestNDTParser(t *testing.T) {
	// Load test data.
	ins := &inMemoryInserter{}
	parser.TmpDir = "./"
	n := parser.NewNDTParser(ins)

	// TODO(prod) - why are so many of the tests to this endpoint and a few others?
	s2cName := `20170509T13:45:13.590210000Z_eb.measurementlab.net:44160.s2c_snaplog`
	s2cData, err := ioutil.ReadFile(`testdata/` + s2cName)
	if err != nil {
		t.Fatalf(err.Error())
	}

	meta := map[string]bigquery.Value{"filename": "tarfile.tgz"}
	err = n.ParseAndInsert(meta, s2cName, s2cData)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if ins.RowsInBuffer() != 0 {
		t.Fatalf("Data processed prematurely.")
	}

	metaName := `20170509T13:45:13.590210000Z_eb.measurementlab.net:53000.meta`
	metaData, err := ioutil.ReadFile(`testdata/` + metaName)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = n.ParseAndInsert(meta, metaName, metaData)
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
				"local_port": int64(40105),
			},
		},
	}
	if !compare(t, actualValues, expectedValues) {
		t.Errorf("Missing expected values:")
		t.Errorf(pretty.Sprint(expectedValues))
	}

	c2sName := `20170509T13:45:13.590210000Z_eb.measurementlab.net:48716.c2s_snaplog`
	c2sData, err := ioutil.ReadFile(`testdata/` + c2sName)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = n.ParseAndInsert(meta, c2sName, c2sData)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if ins.RowsInBuffer() != 2 {
		t.Fatalf("Failed to insert snaplog data.")
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
				t.Logf("Wrong strings for key %q: got %q; want %q",
					key, v, actual[key].(string))
				match = false
			}
		case int64:
			if actual[key].(int64) != v {
				t.Logf("Wrong ints for key %q: got %d; want %d",
					key, v, actual[key].(int64))
				match = false
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
