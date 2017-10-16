package parser_test

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"

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
		_, err := parser.ParseNDTFileName("2017/05/09/" + test)
		if err != nil {
			t.Error(err)
		}
	}
}

func TestNDTParser(t *testing.T) {
	// Load test data.
	ins := newInMemoryInserter()
	n := parser.NewNDTParser(ins)

	// TODO(prod) - why are so many of the tests to this endpoint and a few others?
	s2cName := `20170509T13:45:13.590210000Z_eb.measurementlab.net:44160.s2c_snaplog`
	s2cData, err := ioutil.ReadFile(`testdata/` + s2cName)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// Use a valid archive name.
	meta := map[string]bigquery.Value{"filename": "gs://mlab-test-bucket/ndt/2017/06/13/20170613T000000Z-mlab3-vie01-ndt-0186.tgz"}
	err = n.ParseAndInsert(meta, s2cName+".gz", s2cData)
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
	// Nothing should happen (with this parser) until new test group or Flush.
	if ins.Accepted() != 0 {
		t.Fatalf("Data processed prematurely.")
	}

	n.Flush()
	if ins.Accepted() != 1 {
		t.Fatalf(fmt.Sprintf("Failed to insert snaplog data. %d", ins.Accepted()))
	}

	// Extract the values saved to the inserter.
	actualValues := ins.data[0].(*bq.MapSaver).Values
	expectedValues := schema.Web100ValueMap{
		"connection_spec": schema.Web100ValueMap{
			"server_hostname": "mlab3.vie01.measurement-lab.org",
		},
		"web100_log_entry": schema.Web100ValueMap{
			"version": "2.5.27 201001301335 net100",
			"snap": schema.Web100ValueMap{
				"RemAddress": "45.56.98.222",
			},
			"connection_spec": schema.Web100ValueMap{
				"local_ip":    "213.208.152.37",
				"local_port":  int64(40105),
				"remote_ip":   "45.56.98.222",
				"remote_port": int64(44160),
				"local_af":    int64(0),
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

	err = n.ParseAndInsert(meta, c2sName+".gz", c2sData)
	if err != nil {
		t.Fatalf(err.Error())
	}
	n.Flush()
	if ins.Accepted() != 2 {
		t.Fatalf("Failed to insert snaplog data.")
	}
}

func TestNDTTaskError(t *testing.T) {
	// Load test data.
	ins := newInMemoryInserter()
	n := parser.NewNDTParser(ins)

	if n.TaskError() != nil {
		t.Error(n.TaskError())
	}

	ins.committed = 10
	if n.TaskError() != nil {
		t.Error(n.TaskError())
	}
	ins.failed = 2
	if n.TaskError() == nil {
		t.Error("Should have non-nil TaskError")
	}
}

// compare recursively checks whether actual values equal values in the expected values.
// The expected values may be a subset of the actual values, but not a superset.
func compare(t *testing.T, actual schema.Web100ValueMap, expected schema.Web100ValueMap) bool {
	match := true
	for key, value := range expected {
		act, ok := actual[key]
		if !ok {
			t.Logf("The actual data is missing a key: %s", key)
			return false
		}
		switch v := value.(type) {
		case schema.Web100ValueMap:
			match = match && compare(t, act.(schema.Web100ValueMap), v)
		case string:
			if act.(string) != v {
				t.Logf("Wrong strings for key %q: got %q; want %q",
					key, v, act.(string))
				match = false
			}
		case int64:
			if act.(int64) != v {
				t.Logf("Wrong ints for key %q: got %d; want %d",
					key, v, act.(int64))
				match = false
			}
		case int32:
			if act.(int32) != v {
				t.Logf("Wrong ints for key %q: got %d; want %d",
					key, v, act.(int32))
				match = false
			}
		case int:
			if act.(int) != v {
				t.Logf("Wrong ints for key %q: got %d; want %d",
					key, v, act.(int))
				match = false
			}
		case []float64:
			if len(v) != len(act.([]float64)) {
				t.Logf("Wrong floats for key %q: got %f; want %v",
					key, v, act.([]float64))
				match = false
			}
			for i := range v {
				if v[i] != act.([]float64)[i] {
					t.Logf("Wrong floats for key %q: got %f; want %v",
						key, v, act.([]float64))
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
	data      []interface{}
	committed int
	failed    int
}

func newInMemoryInserter() *inMemoryInserter {
	data := make([]interface{}, 0)
	return &inMemoryInserter{data, 0, 0}
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
