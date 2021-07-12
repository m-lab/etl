package parser_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"cloud.google.com/go/bigquery"

	"github.com/go-test/deep"
	"github.com/kr/pretty"

	v2as "github.com/m-lab/annotation-service/api/v2"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/etl/schema"
)

func assertNDTTestIsAnnotatable(r parser.NDTTest) {
	func(row.Annotatable) {}(r)
}

func assertNDTTestIsValueSaver(r parser.NDTTest) {
	func(bigquery.ValueSaver) {}(r)
}

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

func TestCopyStructToMap(t *testing.T) {
	tests := []struct {
		source interface{}
		dest   map[string]bigquery.Value
		res    map[string]bigquery.Value
	}{
		{
			source: &struct {
				A   int64
				Bee string
			}{A: 1, Bee: "2"},
			dest: make(map[string]bigquery.Value),
			res:  map[string]bigquery.Value{"a": int64(1), "bee": "2"},
		},
		{
			source: &struct {
				A   int64
				Bee string
			}{A: 0, Bee: ""},
			dest: make(map[string]bigquery.Value),
			res:  map[string]bigquery.Value{},
		},
		{
			source: &struct{}{},
			dest:   make(map[string]bigquery.Value),
			res:    map[string]bigquery.Value{},
		},
	}
	for _, test := range tests {
		parser.CopyStructToMap(test.source, test.dest)
		if diff := deep.Equal(test.dest, test.res); diff != nil {
			t.Error(diff)
		}
	}
}

func TestNDTParser(t *testing.T) {
	// Load test data.
	ins := newInMemoryInserter()

	// Completely fake annotation data.
	responseJSON := `{"AnnotatorDate":"2018-12-05T00:00:00Z",
		"Annotations":{
		   "45.56.98.222":{"Geo":{"postal_code":"45569", "latitude": 1.0, "longitude": 2.0}, "Network":{"ASName":"Fake Client ISP", "ASNumber": 123, "Systems":[{"ASNs":[123]}]}},
		   "213.208.152.37":{"Geo":{"postal_code":"21320", "latitude": 3.0, "longitude": 4.0}, "Network":{"ASName":"Fake Server ISP", "ASNumber": 456, "CIDR": "213.208.152.0/26", "Systems":[{"ASNs":[456]}]}}
	   }}`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, responseJSON)
	}))
	defer ts.Close()

	n := parser.NewNDTParser(ins, v2as.GetAnnotator(ts.URL))

	// TODO(prod) - why are so many of the tests to this endpoint and a few others?
	// A: because this is EB, which runs all the health tests.
	s2cName := `20170509T13:45:13.590210000Z_eb.measurementlab.net:44160.s2c_snaplog`
	s2cData, err := ioutil.ReadFile(`testdata/web100/` + s2cName)
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
	metaData, err := ioutil.ReadFile(`testdata/web100/` + metaName)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = n.ParseAndInsert(meta, metaName, metaData)
	if err != nil {
		t.Fatalf(err.Error())
	}

	c2sName := `20170509T13:45:13.590210000Z_eb.measurementlab.net:48716.c2s_snaplog`
	c2sData, err := ioutil.ReadFile(`testdata/web100/` + c2sName)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = n.ParseAndInsert(meta, c2sName+".gz", c2sData)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// Nothing should happen (with this parser) until new test group or Flush.
	if ins.Accepted() != 0 {
		t.Fatalf("Data processed prematurely.")
	}

	err = n.Flush()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if ins.Accepted() != 2 {
		t.Fatalf("Failed to insert snaplog data.")
	}

	// Extract the values saved to the inserter.
	actualValues := ins.data[0].(parser.NDTTest).Web100ValueMap
	expectedValues := schema.Web100ValueMap{
		// echo -n 20170509T13:45:13.590210000Z_eb.measurementlab.net:44160.s2c_snaplog.gz | openssl dgst -binary -md5 | base64  | tr '/+' '_-' | tr -d '='
		"id": "nYjSCZhB0EfQPChl2tT8Fg",
		"connection_spec": schema.Web100ValueMap{
			"server_hostname": "mlab3.vie01.measurement-lab.org",
			"client": schema.Web100ValueMap{
				"network": schema.Web100ValueMap{"asn": int64(123)},
			},
			"server": schema.Web100ValueMap{
				"network":   schema.Web100ValueMap{"asn": int64(456)},
				"iata_code": "VIE",
			},
			"ClientX": schema.Web100ValueMap{
				"Network": schema.Web100ValueMap{
					"ASName":   "Fake Client ISP",
					"ASNumber": int64(123),
					"Missing":  false,
				},
				"Geo": schema.Web100ValueMap{
					"Latitude":  1.0,
					"Longitude": 2.0,
				},
			},
			"ServerX": schema.Web100ValueMap{
				"Site":    "vie01",
				"Machine": "mlab3",
				"Network": schema.Web100ValueMap{
					"ASName":   "Fake Server ISP",
					"ASNumber": int64(456),
					"CIDR":     "213.208.152.0/26",
					"Missing":  false,
				},
				"Geo": schema.Web100ValueMap{
					"Latitude":  3.0,
					"Longitude": 4.0,
				},
			},
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
		case float64:
			if v != act.(float64) {
				t.Logf("Wrong floats for key %q: got %f; want %v",
					key, v, act.(float64))
				match = false
			}
		case bool:
			if act.(bool) != v {
				t.Logf("Wrong bool for key %q: got %t; want %t",
					key, v, act.(bool))
				match = false
			}
		default:
			fmt.Printf("Unsupported type. %T\n", v)
			panic(nil)
		}
	}
	return match
}

func assertInserter(in etl.Inserter) {
	func(in etl.Inserter) {}(&inMemoryInserter{})
}

type inMemoryInserter struct {
	data      []interface{}
	committed int
	failed    int
	token     chan struct{}
}

func newInMemoryInserter() *inMemoryInserter {
	data := make([]interface{}, 0)
	token := make(chan struct{}, 1)
	token <- struct{}{}
	return &inMemoryInserter{data, 0, 0, token}
}

// acquire and release handle the single token that protects the FlushSlice and
// access to the metrics.
func (in *inMemoryInserter) acquire() {
	<-in.token
}
func (in *inMemoryInserter) release() {
	in.token <- struct{}{} // return the token.
}

func (in *inMemoryInserter) Commit(data []interface{}, label string) error {
	return in.Put(data)
}

func (in *inMemoryInserter) Params() etl.InserterParams {
	return etl.InserterParams{}
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
