package parser

// TODO - use parser_test, to force proper package isolation.

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"

	"cloud.google.com/go/bigquery"
)

func TestNDTParser(t *testing.T) {
	// Load test data.
	rawData, err := ioutil.ReadFile("testdata/c2s_snaplog")

	var n Parser
	n = &NDTParser{tmpDir: "./", tableName: "ndt-table"}
	values, err := n.Parse(nil, "filename", rawData)
	if err != nil {
		t.Fatalf(err.Error())
	}

	results := values.(map[string]bigquery.Value)
	// TODO(dev): find a better way to verify the returned values are correct.
	expectedValues := map[string]bigquery.Value{
		"web100_log_entry.version":                    "2.5.27 201001301335 net100",
		"web100_log_entry.snap.RemAddress":            "45.56.98.222",
		"web100_log_entry.connection_spec.local_port": int64(43685),
	}
	for key, value := range expectedValues {
		// Raw bigquery.Value instances do not compare.
		if results[key] != value {
			t.Errorf("Wrong value for %q: got %q; want %q", key, str(results[key]), str(value))
		}
	}
	// TODO(dev): remove print of entire snaplog.
	prettyPrint(results)
}

// TODO(dev): is there a better way to display these values?
func str(v bigquery.Value) string {
	switch v.(type) {
	case string:
		s := v.(string)
		return s
	case int64:
		i := v.(int64)
		return fmt.Sprintf("%d", i)
	default:
		panic("Only string and int64 types are supported.")
	}
}

func prettyPrint(results map[string]bigquery.Value) {
	b, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Print(string(b))
}
