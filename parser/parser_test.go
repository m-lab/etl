// TODO(soon) Implement good tests for the existing parsers.
//
package parser_test

import (
	"fmt"
	"log"
	"os"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/parser"
	pipe "gopkg.in/m-lab/pipe.v3"
)

func init() {
	etl.Version = "foobar"
	parser.InitParserVersionForTest()
}

// countingInserter counts the calls to InsertRows and Flush.
// Inject into Parser for testing.
type countingInserter struct {
	etl.Inserter
	CallCount  int
	RowCount   int
	FlushCount int
}

func (ti *countingInserter) InsertRow(data interface{}) error {
	ti.CallCount++
	ti.RowCount++
	return nil
}
func (ti *countingInserter) InsertRows(data []interface{}) error {
	ti.CallCount++
	ti.RowCount += len(data)
	return nil
}
func (ti *countingInserter) Flush() error {
	ti.FlushCount++
	return nil
}

//------------------------------------------------------------------------------------
// TestParser ignores the content, returns a MapSaver containing meta data and
// "testname":"..."
// TODO add tests
type TestParser struct {
	inserter     etl.Inserter
	etl.RowStats // Allows RowStats to be implemented through an embedded struct.
}

func NewTestParser(ins etl.Inserter) etl.Parser {
	return &TestParser{
		ins,
		&parser.FakeRowStats{}} // Use a FakeRowStats to provide the RowStats functions.
}

func (tp *TestParser) IsParsable(testName string, test []byte) (string, bool) {
	return "ext", true
}

func (tp *TestParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error {
	metrics.TestCount.WithLabelValues("table", "test", "ok").Inc()
	values := make(map[string]bigquery.Value, len(meta)+1)
	// TODO is there a better way to do this?
	for k, v := range meta {
		values[k] = v
	}
	values["testname"] = testName
	return tp.inserter.InsertRow(bq.MapSaver(values))
}

// These functions are also required to complete the etl.Parser interface.
func (tp *TestParser) Flush() error {
	return nil
}
func (tp *TestParser) TableName() string {
	return "test-table"
}
func (tp *TestParser) FullTableName() string {
	return "test-table"
}
func (tp *TestParser) TaskError() error {
	return nil
}

func TestPlumbing(t *testing.T) {
	foo := [10]byte{1, 2, 3, 4, 5, 1, 2, 3, 4, 5}
	tci := countingInserter{}
	var ti etl.Inserter = &tci
	var p etl.Parser = NewTestParser(ti)
	err := p.ParseAndInsert(nil, "foo", foo[:])
	if err != nil {
		fmt.Println(err)
	}
	if tci.CallCount != 1 {
		t.Error("Should have called the inserter")
	}
}

func TestMain(m *testing.M) {
	p := pipe.Script(
		"unpacking testdata files",
		pipe.Exec("tar", "-C", "testdata", "-xvf", "testdata/pt-files.tar.gz"),
		pipe.Exec("tar", "-C", "testdata", "-xvf", "testdata/web100-files.tar.gz"),
		pipe.Exec("tar", "-C", "testdata", "-xvf", "testdata/sidestream-files.tar.gz"),
	)
	_, err := pipe.CombinedOutput(p)
	if err != nil {
		log.Fatal(err)
	}
	exitCode := m.Run()
	for _, dir := range []string{"testdata/PT", "testdata/web100", "testdata/sidestream"} {
		os.RemoveAll(dir)
	}
	os.Exit(exitCode)
}
