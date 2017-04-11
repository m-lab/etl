// TODO(dev) add test overview
//
package task_test

import (
	"archive/tar"
	"bytes"
	"cloud.google.com/go/bigquery"
	"fmt"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/task"
	"reflect"
	"testing"
)

// Just test call to NullParser.HandleTest
func TestPlumbing(t *testing.T) {
	foo := [10]byte{1, 2, 3, 4, 5, 1, 2, 3, 4, 5}
	p := parser.NullParser{}
	_, err := p.HandleTest("foo", "table", foo[:])
	if err != nil {
		fmt.Println(err)
	}
}

// Create a tar.Reader with simple test contents.
func MakeTestTar(t *testing.T) *tar.Reader {
	b := new(bytes.Buffer)
	tw := tar.NewWriter(b)
	hdr := tar.Header{Name: "foo", Mode: 0666, Typeflag: tar.TypeReg, Size: int64(8)}
	tw.WriteHeader(&hdr)
	_, err := tw.Write([]byte("biscuits"))
	if err != nil {
		t.Fatal(err)
	}

	hdr = tar.Header{Name: "bar", Mode: 0666, Typeflag: tar.TypeReg, Size: int64(11)}
	tw.WriteHeader(&hdr)
	_, err = tw.Write([]byte("butter milk"))
	if err = tw.Close(); err != nil {
		t.Fatal(err)
	}

	return tar.NewReader(b)
}

type TestParser struct {
	parser.Parser
	files []string
}

func (tp *TestParser) HandleTest(fn string, table string, test []byte) (bigquery.ValueSaver, error) {
	// TODO - pass filename through to BQ inserter
	tp.files = append(tp.files, fn)
	return nil, nil
}

// TODO(dev) - add good comments
func TestTarFileInput(t *testing.T) {
	rdr := MakeTestTar(t)

	var prsr TestParser
	tt := task.NewTask(rdr, &prsr, "test_table")
	fn, bb, err := tt.Next()
	if err != nil {
		t.Error(err)
	}
	if fn != "foo" {
		t.Error("Expected foo")
	}
	if string(bb) != "biscuits" {
		t.Error("Expected biscuits but got ", string(bb))
	}

	fn, bb, err = tt.Next()
	if err != nil {
		t.Error(err)
	}
	if fn != "bar" {
		t.Error("Expected bar")
	}
	if string(bb) != "butter milk" {
		t.Error("Expected butter milk but got ", string(bb))
	}

	rdr = MakeTestTar(t)
	tt = task.NewTask(rdr, &prsr, "test_table")
	tt.ProcessAllTests()

	if len(prsr.files) != 2 {
		t.Error("Too few files ", len(prsr.files))
	}
	if !reflect.DeepEqual(prsr.files, []string{"foo", "bar"}) {
		t.Error("Not expected files: ", prsr.files)
	}

}
