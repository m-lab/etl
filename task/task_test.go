// TODO(dev) add test overview
package task_test

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"time"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/storage" // TODO - would be better not to have this.
	"github.com/m-lab/etl/task"
)

// Just test call to NullParser.Parse
func TestPlumbing(t *testing.T) {
	foo := [10]byte{1, 2, 3, 4, 5, 1, 2, 3, 4, 5}
	p := parser.NullParser{}
	err := p.ParseAndInsert(nil, "foo", foo[:])
	if err != nil {
		fmt.Println(err)
	}
}

type NullCloser struct{}

func (nc NullCloser) Close() error {
	return nil
}

// Create a TarReader with simple test contents.
// TODO - could we break the dependency on storage here?
func MakeTestSource(t *testing.T) etl.TestSource {
	b := new(bytes.Buffer)
	tw := tar.NewWriter(b)
	hdr := tar.Header{Name: "foo", Mode: 0666, Typeflag: tar.TypeReg, Size: int64(8)}
	tw.WriteHeader(&hdr)
	_, err := tw.Write([]byte("biscuits"))
	if err != nil {
		t.Fatal(err)
	}

	// Put a large file in the middle to test skipping.
	hdr = tar.Header{Name: "big_file", Mode: 0666, Typeflag: tar.TypeReg, Size: int64(101)}
	tw.WriteHeader(&hdr)
	_, err = tw.Write(make([]byte, 101))
	if err != nil {
		t.Fatal(err)
	}

	hdr = tar.Header{Name: "bar", Mode: 0666, Typeflag: tar.TypeReg, Size: int64(11)}
	tw.WriteHeader(&hdr)
	_, err = tw.Write([]byte("butter milk"))
	if err = tw.Close(); err != nil {
		t.Fatal(err)
	}

	return &storage.GCSSource{TarReader: tar.NewReader(b), Closer: NullCloser{}, RetryBaseTime: time.Millisecond}
}

type TestParser struct {
	parser.FakeRowStats
	files []string
}

func (tp *TestParser) IsParsable(testName string, test []byte) (string, bool) {
	return "ext", true
}

func (tp *TestParser) TableName() string {
	return "test-table"
}
func (tp *TestParser) FullTableName() string {
	return "test-table"
}
func (tp *TestParser) Flush() error {
	return nil
}
func (tp *TestParser) TaskError() error {
	return nil
}

// TODO - pass testName through to BQ inserter?
func (tp *TestParser) ParseAndInsert(meta etl.Metadata, testName string, test []byte) error {
	tp.files = append(tp.files, testName)
	return nil
}

type badSource struct{}

func (bs *badSource) Next() (*tar.Header, error) {
	return nil, errors.New("Random Error")
}
func (bs *badSource) Read(b []byte) (int, error) {
	return 0, errors.New("Read error")
}

// TODO - this test is very slow, because it triggers the backoff and retry mechanism.
func TestBadTarFileInput(t *testing.T) {
	rdr := &storage.GCSSource{TarReader: &badSource{}, Closer: NullCloser{}, RetryBaseTime: time.Millisecond}

	tp := &TestParser{}

	// Among other things, this requires that tp implements etl.Parser.
	tt := task.NewTask("filename", rdr, tp, &NullCloser{})
	fc, err := tt.ProcessAllTests(true)
	if err.Error() != "Random Error" {
		t.Error("Expected Random Error, but got " + err.Error())
	}
	// Should see 1 files.
	if fc != 1 {
		t.Error("Expected 1 file: ", fc)
	}
	// ... but process none.
	if len(tp.files) != 0 {
		t.Error("Should have processed no files: ", len(tp.files))
	}
}

func TestTarFileInput(t *testing.T) {
	rdr := MakeTestSource(t)

	tp := &TestParser{}

	// Among other things, this requires that tp implements etl.Parser.
	tt := task.NewTask("filename", rdr, tp, &NullCloser{})
	fn, bb, err := tt.NextTest(100)
	if err != nil {
		t.Error(err)
	}
	if fn != "foo" {
		t.Error("Expected foo")
	}
	if string(bb) != "biscuits" {
		t.Error("Expected biscuits but got ", string(bb))
	}

	// Here we expect an oversize file error, with filename = big_file.
	fn, bb, err = tt.NextTest(100)
	if fn != "big_file" {
		t.Error("Expected big_file: " + fn)
	}
	if err == nil {
		t.Error("Expected oversize file")
	} else if err != storage.ErrOversizeFile {
		t.Error("Expected oversize file but got: " + err.Error())
	}

	// This is the last file, so we expect EOF.
	fn, bb, err = tt.NextTest(100)
	if err != nil {
		t.Error(err)
	}
	if fn != "bar" {
		t.Error("Expected bar")
	}
	if string(bb) != "butter milk" {
		t.Error("Expected butter milk but got ", string(bb))
	}

	// Reset the tar reader and create new task, to test the ProcessAllTests behavior.
	rdr = MakeTestSource(t)

	tt = task.NewTask("filename", rdr, tp, &NullCloser{})
	tt.SetMaxFileSize(100)
	fc, err := tt.ProcessAllTests(false)
	if err != nil {
		t.Error("Expected nil error, but got ", err)
	}
	// Should see 3 files.
	if fc != 3 {
		t.Error("Expected 3 files: ", fc)
	}
	// ... but process only two.
	if len(tp.files) != 2 {
		t.Error("Should have processed two files: ", len(tp.files))
	}
	if !reflect.DeepEqual(tp.files, []string{"foo", "bar"}) {
		t.Error("Not expected files: ", tp.files)
	}

}
