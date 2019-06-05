package parser_test

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/etl/storage"
	"github.com/m-lab/etl/task"
)

func assertTCPInfoParser(in *parser.TCPInfoParser) {
	func(p etl.Parser) {}(in)
}

func localETLSource(fn string) (*storage.ETLSource, error) {
	if !(strings.HasSuffix(fn, ".tgz") || strings.HasSuffix(fn, ".tar") ||
		strings.HasSuffix(fn, ".tar.gz")) {
		return nil, errors.New("not tar or tgz: " + fn)
	}

	var rdr io.ReadCloser
	var raw io.ReadCloser
	raw, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	// Handle .tar.gz, .tgz files.
	if strings.HasSuffix(strings.ToLower(fn), "gz") {
		rdr, err = gzip.NewReader(raw)
		if err != nil {
			raw.Close()
			return nil, err
		}
	} else {
		rdr = raw
	}
	tarReader := tar.NewReader(rdr)

	timeout := 16 * time.Millisecond
	return &storage.ETLSource{TarReader: tarReader, Closer: raw, RetryBaseTime: timeout, TableBase: "test"}, nil
}

func TestTCPParser(t *testing.T) {
	os.Setenv("RELEASE_TAG", "foobar")
	parserVersion := parser.InitParserVersionForTest()

	filename := "testdata/20190516T013026.744845Z-tcpinfo-mlab4-arn02-ndt.tgz"

	src, err := localETLSource(filename)
	if err != nil {
		t.Fatalf("cannot read testdata.")
	}

	ins := &inMemoryInserter{}
	p := parser.NewTCPInfoParser(ins)
	task := task.NewTask(filename, src, p)

	startDecode := time.Now()
	n, err := task.ProcessAllTests()
	decodeTime := time.Since(startDecode)
	if err != nil {
		t.Fatal(err)
	}

	// This taskfile has 364 tcpinfo files in it.
	// tar -tf parser/testdata/20190516T013026.744845Z-tcpinfo-mlab4-arn02-ndt.tgz | wc
	if n != 364 {
		t.Error("Processed", n)
	}

	// Two tests (Cookies 2E1E and 2DEE) and have no snapshots, so there are only 362 rows committed.
	if ins.Committed() != 362 {
		t.Errorf("Expected %d, Got %d.", 362, ins.Committed())
	}

	if len(ins.data) < 1 {
		t.Fatal("Should have at least one inserted row")
	}

	// Examine first row in some detail...
	first, ok := ins.data[0].(*schema.TCPRow)
	if !ok {
		t.Fatal("not a TCPRow")
	}
	if first.ParseInfo.ParseTime.After(time.Now()) {
		t.Error("Should have inserted parse_time")
	}
	if first.ParseInfo.TaskFileName != filename {
		t.Error("Should have correct filename", filename, "!=", first.ParseInfo.TaskFileName)
	}

	if first.ParseInfo.ParserVersion != parserVersion {
		t.Error("ParserVersion not properly set", first.ParseInfo.ParserVersion)
	}
	// Spot check the SockID.SPort.
	if first.SockID.SPort != 3010 {
		t.Error("SPort should be 3010", first.SockID)
	}

	// This section is just for understanding how big these objects typically are, and what kind of compression
	// rates we see.  Not fundamental to the test.
	// Find the row with the largest json representation, and estimate the Marshalling time per snapshot.
	startMarshal := time.Now()
	var largestRow *schema.TCPRow
	var largestJson []byte
	totalSnaps := int64(0)
	for _, r := range ins.data {
		row, _ := r.(*schema.TCPRow)
		jsonBytes, _ := json.Marshal(r)
		totalSnaps += int64(len(row.Snapshots))
		if len(jsonBytes) > len(largestJson) {
			largestRow = row
			largestJson = jsonBytes
		}
	}
	marshalTime := time.Since(startMarshal)

	duration := largestRow.FinalSnapshot.Timestamp.Sub(largestRow.Snapshots[0].Timestamp)
	t.Log("Largest json is", len(largestJson), "bytes in", len(largestRow.Snapshots), "snapshots, over", duration, "with", len(largestJson)/len(largestRow.Snapshots), "json bytes/snap")
	t.Log("Total of", totalSnaps, "snapshots decoded and marshalled")
	t.Log("Average", decodeTime.Nanoseconds()/totalSnaps, "nsec/snap to decode", marshalTime.Nanoseconds()/totalSnaps, "nsec/snap to marshal")

	// Log one snapshot for debugging
	snapJson, _ := json.Marshal(largestRow.FinalSnapshot)
	t.Log(string(snapJson))

	if duration > 20*time.Second {
		t.Error("Incorrect duration calculation", duration)
	}
}

func TestTCPTask(t *testing.T) {
	os.Setenv("RELEASE_TAG", "foobar")
	parser.InitParserVersionForTest()

	ins := &inMemoryInserter{}
	p := parser.NewTCPInfoParser(ins)

	filename := "testdata/20190516T013026.744845Z-tcpinfo-mlab4-arn02-ndt.tgz"
	src, err := localETLSource(filename)
	if err != nil {
		t.Fatalf("cannot read testdata.")
	}

	task := task.NewTask(filename, src, p)

	n, err := task.ProcessAllTests()
	if err != nil {
		t.Fatal(err)
	}
	if n != 364 {
		t.Error(n, "!=", 364)
	}
}

func TestBQSaver(t *testing.T) {
	os.Setenv("RELEASE_TAG", "foobar")
	parser.InitParserVersionForTest()

	ins := &inMemoryInserter{}
	p := parser.NewTCPInfoParser(ins)

	filename := "testdata/20190516T013026.744845Z-tcpinfo-mlab4-arn02-ndt.tgz"
	src, err := localETLSource(filename)
	if err != nil {
		t.Fatalf("cannot read testdata.")
	}

	task := task.NewTask(filename, src, p)

	_, err = task.ProcessAllTests()
	if err != nil {
		t.Fatal(err)
	}

	row, _ := ins.data[0].(*schema.TCPRow)
	rowMap, _, _ := row.Save()
	sid, ok := rowMap["SockID"]
	if !ok {
		t.Fatal("Should have SockID")
	}
	id := sid.(map[string]bigquery.Value)
	if id["SPort"].(uint16) != 3010 {
		t.Error(id)
	}
}
func BenchmarkTCPParser(b *testing.B) {
	os.Setenv("RELEASE_TAG", "foobar")
	parser.InitParserVersionForTest()

	ins := &inMemoryInserter{}
	p := parser.NewTCPInfoParser(ins)

	filename := "testdata/20190516T013026.744845Z-tcpinfo-mlab4-arn02-ndt.tgz"
	n := 0
	for i := 0; i < b.N; i += n {

		src, err := localETLSource(filename)
		if err != nil {
			b.Fatalf("cannot read testdata.")
		}

		task := task.NewTask(filename, src, p)

		n, err = task.ProcessAllTests()
		if err != nil {
			b.Fatal(err)
		}
	}
}
