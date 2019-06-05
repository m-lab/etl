package parser_test

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"log"
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
	if n != 364 {
		t.Error("Processed", n)
	}

	if ins.Committed() != 362 {
		t.Errorf("Expected %d, Got %d.", 362, ins.Committed())
	}

	if len(ins.data) < 1 {
		t.Fatal("Should have at least one inserted row")
	}
	inserted, ok := ins.data[0].(*schema.TCPRow)
	if !ok {
		t.Fatal("not a TCPRow")
	}
	if inserted.ParseInfo.ParseTime.After(time.Now()) {
		t.Error("Should have inserted parse_time")
	}
	if inserted.ParseInfo.TaskFileName != filename {
		t.Error("Should have correct filename", filename, "!=", inserted.ParseInfo.TaskFileName)
	}

	if inserted.ParseInfo.ParserVersion != parserVersion {
		t.Error("ParserVersion not properly set", inserted.ParseInfo.ParserVersion)
	}

	startMarshal := time.Now()
	jsonBytes, err := json.Marshal(ins.data)
	if err != nil {
		t.Fatal(err)
	}
	marshalTime := time.Since(startMarshal)
	t.Log("json is", len(jsonBytes)/n, " bytes per file")

	largest := 0
	snaps := 0
	duration := time.Duration(0)
	sumSnaps := int64(0)
	for _, r := range ins.data {
		jsonBytes, _ := json.Marshal(r)
		row, _ := r.(*schema.TCPRow)
		sumSnaps += int64(len(row.Snapshots))
		if len(jsonBytes) > largest {
			largest = len(jsonBytes)
			snaps = len(row.Snapshots)
			duration = row.FinalSnapshot.Timestamp.Sub(row.Snapshots[0].Timestamp)
		}
	}
	t.Log("Largest is", largest, "bytes in", snaps, "snapshots, over", duration, "with", largest/snaps, "json bytes/snap")
	t.Log("Total of", sumSnaps, "snapshots decoded and marshalled")
	t.Log("Average", decodeTime.Nanoseconds()/sumSnaps, "nsec/snap to decode", marshalTime.Nanoseconds()/sumSnaps, "nsec/snap to marshal")

	row, _ := ins.data[0].(*schema.TCPRow)
	// Spot check the SockID.SPort.
	if row.SockID.SPort != 3010 {
		t.Error("SPort should be 3010", row.SockID)
	}
	snapJson, _ := json.Marshal(row.FinalSnapshot)
	log.Println(string(snapJson))

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
