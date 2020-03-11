package parser_test

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	v2 "github.com/m-lab/annotation-service/api/v2"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/etl/storage"
	"github.com/m-lab/etl/task"
)

func assertTCPInfoParser(in *parser.TCPInfoParser) {
	func(p etl.Parser) {}(in)
}

func fileSource(fn string) (etl.TestSource, error) {
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
	return &storage.GCSSource{TarReader: tarReader, Closer: raw, RetryBaseTime: timeout, TableBase: "test"}, nil
}

type fakeAnnotator struct{}

func (ann *fakeAnnotator) GetAnnotations(ctx context.Context, date time.Time, ips []string, info ...string) (*v2.Response, error) {
	return &v2.Response{AnnotatorDate: time.Now(), Annotations: make(map[string]*api.Annotations, 0)}, nil
}

type inMemorySink struct {
	data      []interface{}
	committed int
	failed    int
	token     chan struct{}
}

func newInMemorySink() *inMemorySink {
	data := make([]interface{}, 0)
	token := make(chan struct{}, 1)
	token <- struct{}{}
	return &inMemorySink{data, 0, 0, token}
}

// acquire and release handle the single token that protects the FlushSlice and
// access to the metrics.
func (in *inMemorySink) acquire() {
	<-in.token
}
func (in *inMemorySink) release() {
	in.token <- struct{}{} // return the token.
}

func (in *inMemorySink) Commit(data []interface{}, label string) error {
	in.acquire()
	defer in.release()
	in.data = append(in.data, data...)
	in.committed = len(in.data)
	return nil
}

func (in *inMemorySink) Flush() error {
	in.committed = len(in.data)
	return nil
}
func (in *inMemorySink) Committed() int {
	return in.committed
}

// NOTE: This uses a fake annotator which returns no annotations.
// TODO: This test seems to be flakey in travis - sometimes only 357 tests instead of 362
func TestTCPParser(t *testing.T) {
	os.Setenv("RELEASE_TAG", "foobar")
	parserVersion := parser.InitParserVersionForTest()

	filename := "testdata/20190516T013026.744845Z-tcpinfo-mlab4-arn02-ndt.tgz"

	src, err := fileSource(filename)
	if err != nil {
		t.Fatal("Failed reading testdata from", filename)
	}

	// Inject fake inserter and annotator
	ins := newInMemorySink()
	p := parser.NewTCPInfoParser(ins, "test", &fakeAnnotator{})
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
		t.Errorf("Expected ProcessAllTests to handle %d files, but it handled %d.\n", 364, n)
	}

	// Two tests (Cookies 2E1E and 2DEE) and have no snapshots, so there are only 362 rows committed.
	if ins.Committed() != 362 {
		t.Errorf("Expected %d, Got %d.", 362, ins.Committed())
	}

	if len(ins.data) < 1 {
		t.Fatal("Should have at least one inserted row")
	}

	// Examine rows in some detail...
	for i, rawRow := range ins.data {
		row, ok := rawRow.(*schema.TCPRow)
		if !ok {
			t.Fatal("not a TCPRow")
		}
		if row.ParseInfo.ParseTime.After(time.Now()) {
			t.Error("Should have inserted parse_time")
		}
		if row.ParseInfo.TaskFileName != filename {
			t.Error("Should have correct filename", filename, "!=", row.ParseInfo.TaskFileName)
		}

		if row.ParseInfo.ParserVersion != parserVersion {
			t.Error("ParserVersion not properly set", row.ParseInfo.ParserVersion)
		}
		// Spot check the SockID.SPort.  First 5 rows have SPort = 3010
		if i < 5 && row.SockID.SPort != 3010 {
			t.Error("SPort should be 3010", row.SockID, i)
		}
		// Check that source (server) IPs are correct.
		if row.SockID.SrcIP != "195.89.146.242" && row.SockID.SrcIP != "2001:5012:100:24::242" {
			t.Error("Wrong SrcIP", row.SockID.SrcIP)
		}

		if row.Client == nil {
			t.Error("Client annotations should not be nil", row.SockID, row.FinalSnapshot)
		}
		if row.Server == nil {
			t.Error("Server annotations should not be nil")
		} else if row.Server.IATA == "" {
			t.Error("Server IATA should not be empty")
		}
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

	if totalSnaps != 1588 {
		t.Error("expected 1588 (thinned) snapshots, got", totalSnaps)
	}
}

// This is a subset of TestTCPParser, but simpler, so might be useful.
func TestTCPTask(t *testing.T) {
	os.Setenv("RELEASE_TAG", "foobar")
	parser.InitParserVersionForTest()

	// Inject fake inserter and annotator
	ins := newInMemorySink()
	p := parser.NewTCPInfoParser(ins, "test", &fakeAnnotator{})

	filename := "testdata/20190516T013026.744845Z-tcpinfo-mlab4-arn02-ndt.tgz"
	src, err := fileSource(filename)
	if err != nil {
		t.Fatal("Failed reading testdata from", filename)
	}

	task := task.NewTask(filename, src, p)

	n, err := task.ProcessAllTests()
	if err != nil {
		t.Fatal(err)
	}
	if n != 364 {
		t.Errorf("Expected ProcessAllTests to handle %d files, but it handled %d.\n", 364, n)
	}
}

func TestBQSaver(t *testing.T) {
	os.Setenv("RELEASE_TAG", "foobar")
	parser.InitParserVersionForTest()

	// Inject fake inserter and annotator
	ins := newInMemorySink()
	p := parser.NewTCPInfoParser(ins, "test", &fakeAnnotator{})

	filename := "testdata/20190516T013026.744845Z-tcpinfo-mlab4-arn02-ndt.tgz"
	src, err := fileSource(filename)
	if err != nil {
		t.Fatal("Failed reading testdata from", filename)
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

	// Inject fake inserter and annotator
	ins := newInMemorySink()
	p := parser.NewTCPInfoParser(ins, "test", &fakeAnnotator{})

	filename := "testdata/20190516T013026.744845Z-tcpinfo-mlab4-arn02-ndt.tgz"
	n := 0
	for i := 0; i < b.N; i += n {
		src, err := fileSource(filename)
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
