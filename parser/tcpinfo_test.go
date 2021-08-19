package parser_test

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-test/deep"

	v2 "github.com/m-lab/annotation-service/api/v2"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
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
	return &storage.GCSSource{TarReader: tarReader, Closer: raw,
		RetryBaseTime: timeout, TableBase: "test", PathDate: civil.Date{Year: 2020, Month: 6, Day: 11}}, nil
}

var tcpInfoAnno = map[string]*api.Annotations{
	// client ip.
	"35.225.75.192": &api.Annotations{
		Geo: &api.GeolocationIP{
			ContinentCode: "NA",
			CountryCode:   "US",
			Latitude:      1.0,
			Longitude:     2.0,
		},
		Network: &api.ASData{
			ASNumber: 1234,
			Systems: []api.System{
				{ASNs: []uint32{1234}},
			},
		},
	},
	// server ip.
	"195.89.146.242": &api.Annotations{
		Geo: &api.GeolocationIP{
			ContinentCode: "NA",
			CountryCode:   "US",
			Latitude:      1.0,
			Longitude:     2.0,
		},
		Network: &api.ASData{
			ASNumber: 1234,
			Systems: []api.System{
				{ASNs: []uint32{1234}},
			},
		},
	},
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

func (in *inMemorySink) Commit(data []interface{}, label string) (int, error) {
	in.acquire()
	defer in.release()
	in.data = append(in.data, data...)
	in.committed = len(in.data)
	return len(data), nil
}

func (in *inMemorySink) Close() error {
	return nil
}

func (in *inMemorySink) Flush() error {
	in.committed = len(in.data)
	return nil
}
func (in *inMemorySink) Committed() int {
	return in.committed
}

type nullCloser struct{}

func (nc nullCloser) Close() error { return nil }

// NOTE: This uses a fake annotator which returns no annotations.
// TODO: This test seems to be flakey in travis - sometimes only 357 tests instead of 362
func TestTCPParser(t *testing.T) {
	parserVersion := parser.InitParserVersionForTest()

	taskfilename := "testdata/20190516T013026.744845Z-tcpinfo-mlab4-arn02-ndt.tgz"
	url := "gs://fake-archive/ndt/tcpinfo/2019/05/16/" + filepath.Base(taskfilename)

	src, err := fileSource(taskfilename)
	if err != nil {
		t.Fatal("Failed reading testdata from", taskfilename)
	}

	// Inject fake inserter and annotator
	ins := newInMemorySink()
	p := parser.NewTCPInfoParser(ins, "test", "_suffix", newFakeAnnotator(tcpInfoAnno))
	task := task.NewTask(url, src, p, nullCloser{})

	startDecode := time.Now()
	n, err := task.ProcessAllTests(false)
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
		if row.ParseInfo.TaskFileName != url {
			t.Error("Should have correct taskfilename", taskfilename, "!=", row.ParseInfo.TaskFileName)
		}

		if !strings.Contains(row.ParseInfo.Filename, row.UUID) {
			t.Errorf("Should have non empty filename containing UUID: %s not found in :%s:\n", row.UUID, row.ParseInfo.Filename)
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

	// Verify the client and server annotations match.
	cx := v2.ConvertAnnotationsToClientAnnotations(tcpInfoAnno["35.225.75.192"])
	if diff := deep.Equal(&largestRow.ClientX, cx); diff != nil {
		t.Errorf("ClientX annotation does not match; %#v", diff)
	}
	sx := v2.ConvertAnnotationsToServerAnnotations(tcpInfoAnno["195.89.146.242"])
	sx.Site = "arn02"
	sx.Machine = "mlab4"
	if diff := deep.Equal(&largestRow.ServerX, sx); diff != nil {
		t.Errorf("ServerX annotation does not match; %#v", diff)
	}
}

// This is a subset of TestTCPParser, but simpler, so might be useful.
func TestTCPTask(t *testing.T) {
	// Inject fake inserter and annotator
	ins := newInMemorySink()
	p := parser.NewTCPInfoParser(ins, "test", "_suffix", newFakeAnnotator(tcpInfoAnno))

	filename := "testdata/20190516T013026.744845Z-tcpinfo-mlab4-arn02-ndt.tgz"
	url := "gs://fake-archive/ndt/tcpinfo/2019/05/16/" + filepath.Base(filename)
	src, err := fileSource(filename)
	if err != nil {
		t.Fatal("Failed reading testdata from", filename)
	}

	task := task.NewTask(url, src, p, &nullCloser{})

	n, err := task.ProcessAllTests(false)
	if err != nil {
		t.Fatal(err)
	}
	if n != 364 {
		t.Errorf("Expected ProcessAllTests to handle %d files, but it handled %d.\n", 364, n)
	}
}

func TestBQSaver(t *testing.T) {
	// Inject fake inserter and annotator
	ins := newInMemorySink()
	p := parser.NewTCPInfoParser(ins, "test", "_suffix", newFakeAnnotator(tcpInfoAnno))

	filename := "testdata/20190516T013026.744845Z-tcpinfo-mlab4-arn02-ndt.tgz"
	url := "gs://fake-archive/ndt/tcpinfo/2019/05/16/" + filepath.Base(filename)
	src, err := fileSource(filename)
	if err != nil {
		t.Fatal("Failed reading testdata from", filename)
	}

	task := task.NewTask(url, src, p, &nullCloser{})

	_, err = task.ProcessAllTests(false)
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

// This test writes 364 rows to a json file in GCS.
// The rows can then be loaded into a BQ table, using the schema in testdata, like:
// bq load --source_format=NEWLINE_DELIMITED_JSON \
//    mlab-sandbox:gfr.small_tcpinfo gs://archive-mlab-testing/gfr/tcpinfo.json ./schema.json
// Recommend commenting out snapshots in tcpinfo.go.
func TestTaskToGCS(t *testing.T) {
	t.Skip("Skipping test intended for manual experimentation")

	c, err := storage.GetStorageClient(true)
	if err != nil {
		t.Fatal(err)
	}

	rw, err := storage.NewRowWriter(context.Background(), c, "archive-mlab-testing", "gfr/tcpinfo2.json")
	if err != nil {
		t.Fatal(err)
	}
	// Inject fake inserter and annotator
	p := parser.NewTCPInfoParser(rw, "test", "_suffix", newFakeAnnotator(tcpInfoAnno))

	filename := "testdata/20190516T013026.744845Z-tcpinfo-mlab4-arn02-ndt.tgz"
	url := "gs://fake-archive/ndt/tcpinfo/2019/05/16/" + filepath.Base(filename)
	src, err := fileSource(filename)
	if err != nil {
		t.Fatal("Failed reading testdata from", filename)
	}

	task := task.NewTask(url, src, p, &nullCloser{})

	n, err := task.ProcessAllTests(false)
	if err != nil {
		t.Fatal(err)
	}
	err = rw.Close()
	if err != nil {
		t.Fatal(err)
	}

	if n != 364 {
		t.Errorf("Expected ProcessAllTests to handle %d files, but it handled %d.\n", 364, n)
	}
}

func BenchmarkTCPParser(b *testing.B) {
	// Inject fake inserter and annotator
	ins := newInMemorySink()
	p := parser.NewTCPInfoParser(ins, "test", "_suffix", newFakeAnnotator(tcpInfoAnno))

	filename := "testdata/20190516T013026.744845Z-tcpinfo-mlab4-arn02-ndt.tgz"
	n := 0
	for i := 0; i < b.N; i += n {
		src, err := fileSource(filename)
		if err != nil {
			b.Fatalf("cannot read testdata.")
		}

		task := task.NewTask(filename, src, p, &nullCloser{})

		n, err = task.ProcessAllTests(false)
		if err != nil {
			b.Fatal(err)
		}
	}
}
