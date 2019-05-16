package parser_test

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/m-lab/etl/storage"
	"github.com/m-lab/etl/task"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/go/bqx"
)

func assertTCPInfoParser(in *parser.TCPInfoParser) {
	func(p etl.Parser) {}(in)
}

func TestDump(t *testing.T) {
	sch, _ := (*parser.TCPRow)(nil).Schema()

	rr := bqx.RemoveRequired(sch)
	pp, err := bqx.PrettyPrint(rr, true)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Print(pp)
}

func localETLSource(t *testing.T, fn string) (*storage.ETLSource, error) {
	if !(strings.HasSuffix(fn, ".tgz") || strings.HasSuffix(fn, ".tar") ||
		strings.HasSuffix(fn, ".tar.gz")) {
		return nil, errors.New("not tar or tgz: " + fn)
	}

	t.Log(fn)
	var rdr io.ReadCloser
	var raw io.ReadCloser
	raw, err := os.Open(fn)
	if err != nil {
		t.Fatal(err)
	}
	// Handle .tar.gz, .tgz files.
	if strings.HasSuffix(strings.ToLower(fn), "gz") {
		rdr, err = gzip.NewReader(raw)
		if err != nil {
			raw.Close()
			return nil, err
		}
		t.Log("gzip")
	} else {
		rdr = raw
	}
	tarReader := tar.NewReader(rdr)

	timeout := 16 * time.Millisecond
	return &storage.ETLSource{tarReader, raw, timeout, "test"}, nil
}

func TestTCPParser(t *testing.T) {
	os.Setenv("RELEASE_TAG", "foobar")
	// parser.InitParserVersionForTest()

	filename := "testdata/20190516T013026.744845Z-tcpinfo-mlab4-arn02-ndt.tgz"

	src, err := localETLSource(t, filename)
	if err != nil {
		t.Fatalf("cannot read testdata.")
	}

	ins := &inMemoryInserter{}
	p := parser.NewTCPInfoParser(ins)
	task := task.NewTask(filename, src, p)

	n, err := task.ProcessAllTests()
	if err != nil {
		t.Fatal(err)
	}
	if n != 364 {
		t.Error("Processed", n)
	}

	if ins.Committed() != 344 {
		t.Errorf("Expected %d, Got %d.", 6, ins.Committed())
	}

	if len(ins.data) < 1 {
		t.Fatal("Should have at least one inserted row")
	}
	inserted, ok := ins.data[0].(*parser.TCPRow)
	if !ok {
		t.Fatal("not a TCPRow")
	}
	if inserted.ParseInfo.ParseTime.After(time.Now()) {
		t.Error("Should have inserted parse_time")
	}
	if inserted.ParseInfo.TaskFileName != filename {
		t.Error("Should have correct filename", filename, "!=", inserted.ParseInfo.TaskFileName)
	}

	if inserted.ParseInfo.ParserVersion != "local development" {
		t.Error("ParserVersion not properly set", inserted.ParseInfo.ParserVersion)
	}
}
