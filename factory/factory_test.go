package factory_test

import (
	"archive/tar"
	"context"
	"io"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/factory"
	"github.com/m-lab/go/rtx"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// Adds a path from testdata to bucket.
func add(svr *fakestorage.Server, bucket string, fn string, rdr io.Reader) {
	data, err := ioutil.ReadAll(rdr)
	rtx.Must(err, "Error reading data for", fn)
	svr.CreateObject(
		fakestorage.Object{
			BucketName: bucket,
			Name:       fn,
			Content:    data})
}

func loadFromTar(svr *fakestorage.Server, bucket string, tf *tar.Reader) *fakestorage.Server {
	for h, err := tf.Next(); err != io.EOF; h, err = tf.Next() {
		if h.Typeflag == tar.TypeReg {
			add(svr, bucket, h.Name, tf)
		}
	}
	return svr
}

func TestSourceFactory(t *testing.T) {
	server := fakestorage.NewServer([]fakestorage.Object{})
	file, fileErr := os.Open("testdata/20200318T003853.425987Z-ndt7-mlab3-syd03-ndt.tgz")
	if fileErr != nil {
		t.Fatal(fileErr)
	}
	defer file.Close()
	fn := "ndt/ndt7/2020/03/18/20200318T003853.425987Z-ndt7-mlab3-syd03-ndt.tgz"
	add(server, "fake-bucket", fn, file)

	f := factory.GCSSourceFactory(server.Client())
	s, err := f.Get(context.Background(),
		etl.DataPath{DataType: "test"})
	if err == nil {
		t.Fatal("Should be invalid data type")
	}

	// Test outcome for a missing file.
	missing := "ndt/ndt7/2020/03/18/20200318T003853.425987Z-ndt7-mlab3-syd99-ndt.tgz"
	dp, vErr := etl.ValidateTestPath("gs://fake-bucket/" + missing)
	if vErr != nil {
		t.Fatal(vErr)
	}
	s, err = f.Get(context.Background(), dp)
	if err == nil {
		t.Fatal("Expected error")
	}

	// Check operation for a valid file.
	dp, vErr = etl.ValidateTestPath("gs://fake-bucket/" + fn)
	if vErr != nil {
		t.Fatal(vErr)
	}

	s, err = f.Get(context.Background(), dp)
	if err != nil {
		t.Fatal(err)
	}

	if s == nil {
		t.Fatal("Nil source")
	}

	// Check the first test file.
	name, d, nextErr := s.NextTest(1000000)
	if nextErr != nil {
		t.Error(nextErr)
	}
	if len(d) != 29299 {
		t.Error("Expected len = 29299, got", len(d))
	}
	expect := "2020/03/18/ndt7-download-20200318T000643.982584404Z.ndt-knwp4_1583603744_00000000000058E8.json.gz"
	if name != expect {
		t.Error("Got:", name)
	}

	n := 0
	for ; nextErr == nil; name, d, nextErr = s.NextTest(1000000) {
		n++
	}
	if nextErr != io.EOF {

	}
	if n != 114 {
		t.Error("Expected 100, got", n)
	}
}
