// +build integration

package storage

import (
	"archive/tar"
	"context"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"

	"github.com/m-lab/go/rtx"

	"github.com/m-lab/etl/etl"
)

var testBucket = "mlab-testing.appspot.com"
var tarFile = "gs://" + testBucket + "/ndt/ndt5/2020/06/11/20200611T123456.12345Z-ndt5-mlab1-foo01-ndt.tar"
var tgzFile = "gs://" + testBucket + "/ndt/ndt5/2020/06/11/20200611T123456.12345Z-ndt5-mlab1-foo01-ndt.tgz"

func assertGCSourceIsTestSource(in etl.TestSource) {
	func(in etl.TestSource) {}(&GCSSource{})
}

func TestGetReader(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping tests that access GCS")
	}
	client, err := GetStorageClient(false)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	rdr, size, err := getReader(ctx, client, testBucket, "test.tar", 60*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rdr.Close()
	if size != 10240 {
		t.Error("Wrong size, expected 10240: ", size)
	}
}

func TestNewTarReader(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping tests that access GCS")
	}
	dpf, err := etl.ValidateTestPath(tarFile)
	if err != nil {
		t.Fatal(err)
	}
	src, err := NewTestSource(client, dpf, "label")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	count := 0
	for _, _, err := src.NextTest(10000000); err != io.EOF; _, _, err = src.NextTest(10000000) {
		if err != nil {
			t.Fatal(err)
		}
		count++
	}
	if count != 3 {
		t.Error("Wrong number of files: ", count)
	}
}

func TestNewTarReaderGzip(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping tests that access GCS")
	}
	dpf, err := etl.ValidateTestPath(tgzFile)
	if err != nil {
		t.Fatal(err)
	}
	src, err := NewTestSource(client, dpf, "label")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	count := 0
	for _, _, err := src.NextTest(10000000); err != io.EOF; _, _, err = src.NextTest(10000000) {
		if err != nil {
			t.Fatal(err)
		}
		count++
	}
	if count != 3 {
		t.Error("Wrong number of files: ", count)
	}
}

// Using a persistent client saves about 80 msec, and 220 allocs, totalling 70kB.
var client stiface.Client

func init() {
	var err error
	client, err = GetStorageClient(false)
	if err != nil {
		panic(err)
	}
}

func BenchmarkNewTarReader(b *testing.B) {
	dpf, err := etl.ValidateTestPath(tarFile)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		src, err := NewTestSource(client, dpf, "label")
		if err == nil {
			src.Close()
		}
	}
}

func BenchmarkNewTarReaderGzip(b *testing.B) {
	dpf, err := etl.ValidateTestPath(tgzFile)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		src, err := NewTestSource(client, dpf, "label")
		if err == nil {
			src.Close()
		}
	}
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

	f := GCSSourceFactory(stiface.AdaptClient(server.Client()))
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
