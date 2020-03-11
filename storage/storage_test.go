package storage

import (
	"io"
	"testing"
	"time"

	"cloud.google.com/go/storage"
)

var testBucket = "mlab-testing.appspot.com"
var tarFile = "gs://" + testBucket + "/test.tar"
var tgzFile = "gs://" + testBucket + "/test.tgz"

func TestGetReader(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping tests that access GCS")
	}
	client, err := GetStorageClient(false)
	if err != nil {
		t.Fatal(err)
	}
	rdr, cancel, err := getReader(client, testBucket, "test.tar", 60*time.Second)

	if err != nil {
		t.Fatal(err)
	}
	rdr.Close()
	cancel()
}

func TestNewTarReader(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping tests that access GCS")
	}
	src, err := NewTestSource(client, tarFile, "label")
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
	src, err := NewTestSource(client, tgzFile, "label")
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
var client *storage.Client

func init() {
	var err error
	client, err = GetStorageClient(false)
	if err != nil {
		panic(err)
	}
}

func BenchmarkNewTarReader(b *testing.B) {
	for i := 0; i < b.N; i++ {
		src, err := NewTestSource(client, tarFile, "label")
		if err == nil {
			src.Close()
		}
	}
}

func BenchmarkNewTarReaderGzip(b *testing.B) {
	for i := 0; i < b.N; i++ {
		src, err := NewTestSource(client, tgzFile, "label")
		if err == nil {
			src.Close()
		}
	}
}
