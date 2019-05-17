package storage

import (
	"io"
	"net/http"
	"testing"
	"time"
)

func TestGetObject(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping tests that access GCS")
	}
	obj, err := getObject(client, "m-lab-sandbox", "testfile", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	obj.Body.Close()
}

func TestNewTarReader(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping tests that access GCS")
	}
	src, err := NewETLSource(client, "gs://m-lab-sandbox/test.tar")
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
	src, err := NewETLSource(client, "gs://m-lab-sandbox/test.tgz")
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
var client *http.Client

func init() {
	var err error
	client, err = GetStorageClient(false)
	if err != nil {
		panic(err)
	}
}

func BenchmarkNewTarReader(b *testing.B) {
	for i := 0; i < b.N; i++ {
		src, err := NewETLSource(client, "gs://m-lab-sandbox/test.tar")
		if err == nil {
			src.Close()
		}
	}
}

func BenchmarkNewTarReaderGzip(b *testing.B) {
	for i := 0; i < b.N; i++ {
		src, err := NewETLSource(client, "gs://m-lab-sandbox/test.tgz")
		if err == nil {
			src.Close()
		}
	}
}
