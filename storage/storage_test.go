package storage

import (
	"io"
	"net/http"
	"testing"
	"time"
)

func TestGetObject(t *testing.T) {
	obj, err := getObject(client, "m-lab-sandbox", "testfile", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	obj.Body.Close()
}

func TestNewTarReader(t *testing.T) {
	reader, err := NewGCSTarReader(client, "gs://m-lab-sandbox/test.tar")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, _, err := reader.NextTest(); err != io.EOF; _, _, err = reader.NextTest() {
		if err != nil {
			t.Fatal(err)
		}
		count += 1
	}
	if count != 3 {
		t.Error("Wrong number of files: ", count)
	}
	reader.Close()
}

func TestNewTarReaderGzip(t *testing.T) {
	reader, err := NewGCSTarReader(client, "gs://m-lab-sandbox/test.tgz")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, _, err := reader.NextTest(); err != io.EOF; _, _, err = reader.NextTest() {
		if err != nil {
			t.Fatal(err)
		}
		count += 1
	}
	if count != 3 {
		t.Error("Wrong number of files: ", count)
	}
	reader.Close()
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
		reader, _ := NewGCSTarReader(client, "gs://m-lab-sandbox/test.tar")
		// Omitting the Close doesn't seem to cause any problems.  Is that really true?
		reader.Close()
	}
}

func BenchmarkNewTarReaderGzip(b *testing.B) {
	for i := 0; i < b.N; i++ {
		reader, _ := NewGCSTarReader(client, "gs://m-lab-sandbox/test.tgz")
		// Omitting the Close doesn't seem to cause any problems.  Is that really true?
		reader.Close()
	}
}
