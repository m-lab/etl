package storage

import (
	"net/http"
	"testing"
	"time"
)

func TestGetObject(t *testing.T) {
	obj, err := getObject(client, "m-lab-sandbox", "testfile", 10*time.Second)
	if err != nil {
		t.Fatal(err)
		return
	}
	obj.Body.Close()
}

func TestNewTarReader(t *testing.T) {
	reader, err := NewGCSTarReader(client, "gs://m-lab-sandbox/test.tar")
	if err != nil {
		t.Fatal(err)
		return
	}
	reader.Close()
}

func TestNewTarReaderGzip(t *testing.T) {
	reader, err := NewGCSTarReader(client, "gs://m-lab-sandbox/test.tgz")
	if err != nil {
		t.Fatal(err)
		return
	}
	reader.Close()
}

// Using a persistent client saves about 80 msec, and 220 allocs, totalling 70kB.
var client *http.Client

func init() {
	var err error
	client, err = getStorageClient(false)
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
