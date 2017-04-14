package storage

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"io/ioutil"
	"strings"

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

// test utility, based on similar implementation in task.go
// Next reads the next test object from the tar file.
// Returns io.EOF when there are no more tests.
func next(tt TarReader) (string, []byte, error) {
	h, err := tt.Next()
	if err != nil {
		return "", nil, err
	}
	if h.Typeflag != tar.TypeReg {
		return h.Name, nil, nil
	}
	var data []byte
	if strings.HasSuffix(strings.ToLower(h.Name), "gz") {
		// TODO add unit test
		zipReader, err := gzip.NewReader(tt)
		if err != nil {
			return h.Name, nil, err
		}
		defer zipReader.Close()
		data, err = ioutil.ReadAll(zipReader)
	} else {
		data, err = ioutil.ReadAll(tt)
	}
	if err != nil {
		return h.Name, nil, err
	}
	return h.Name, data, nil
}

func TestNewTarReader(t *testing.T) {
	reader, err := NewGCSTarReader(client, "gs://m-lab-sandbox/test.tar")
	if err != nil {
		t.Fatal(err)
		return
	}
	count := 0
	for _, _, err := next(reader); err != io.EOF; _, _, err = next(reader) {
		if err != nil {
			t.Fatal(err)
			return
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
		return
	}
	count := 0
	for _, _, err := next(reader); err != io.EOF; _, _, err = next(reader) {
		if err != nil {
			t.Fatal(err)
			return
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
