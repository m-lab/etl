package storage

import (
	"testing"
	"time"
)

func TestGetObject(t *testing.T) {

	client, err := getClient()
	if err != nil {
		t.Fatal(err)
		return
	}
	obj, err := getObject(client, "m-lab-sandbox", "testfile", 10*time.Second)
	if err != nil {
		t.Fatal(err)
		return
	}
	obj.Body.Close()
}

func TestNewTarReader(t *testing.T) {

	reader, err := newGCSTarReader("gs://m-lab-sandbox/testfile")
	if err != nil {
		t.Fatal(err)
		return
	}
	reader.Close()
}
