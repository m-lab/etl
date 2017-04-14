// GCS related utility functions to fetch and wrap objects with tar.Reader.
//
// Testing:
//   This has been manually tested, but test automation is probably not
//   worthwhile until there is an emulator for GCS.

package storage

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	storage "google.golang.org/api/storage/v1"
	"io"
	"net/http"
	"strings"
	"time"
)

type TarReader interface {
	Next() (*tar.Header, error)
	Read(b []byte) (int, error)
}

type TarReaderCloser struct {
	TarReader
	zipper io.Closer // Must be non-null
	body   io.Closer // Must be non-null
}

// Implement io.Closer
func (t *TarReaderCloser) Close() error {
	err := t.zipper.Close()
	t.body.Close()
	return err
}

type NullCloser struct{}

func (c *NullCloser) Close() error { return nil }

var errNoClient = errors.New("client should be non-null")
var nullCloser io.Closer = new(NullCloser)

// Create a tar.Reader suitable for injecting into Task.
// Caller is responsible for calling Close on the returned object.
//
// uri should be of form gs://bucket/filename.tar or gs://bucket/filename.tgz
// FYI Using a persistent client saves about 80 msec, and 220 allocs, totalling 70kB.
func NewGCSTarReader(client *http.Client, uri string) (*TarReaderCloser, error) {
	if client == nil {
		return nil, errNoClient
	}
	// For now only handle gcs paths.
	if !strings.HasPrefix(uri, "gs://") {
		return nil, errors.New("invalid file path: " + uri)
	}
	parts := strings.SplitN(uri, "/", 4)
	if len(parts) != 4 {
		return nil, errors.New("invalid file path: " + uri)
	}
	bucket := parts[2]
	fn := parts[3]

	// TODO - consider just always testing for valid gzip file.
	if !(strings.HasSuffix(fn, ".tgz") || strings.HasSuffix(fn, ".tar") ||
		strings.HasSuffix(fn, ".tar.gz")) {
		return nil, errors.New("not tar or tgz: " + uri)
	}

	obj, err := getObject(client, bucket, fn, 60*time.Second)
	if err != nil {
		return nil, err
	}

	// Default nullCloser, if we don't need a gzip reader.
	zc := nullCloser

	rdr := obj.Body
	// Is it a tar.gz or tgz file?
	if strings.HasSuffix(strings.ToLower(fn), "gz") {
		// TODO add unit test
		var err error
		// NB: This must not be :=, or it creates local rdr.
		rdr, err = gzip.NewReader(obj.Body)
		if err != nil {
			obj.Body.Close()
			return nil, err
		}

		zc = rdr
	}
	tarReader := tar.NewReader(rdr)

	return &TarReaderCloser{tarReader, zc, obj.Body}, nil
}

//---------------------------------------------------------------------------------
//          Local functions
//---------------------------------------------------------------------------------

// Create a storage reader client.
func getStorageClient(writeAccess bool) (*http.Client, error) {
	var scope string
	if writeAccess {
		scope = storage.DevstorageReadWriteScope
	} else {
		scope = storage.DevstorageReadOnlyScope
	}

	// Use a short timeout, so we get an error quickly if there is a problem.
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := google.DefaultClient(ctx, scope)
	if err != nil {
		fmt.Printf("Unable to create client: %v\n", err)
		return nil, err
	}
	return client, nil
}

// Caller is responsible for closing response body.
func getObject(client *http.Client, bucket string, fn string, timeout time.Duration) (*http.Response, error) {
	// Lightweight, error only if client is nil.
	service, err := storage.New(client)
	if err != nil {
		return nil, err
	}

	// Lightweight - only setting up the local object.
	call := service.Objects.Get(bucket, fn)
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	call = call.Context(ctx)

	// Heavyweight.
	// Doesn't look like any googleapi.CallOptions are useful here.
	contentResponse, err := call.Download()
	if err != nil {
		return nil, err
	}
	return contentResponse, err
}
