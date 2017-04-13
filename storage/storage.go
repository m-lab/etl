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
	io.Closer
}

type C struct {
	closer func() error
}

// Implement io.Closer
func (c *C) Close() error { return c.closer() }

// Create a tar.Reader suitable for injecting into Task.
// Caller is responsible for calling Close on the returned object.
//
// uri should be of form gs://bucket/filename.tar or gs://bucket/filename.tgz
// FYI Using a persistent client saves about 80 msec, and 220 allocs, totalling 70kB.
func NewGCSTarReader(client *http.Client, uri string) (*TarReaderCloser, error) {
	if client == nil {
		var err error
		client, err = getStorageClient(false)
		if err != nil {
			return nil, err
		}
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

	if !(strings.HasSuffix(fn, ".tgz") || strings.HasSuffix(fn, ".tar") ||
		strings.HasSuffix(fn, ".tar.gz")) {
		return nil, errors.New("not tar or tgz: " + uri)
	}

	obj, err := getObject(client, bucket, fn, 60*time.Second)
	if err != nil {
		return nil, err
	}

	// Wrap with a tar.Reader that also has a response closer.
	rdr := obj.Body
	closer := obj.Body.Close

	// Is it a tar.gz or tgz file?
	if strings.HasSuffix(strings.ToLower(fn), "gz") {
		// TODO add unit test
		rdr, err := gzip.NewReader(obj.Body)
		if err != nil {
			obj.Body.Close()
			return nil, err
		}
		closer = func() error {
			rdr.Close()
			obj.Body.Close()
			// TODO handle errors?
			return nil
		}
	}
	tarReader := tar.NewReader(rdr)
	// No closer needed for tar.Reader

	return &TarReaderCloser{tarReader, &C{closer}}, nil
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
