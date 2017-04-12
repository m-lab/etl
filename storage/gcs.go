package storage

import (
	"archive/tar"
	"compress/gzip"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	storage "google.golang.org/api/storage/v1"
	"net/http"
	"strings"
	"time"
)

type TarReaderCloser struct {
	*tar.Reader
	// Should this be a slice?
	closer func() error
}

func (trc *TarReaderCloser) Close() {
	trc.closer()
}

// TODO - is gensupport.URLParams useful?
// Create a tar.Reader suitable for injecting into Task.
// When do we close the response body?
func newTarReader(uri string) (*TarReaderCloser, error) {

	client, err := getClient()
	if err != nil {
		return nil, err
	}
	obj, err := getObject(client, uri, 60*time.Second)
	if err != nil {
		return nil, err
	}

	// Wrap with a tar.Reader that also has a response closer.

	// Is it a tar or tgz file?
	rdr := obj.Body
	closer := obj.Body.Close

	if strings.HasSuffix(strings.ToLower(uri), ".gz") {
		// TODO add unit test
		rdr, err := gzip.NewReader(obj.Body)
		if err != nil {
			closer()
			return nil, err
		}
		c := func() error {
			defer closer()
			rdr.Close()
			// TODO handle errors?
			return nil
		}
		closer = c
	}
	tarReader := tar.NewReader(rdr)
	// No closer needed for tar.Reader

	return &TarReaderCloser{tarReader, closer}, nil
}

func getClient() (*http.Client, error) {
	// TODO - is this the scope we want?
	scope := storage.DevstorageFullControlScope
	// Use a short timeout, so we get an error quickly if there is a problem.
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := google.DefaultClient(ctx, scope)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// Caller responsible for closing response body.
func getObject(client *http.Client, uri string, timeout time.Duration) (*http.Response, error) {
	// Lightweight, error only if client is nil.
	service, err := storage.New(client)
	if err != nil {
		return nil, err
	}

	// How do we parse the uri to get the bucket and object names?
	call := service.Objects.Get("", "") // sourceBucket, objectName)
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
