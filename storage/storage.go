// GCS related utility functions to fetch and wrap objects with tar.Reader.
//
// Testing:
//   This has been manually tested, but test automation is probably not
//   worthwhile until there is an emulator for GCS.

package storage

import (
	"archive/tar"
	"compress/gzip"
	"encoding/base64"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/m-lab/etl/metrics"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	storage "google.golang.org/api/storage/v1"
)

type TarReader interface {
	Next() (*tar.Header, error)
	Read(b []byte) (int, error)
}

type ETLSource struct {
	TarReader
	io.Closer
}

// Next reads the next test object from the tar file.
// Returns io.EOF when there are no more tests.
func (rr *ETLSource) NextTest() (string, []byte, error) {
	metrics.WorkerState.WithLabelValues("read").Inc()
	defer metrics.WorkerState.WithLabelValues("read").Dec()

	h, err := rr.Next()
	if err != nil {
		return "", nil, err
	}
	if h.Typeflag != tar.TypeReg {
		return h.Name, nil, nil
	}
	var data []byte
	if strings.HasSuffix(strings.ToLower(h.Name), "gz") {
		// TODO add unit test
		zipReader, err := gzip.NewReader(rr)
		if err != nil {
			metrics.TaskCount.WithLabelValues("ETLSource", "zipReaderError").Inc()
			return h.Name, nil, err
		}
		defer zipReader.Close()
		data, err = ioutil.ReadAll(zipReader)
	} else {
		data, err = ioutil.ReadAll(rr)
	}
	if err != nil {
		// We are seeing these very rarely, maybe 1 per hour.
		if strings.Contains(err.Error(), "stream error") {
			metrics.TaskCount.WithLabelValues("ETLSource", "stream error").Inc()
		} else {
			metrics.TaskCount.WithLabelValues("ETLSource", "NextTest Error").Inc()
		}
		return h.Name, nil, err
	}
	return h.Name, data, nil
}

// Compound closer, for use with gzip files.
type Closer struct {
	zipper io.Closer // Must be non-null
	body   io.Closer // Must be non-null
}

func (t *Closer) Close() error {
	err := t.zipper.Close()
	t.body.Close()
	return err
}

var errNoClient = errors.New("client should be non-null")

// Create a ETLSource suitable for injecting into Task.
// Caller is responsible for calling Close on the returned object.
//
// uri should be of form gs://bucket/filename.tar or gs://bucket/filename.tgz
// FYI Using a persistent client saves about 80 msec, and 220 allocs, totalling 70kB.
// TODO(now) rename
func NewETLSource(client *http.Client, uri string) (*ETLSource, error) {
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

	obj, err := getObject(client, bucket, fn, 10*time.Minute)
	if err != nil {
		return nil, err
	}

	rdr := obj.Body
	var closer io.Closer = obj.Body
	// Handle .tar.gz, .tgz files.
	if strings.HasSuffix(strings.ToLower(fn), "gz") {
		// TODO add unit test
		// NB: This must not be :=, or it creates local rdr.
		rdr, err = gzip.NewReader(obj.Body)
		if err != nil {
			obj.Body.Close()
			return nil, err
		}

		closer = &Closer{rdr, obj.Body}
	}
	tarReader := tar.NewReader(rdr)

	return &ETLSource{tarReader, closer}, nil
}

// Create a storage reader client.
func GetStorageClient(writeAccess bool) (*http.Client, error) {
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
		return nil, err
	}
	return client, nil
}

// Turn the bytes received from the queue into a filename
// TODO(dev) Add unit test
func GetFilename(filename string) (string, error) {
	if strings.HasPrefix(filename, "gs://") {
		return filename, nil
	}

	decode, err := base64.StdEncoding.DecodeString(filename)
	if err != nil {
		return "", errors.New("invalid file path: " + filename)
	}
	fn := string(decode[:])
	if strings.HasPrefix(fn, "gs://") {
		return fn, nil
	}

	return "", errors.New("invalid base64 encoded file path: " + fn)
}

//---------------------------------------------------------------------------------
//          Local functions
//---------------------------------------------------------------------------------

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
