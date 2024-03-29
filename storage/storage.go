// GCS related utility functions to fetch and wrap objects with tar.Reader.
//
// Testing:
//   This has been manually tested, but test automation is probably not
//   worthwhile until there is an emulator for GCS.

package storage

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/civil"
	gcs "cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/factory"
	"github.com/m-lab/etl/metrics"
)

// ErrOversizeFile is returned when exceptionally large files are skipped.
var ErrOversizeFile = errors.New("Oversize file")

// TarReader provides Next and Read functions.
type TarReader interface {
	Next() (*tar.Header, error)
	Read(b []byte) (int, error)
}

// GCSSource wraps a gsutil tar file containing tests.
type GCSSource struct {
	FilePath      string
	Size          int64
	TarReader                   // TarReader interface provided by an embedded struct.
	io.Closer                   // Closer interface to be provided by an embedded struct.
	RetryBaseTime time.Duration // The base time for backoff and retry.
	TableBase     string        // TableBase is BQ table associated with this source, or "invalid".
	PathDate      civil.Date    // Date associated with YYYY/MM/DD in FilePath.
}

// Retrieve next file header.
// Lots of error handling because of common faults in underlying GCS.
func (src *GCSSource) nextHeader(trial int) (*tar.Header, bool, error) {
	h, err := src.Next()
	if err != nil {
		if err == io.EOF {
			return nil, false, err
		} else if strings.Contains(err.Error(), "unexpected EOF") {
			metrics.GCSRetryCount.WithLabelValues(
				src.TableBase, "nextHeader", strconv.Itoa(trial), "unexpected EOF").Inc()
			// TODO: These are likely unrecoverable, so we should
			// probably return false.
		} else {
			// Quite a few of these now, and they seem to be
			// unrecoverable.
			metrics.GCSRetryCount.WithLabelValues(
				src.TableBase, "nextHeader", strconv.Itoa(trial), "other").Inc()
		}
		log.Printf("ERROR: nextHeader: %v\n", err)
	}
	return h, true, err
}

// Retrieve the data for a single file.
// Lots of error handling because of common faults in underlying GCS.
// Returns data in byte array, error and boolean regarding whether to retry.
func (src *GCSSource) nextData(h *tar.Header, trial int) ([]byte, bool, error) {
	var data []byte
	var err error
	var phase string
	if strings.HasSuffix(strings.ToLower(h.Name), "gz") {
		// TODO add unit test
		var zipReader *gzip.Reader
		zipReader, err = gzip.NewReader(src)
		if err != nil {
			if err == io.EOF {
				return nil, false, err
			}
			metrics.GCSRetryCount.WithLabelValues(
				src.TableBase, "open zip", strconv.Itoa(trial), "zipReaderError").Inc()
			log.Printf("ERROR: zipReader(%d): %v in file %s\n", trial, err, h.Name)
			return nil, true, err
		}
		defer zipReader.Close()
		phase = "nextData zip"
		data, err = ioutil.ReadAll(zipReader)
	} else {
		phase = "nextData"
		data, err = ioutil.ReadAll(src)
	}
	if err != nil {
		// These errors seem to be recoverable, at least with zip files.
		if strings.Contains(err.Error(), "stream error") {
			// We are seeing these very rarely, maybe 1 per hour.
			// They are non-deterministic, so probably related to GCS problems.
			metrics.GCSRetryCount.WithLabelValues(
				src.TableBase, phase, strconv.Itoa(trial), "stream error").Inc()
		} else if strings.Contains(err.Error(), "unexpected EOF") {
			// We ARE seeing 438 of these for ndt7/read-zip, June 2020.
			// They are consistent for each reprocessing.
			// They occur when there is a truncated gz file within an archive.  For example:
			//   ERROR nextData:1 [unexpected EOF] 2020/04/20/ndt7-upload-20200420T051229.808987688Z.ndt-s5hxm_1583551492_000000000021ADBB.json.gz
			//   (10 bytes) from gs://archive-measurement-lab/ndt/ndt7/2020/04/20/20200420T060456.005849Z-ndt7-mlab3-lhr03-ndt.tgz
			metrics.GCSRetryCount.WithLabelValues(
				src.TableBase, phase, strconv.Itoa(trial), "unexpected EOF").Inc()
		} else {
			metrics.GCSRetryCount.WithLabelValues(
				src.TableBase, phase, strconv.Itoa(trial), "other error").Inc()
		}
		log.Printf("ERROR: nextData:%d [%s] %s (%d bytes) from %s\n", trial, err, h.Name, h.Size, src.FilePath)
		return nil, true, err
	}

	return data, false, nil
}

// Type returns a string for use in metrics and logs.
func (src *GCSSource) Type() string {
	return src.TableBase
}

// Detail returns a string for use in logs.
func (src *GCSSource) Detail() string {
	return src.FilePath
}

// Date returns a civil.Date associated with the GCSSource archive path.
func (src *GCSSource) Date() civil.Date {
	return src.PathDate
}

// GetSize returns a size of the archive at the GCSSource archive path.
func (src *GCSSource) GetSize() int64 {
	return src.Size
}

// NextTest reads the next test object from the tar file.
// Skips reading contents of any file larger than maxSize, returning empty data
// and storage.ErrOversizeFile.
// Returns io.EOF when there are no more tests.
func (src *GCSSource) NextTest(maxSize int64) (string, []byte, error) {
	metrics.WorkerState.WithLabelValues(src.TableBase, "read").Inc()
	defer metrics.WorkerState.WithLabelValues(src.TableBase, "read").Dec()

	// Try to get the next file.  We retry multiple times, because sometimes
	// GCS stalls and produces stream errors.
	var err error
	var data []byte
	var h *tar.Header

	// With default RetryBaseTime, the last trial will be after total delay of
	// 16ms + 32ms + ... + 8192ms, or about 15 seconds.
	// TODO - should add a random element to the backoff?
	trial := 0
	delay := src.RetryBaseTime
	for {
		trial++
		var retry bool
		h, retry, err = src.nextHeader(trial)
		if err == nil {
			break
		}
		if !retry || trial >= 10 {
			return "", nil, err
		}
		// For each trial, increase backoff delay by 2x.
		delay *= 2
		time.Sleep(delay)
	}

	if h.Size > maxSize {
		return h.Name, data, ErrOversizeFile
	}

	// Only process regular files.
	if h.Typeflag != tar.TypeReg {
		return h.Name, data, nil
	}

	trial = 0
	delay = src.RetryBaseTime
	for {
		trial++
		var retry bool
		data, retry, err = src.nextData(h, trial)
		if err == nil {
			break
		}
		if !retry || trial >= 10 {
			// FYI, it appears that stream errors start in the
			// nextData phase of reading, but then persist on
			// the next call to nextHeader.
			break
		}
		// For each trial, increase backoff delay by 2x.
		delay *= 2
		time.Sleep(delay)
	}

	return h.Name, data, nil
}

// Closer handles gzip files.
type Closer struct {
	zipper io.Closer // Must be non-null
	rdr    io.Closer // Must be non-null
	cancel func()    // Context cancel.
}

// Close invokes the gzip and body Close() functions.
func (t *Closer) Close() error {
	defer t.cancel()
	var err error
	if t.zipper != nil {
		err = t.zipper.Close()
		t.rdr.Close() // ignoring this error?
	} else {
		err = t.rdr.Close()
	}
	return err
}

var errNoClient = errors.New("client should be non-null")

// NewTestSource creates an TestSource suitable for injecting into Task.
// Caller is responsible for calling Close on the returned object.
//
// uri should be of form gs://bucket/filename.tar or gs://bucket/filename.tgz
// FYI Using a persistent client saves about 80 msec, and 220 allocs, totalling 70kB.
func NewTestSource(client stiface.Client, dp etl.DataPath, label string) (etl.TestSource, error) {
	if client == nil {
		return nil, errNoClient
	}
	// For now only handle gcs paths.
	if !strings.HasPrefix(dp.URI, "gs://") {
		return nil, errors.New("invalid file path: " + dp.URI)
	}
	bucket := dp.Bucket
	fn := dp.Path

	archiveDate, err := time.Parse("2006/01/02", dp.DatePath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse archive date path: %w", err)
	}

	// TODO - consider just always testing for valid gzip file.
	if !(strings.HasSuffix(fn, ".tgz") || strings.HasSuffix(fn, ".tar") ||
		strings.HasSuffix(fn, ".tar.gz")) {
		return nil, errors.New("not tar or tgz: " + dp.URI)
	}

	ctx, cancel := context.WithCancel(context.Background())
	// TODO(prod) Evaluate whether timeout this is long enough.
	// TODO - appengine requests time out after 60 minutes, so more than that doesn't help.
	// SS processing sometimes times out with 1 hour.
	// Is there a limit on http requests from task queue, or into flex instance?
	rdr, size, err := getReader(ctx, client, bucket, fn, 300*time.Minute)
	if err != nil {
		cancel()
		log.Println(err)
		return nil, err
	}

	closer := &Closer{nil, rdr, cancel}
	// Handle .tar.gz, .tgz files.
	if strings.HasSuffix(strings.ToLower(fn), "gz") {
		// TODO add unit test
		// NB: This must not be :=, or it creates local rdr.
		// TODO - add retries with backoff.
		gzRdr, err := gzip.NewReader(rdr)
		if err != nil {
			closer.Close()
			log.Println(err)
			return nil, err
		}
		closer.zipper = gzRdr
		rdr = gzRdr
	}
	tarReader := tar.NewReader(rdr)

	baseTimeout := 16 * time.Millisecond
	gcs := &GCSSource{
		FilePath:      dp.URI,
		Size:          size,
		TarReader:     tarReader,
		Closer:        closer,
		RetryBaseTime: baseTimeout,
		TableBase:     label,
		PathDate:      civil.DateOf(archiveDate),
	}
	return gcs, nil
}

// GetStorageClient provides a storage reader client.
// This contacts the backend server, so should be used infrequently.
func GetStorageClient(writeAccess bool) (stiface.Client, error) {
	var scope string
	if writeAccess {
		scope = gcs.ScopeReadWrite
	} else {
		scope = gcs.ScopeReadOnly
	}

	// This cannot include a defer cancel, as the client then doesn't work after
	// the cancel.
	client, err := gcs.NewClient(context.Background(), option.WithScopes(scope))
	if err != nil {
		return nil, err
	}
	return stiface.AdaptClient(client), nil
}

type gcsSourceFactory struct {
	client stiface.Client
}

// Get implements SourceFactory.Get
func (sf *gcsSourceFactory) Get(ctx context.Context, dp etl.DataPath) (etl.TestSource, etl.ProcessingError) {
	label := dp.TableBase() // On error, this will be "invalid", so not all that useful.
	// TODO - is this already handled upstream?
	dataType := dp.GetDataType()
	if dataType == etl.INVALID {
		return nil, factory.NewError(dp.DataType, "InvalidDatatype",
			http.StatusInternalServerError, etl.ErrBadDataType)
	}

	tr, err := NewTestSource(sf.client, dp, label)
	if err != nil {
		log.Printf("ERROR: opening gcs file: %v", err)
		// TODO - anything better we could do here?
		return nil, factory.NewError(dp.DataType, "ETLSourceError",
			http.StatusInternalServerError,
			fmt.Errorf("ETLSourceError %w", err))
	}

	return tr, nil
}

// GCSSourceFactory returns the default SourceFactory
func GCSSourceFactory(c stiface.Client) factory.SourceFactory {
	return &gcsSourceFactory{c}
}

//---------------------------------------------------------------------------------
//          Local functions
//---------------------------------------------------------------------------------

// Caller is responsible for closing response body.
func getReader(ctx context.Context, client stiface.Client, bucket string, fn string, timeout time.Duration) (io.ReadCloser, int64, error) {
	// Lightweight - only setting up the local object.
	b := client.Bucket(bucket)
	obj := b.Object(fn)
	rdr, err := obj.NewReader(ctx)
	if err != nil {
		return rdr, 0, err
	}
	attr, err := obj.Attrs(ctx)
	if err != nil {
		// rdr is ok, but attribute not available
		return rdr, 0, err
	}
	return rdr, attr.Size, err
}
