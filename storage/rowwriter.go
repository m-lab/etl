package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"

	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"google.golang.org/api/googleapi"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/factory"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
)

// ObjectWriter creates a writer to a named object.
// It may overwrite an existing object.
// Caller must Close() the writer, or cancel the context.
func ObjectWriter(ctx context.Context, client stiface.Client, bucket string, path string) stiface.Writer {
	b := client.Bucket(bucket)
	o := b.Object(path)
	w := o.NewWriter(ctx)
	// Set smaller chunk size to conserve memory.
	w.SetChunkSize(4 * 1024 * 1024)
	return w
}

// RowWriter implements row.Sink to a GCS file backend.
type RowWriter struct {
	w stiface.Writer

	bucket string
	path   string

	// These act as tokens to serialize access to the writer.
	// This allows concurrent encoding and writing, while ensuring
	// that single client access is correctly ordered.
	encoding chan struct{} // Token required for metric updates.
	writing  chan struct{} // Token required for metric updates.
}

// NewRowWriter creates a RowWriter.
func NewRowWriter(ctx context.Context, client stiface.Client, bucket string, path string) (row.Sink, error) {
	w := ObjectWriter(ctx, client, bucket, path)
	encoding := make(chan struct{}, 1)
	encoding <- struct{}{}
	writing := make(chan struct{}, 1)
	writing <- struct{}{}

	return &RowWriter{bucket: bucket, path: path, w: w, encoding: encoding, writing: writing}, nil
}

// Acquire the encoding token.
// TODO can we allow two encoders, and still sequence the writing?
func (rw *RowWriter) acquireEncodingToken() {
	<-rw.encoding
}

func (rw *RowWriter) releaseEncodingToken() {
	if len(rw.encoding) > 0 {
		log.Println("token error")
		return
	}
	rw.encoding <- struct{}{}
}

// Swap the encoding token for the write token.
// MUST already hold the write token.
func (rw *RowWriter) swapForWritingToken() {
	<-rw.writing
	rw.releaseEncodingToken()
}

func (rw *RowWriter) releaseWritingToken() {
	rw.writing <- struct{}{} // return the token.
}

// Commit commits rows, in order, to the GCS object.
// The GCS object is not available until Close is called, at which
// point the entire object becomes available atomically.
// The returned int is the number of rows written (and pending), or,
// if error is not nil, an estimate of the number of rows written.
func (rw *RowWriter) Commit(rows []interface{}, label string) (int, error) {
	rw.acquireEncodingToken()
	// First, do the encoding.  Other calls to Commit will block here
	// until encoding is done.
	// NOTE: This can cause a fairly hefty memory footprint for
	// large numbers of large rows.
	buf := bytes.NewBuffer(nil)

	for i := range rows {
		j, err := json.Marshal(rows[i])
		if err != nil {
			rw.releaseEncodingToken()
			metrics.BackendFailureCount.WithLabelValues(
				label, "encoding error").Inc()
			return 0, err
		}
		metrics.RowSizeHistogram.WithLabelValues(label).Observe(float64(len(j)))
		buf.Write(j)
		buf.WriteByte('\n')
	}
	numBytes := buf.Len()
	rw.swapForWritingToken()
	defer rw.releaseWritingToken()
	n, err := buf.WriteTo(rw.w) // This is buffered (by 4MB chunks).  Are the writes to GCS synchronous?
	if err != nil {
		switch typedErr := err.(type) {
		case *googleapi.Error:
			metrics.BackendFailureCount.WithLabelValues(
				label, "googleapi.Error").Inc()
			log.Println(typedErr, rw.bucket, rw.path)
			for _, e := range typedErr.Errors {
				log.Println(e)
			}
		default:
			metrics.BackendFailureCount.WithLabelValues(
				label, "other error").Inc()
			log.Println(typedErr, rw.bucket, rw.path)
		}
		// This approximates the number of rows written prior to error.
		// It is unclear whether these rows will actually show up.
		// The caller should likely abandon the archive at this point,
		// as further writing will likely result in a corrupted file.
		// See https://github.com/m-lab/etl/issues/899
		return int(n) * len(rows) / numBytes, err
	}

	// TODO - these may not be committed, so the returned value may be wrong.
	return len(rows), nil
}

// Close synchronizes on the tokens, and closes the backing file.
func (rw *RowWriter) Close() error {
	// Take BOTH tokens, to ensure no other goroutines are still running.
	<-rw.encoding
	<-rw.writing

	close(rw.encoding)
	close(rw.writing)

	log.Println("Closing", rw.bucket, rw.path)
	err := rw.w.Close()
	if err != nil {
		log.Println(err)
	} else {
		log.Println(rw.w.Attrs())
	}
	return err
}

// SinkFactory implements factory.SinkFactory.
type SinkFactory struct {
	client       stiface.Client
	outputBucket string
}

// returns the full path/filename from a gs://bucket/path/filename string.
func pathAndFilename(uri string) (string, error) {
	parts := strings.SplitN(uri, "/", 4)
	if len(parts) != 4 || parts[0] != "gs:" || len(parts[3]) == 0 {
		return "", errors.New("Bad GCS path")
	}
	return parts[3], nil
}

// Get implements factory.SinkFactory
func (sf *SinkFactory) Get(ctx context.Context, path etl.DataPath) (row.Sink, etl.ProcessingError) {

	fn, err := pathAndFilename(path.URI)
	if err != nil {
		return nil, factory.NewError(path.DataType, "InvalidPath",
			http.StatusInternalServerError, err)
	}
	s, err := NewRowWriter(context.Background(), sf.client, sf.outputBucket, fn+".json")
	if err != nil {
		return nil, factory.NewError(path.DataType, "SinkFactory",
			http.StatusInternalServerError, err)
	}
	return s, nil
}

// NewSinkFactory returns the default SinkFactory
func NewSinkFactory(client stiface.Client, outputBucket string) factory.SinkFactory {
	return &SinkFactory{client: client, outputBucket: outputBucket}
}
