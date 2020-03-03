package storage

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
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
	// These act as tokens to serialize access to the writer.
	// This allows concurrent encoding and writing, while ensuring
	// that single client access is correctly ordered.
	encoding chan struct{} // Token required for metric updates.
	writing  chan struct{} // Token required for metric updates.
}

// NewRowWriter creates a RowWriter.
func NewRowWriter(ctx context.Context, client stiface.Client, bucket string, path string) (*RowWriter, error) {
	w := ObjectWriter(ctx, client, bucket, path)
	encoding := make(chan struct{}, 1)
	encoding <- struct{}{}
	writing := make(chan struct{}, 1)
	writing <- struct{}{}

	return &RowWriter{w: w, encoding: encoding, writing: writing}, nil
}

// Acquire the encoding token.
// TODO can we allow two encoders, and still sequence the writing?
func (rw *RowWriter) encodeToken() {
	<-rw.encoding
}

// Swap the encoding token for the write token.
// MUST already hold the write token.
func (rw *RowWriter) writeToken() {
	<-rw.writing
	rw.encoding <- struct{}{}
}

func (rw *RowWriter) release() {
	rw.writing <- struct{}{} // return the token.
}

// Commit commits rows, in order, to the GCS object.
func (rw *RowWriter) Commit(rows []interface{}, label string) error {
	rw.encodeToken()
	// First, do the encoding.  Other calls to Commit will block here
	// until encoding is done.
	// NOTE: This can cause a fairly hefty memory footprint for
	// large numbers of large rows.
	buf := bytes.NewBuffer(nil)

	for i := range rows {
		j, err := json.Marshal(rows[i])
		if err != nil {
			return err
		}
		json.Compact(buf, j)
		buf.WriteByte('\n')
	}
	// Exchange the encoding token for the writing token.  Another caller
	// will be allowed to start encoding.
	rw.writeToken()
	_, err := buf.WriteTo(rw.w) // This is buffered (by 4MB chunks).  Are the writes to GCS synchronous?
	if err != nil {
		return err
	}

	// Release the write token.  Another caller will be allowed to start writing.
	rw.release()
	return nil
}

// Close synchronizes on the tokens, and closes the backing file.
func (rw *RowWriter) Close() error {
	// Take BOTH tokens, to ensure no other goroutines are still running.
	<-rw.encoding
	<-rw.writing
	return rw.w.Close()
}
