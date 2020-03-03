package storage

import (
	"context"
	"encoding/json"

	"github.com/googleapis/google-cloud-go-testing/storage/stiface"

	"github.com/tidwall/pretty"
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

type RowWriter struct {
	w        stiface.Writer
	encoding chan struct{} // Token required for metric updates.
	writing  chan struct{} // Token required for metric updates.
}

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
func (rw *RowWriter) encode() {
	<-rw.encoding
}

// Swap the encoding token for the writing token.
// MUST already hold the writing token.
func (rw *RowWriter) write() {
	<-rw.writing
	rw.encoding <- struct{}{}
}

func (rw *RowWriter) done() {
	rw.writing <- struct{}{} // return the token.
}

// Commit commits rows, in order, to the GCS object.
func (rw *RowWriter) Commit(rows []interface{}, label string) error {
	rw.encode()
	// Do the encoding
	var j []byte
	for i := range rows {
		var err error
		j, err = json.Marshal(rows[i])
		if err != nil {
			return err
		}
		pretty.UglyInPlace(j)
		j := append(j, '\n')
		rw.w.Write(j) // This is buffered (by 4MB chunks).  Are the writes to GCS synchronous?
	}

	rw.write()
	rw.done()
	return nil
}

func (rw *RowWriter) Close() error {
	// Take BOTH tokens, to ensure no other goroutines are still running.
	<-rw.encoding
	<-rw.writing
	return rw.w.Close()
}
