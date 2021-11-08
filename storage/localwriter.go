package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/factory"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
)

// LocalWriter provides a Sink interface for parsers to output to local files.
type LocalWriter struct {
	f    *os.File
	rows int
}

// NewLocalWriter creates a new LocalWriter for output to the given dir and
// path. On success, missing directories are created and a new file pointer is
// allocated. Callers must call Close() to release this file pointer.
func NewLocalWriter(dir string, path string) (row.Sink, error) {
	p := filepath.Join(dir, path)
	d := filepath.Dir(p) // path may include additional directory elements.
	err := os.MkdirAll(d, os.ModePerm)
	if err != nil {
		return nil, err
	}
	// All rows from an archive are appended in a single session, so this
	// does not need O_APPEND.
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	l := &LocalWriter{
		f: f,
	}
	return l, nil
}

// Commit writes the given rows to the local writer file immediately. This is in
// contrast to the bigquery RowWriter does not make content available until
// Close.
func (lw *LocalWriter) Commit(rows []interface{}, label string) (int, error) {
	buf := bytes.NewBuffer(nil)

	for i := range rows {
		j, err := json.Marshal(rows[i])
		if err != nil {
			metrics.BackendFailureCount.WithLabelValues(label, "encoding error").Inc()
			return 0, err
		}
		metrics.RowSizeHistogram.WithLabelValues(label).Observe(float64(len(j)))
		buf.Write(j)
		buf.WriteByte('\n')
	}
	_, err := buf.WriteTo(lw.f)
	if err != nil {
		return 0, err
	}
	lw.rows += len(rows)
	return len(rows), nil
}

// Close closes the underlying LocalWriter file object.
func (lw *LocalWriter) Close() error {
	err := lw.f.Close()
	if err != nil {
		return err
	}
	log.Printf("Successful LocalWriter.Close(); wrote %d rows to %s", lw.rows, lw.f.Name())
	return nil
}

// LocalFactory creates LocalWriters sinks within a given output directory.
type LocalFactory struct {
	outputDir string
}

// Get implements factory.SinkFactory for LocalWriters.
func (lf *LocalFactory) Get(ctx context.Context, dp etl.DataPath) (row.Sink, etl.ProcessingError) {
	s, err := NewLocalWriter(lf.outputDir, dp.Path+".jsonl")
	if err != nil {
		return nil, factory.NewError(dp.DataType, "LocalFactory", http.StatusInternalServerError, err)
	}
	return s, nil
}

// NewLocalFactory creates a new LocalFactory that produces LocalWriters that
// output to the named output directory.
func NewLocalFactory(outputDir string) factory.SinkFactory {
	return &LocalFactory{
		outputDir: outputDir,
	}
}
