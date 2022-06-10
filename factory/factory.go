// Package factory provides factories for constructing Task components.
package factory

import (
	"context"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/row"
)

type processingError struct {
	dataType string
	detail   string
	code     int
	error
}

func (pe processingError) DataType() string {
	return pe.dataType
}

func (pe processingError) Detail() string {
	return pe.detail
}

func (pe processingError) Code() int {
	return pe.code
}

// NewError creates a new ProcessingError.
func NewError(dt, detail string, code int, err error) etl.ProcessingError {
	return processingError{dt, detail, code, err}
}

// SinkFactory provides Get() which may return a new or existing Sink.
// If existing Sink, the Commit method must support concurrent calls.
// Existing Sink may or may not respect the context.
type SinkFactory interface {
	Get(context.Context, etl.DataPath) (row.Sink, etl.ProcessingError)
}

// SourceFactory provides Get() which always produces a new TestSource.
type SourceFactory interface {
	Get(context.Context, etl.DataPath) (etl.TestSource, etl.ProcessingError)
}
