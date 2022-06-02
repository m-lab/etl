// Package factory provides factories for constructing Task components.
package factory

import (
	"context"

	v2 "github.com/m-lab/annotation-service/api/v2"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/parser"
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

// AnnotatorFactory provides Get() which always returns a new or existing Annotator.
type AnnotatorFactory interface {
	Get(context.Context, etl.DataPath) (v2.Annotator, etl.ProcessingError)
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

//=======================================================================
//  Implementations
//=======================================================================

type defaultAnnotatorFactory struct{}

// Get implements AnnotatorFactory.Get
func (ann *defaultAnnotatorFactory) Get(ctx context.Context, dp etl.DataPath) (v2.Annotator, etl.ProcessingError) {
	return &parser.NullAnnotator{}, nil
}

// DefaultAnnotatorFactory returns the annotation service annotator.
func DefaultAnnotatorFactory() AnnotatorFactory {
	return &defaultAnnotatorFactory{}
}
