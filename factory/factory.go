// Package factory provides factories for constructing Task components.
package factory

import (
	"context"
	"fmt"
	"log"
	"net/http"

	gcs "cloud.google.com/go/storage"
	v2 "github.com/m-lab/annotation-service/api/v2"

	"github.com/m-lab/etl/annotation"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/etl/storage"
	"github.com/m-lab/etl/task"
)

// ProcessingError extends error to provide dataType and detail for metrics,
// and appropriate return codes for http handlers.
type ProcessingError interface {
	DataType() string
	Detail() string
	Code() int
	error
}

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
func NewError(dt, detail string, code int, err error) ProcessingError {
	return processingError{dt, detail, code, err}
}

// TaskFactory provides Get() which always returns a new, complete Task.
// TODO for the defs that stay in factory package, remove ...Factory.
type TaskFactory interface {
	Get(context.Context, etl.DataPath) (*task.Task, ProcessingError)
}

// AnnotatorFactory provides Get() which always returns a new or existing Annotator.
type AnnotatorFactory interface {
	Get(context.Context, etl.DataPath) (v2.Annotator, ProcessingError)
}

// SinkFactory provides Get() which may return a new or existing Sink.
// If existing Sink, the Commit method must support concurrent calls.
// Existing Sink may or may not respect the context.
type SinkFactory interface {
	Get(context.Context, etl.DataPath) (row.Sink, ProcessingError)
}

// SourceFactory provides Get() which always produces a new TestSource.
type SourceFactory interface {
	Get(context.Context, etl.DataPath) (etl.TestSource, ProcessingError)
}

//=======================================================================
//  Implementations
//=======================================================================

type defaultAnnotatorFactory struct{}

// Get implements AnnotatorFactory.Get
func (ann *defaultAnnotatorFactory) Get(ctx context.Context, dp etl.DataPath) (v2.Annotator, ProcessingError) {
	return v2.GetAnnotator(annotation.BatchURL), nil
}

// DefaultAnnotatorFactory returns the annotation service annotator.
func DefaultAnnotatorFactory() AnnotatorFactory {
	return &defaultAnnotatorFactory{}
}

// TODO - might be preferable to put this in storage package, but that
// currently creates an import cycle.  task -> storage -> factory -> task ?
// Will likely refactor in a later PR in a few days.
type gcsSourceFactory struct {
	client *gcs.Client
}

// Get implements SourceFactory.Get
func (sf *gcsSourceFactory) Get(ctx context.Context, dp etl.DataPath) (etl.TestSource, ProcessingError) {
	label := dp.TableBase() // On error, this will be "invalid", so not all that useful.
	// TODO - is this already handled upstream?
	dataType := dp.GetDataType()
	if dataType == etl.INVALID {
		return nil, NewError(dp.DataType, "InvalidDatatype",
			http.StatusInternalServerError, etl.ErrBadDataType)
	}

	tr, err := storage.NewTestSource(sf.client, dp.URI, label)
	if err != nil {
		log.Printf("Error opening gcs file: %v", err)
		// TODO - anything better we could do here?
		return nil, NewError(dp.DataType, "ETLSourceError",
			http.StatusInternalServerError,
			fmt.Errorf("ETLSourceError %w", err))
	}

	return tr, nil
}

// GCSSourceFactory returns the default SourceFactory
func GCSSourceFactory(c *gcs.Client) SourceFactory {
	return &gcsSourceFactory{c}
}
