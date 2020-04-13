// Package factory provides factory definitions.
// It may cause import cycles and have to be broken up.S
package factory

import (
	"context"
	"log"
	"net/http"

	v2 "github.com/m-lab/annotation-service/api/v2"

	"github.com/m-lab/etl/annotation"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/etl/storage"
	"github.com/m-lab/etl/task"
)

// ProcessingError extends error to provide  dataType and detail for metrics.
type ProcessingError struct {
	DataType string
	Detail   string
	Code     int
	error
}

// NewError creates a new ProcessingError.
func NewError(dt, detail string, code int, err error) *ProcessingError {
	return &ProcessingError{dt, detail, code, err}
}

// TaskFactory provides Get() which always returns a new, complete Task.
// TODO for the defs that stay in factory package, remove ...Factory.
type TaskFactory interface {
	Get(context.Context, etl.DataPath) (*task.Task, *ProcessingError)
}

// AnnotatorFactory provides Get() which always returns a new or existing Annotator.
type AnnotatorFactory interface {
	Get(context.Context, etl.DataPath) (v2.Annotator, *ProcessingError)
}

// SinkFactory provides Get() which may return a new or existing Sink.
// If existing Sink, the Commit method must support concurrent calls.
// Existing Sink may or may not respect the context.
type SinkFactory interface {
	Get(context.Context, etl.DataPath) (row.Sink, *ProcessingError)
}

// SourceFactory provides Get() which always produces a new TestSource.
type SourceFactory interface {
	Get(context.Context, etl.DataPath) (etl.TestSource, *ProcessingError)
}

type defaultAnnotatorFactory struct{}

// Get implements AnnotatorFactory.Get
func (ann *defaultAnnotatorFactory) Get(ctx context.Context, dp etl.DataPath) (v2.Annotator, *ProcessingError) {
	return v2.GetAnnotator(annotation.BatchURL), nil
}

// DefaultAnnotatorFactory returns the annotation service annotator.
func DefaultAnnotatorFactory() AnnotatorFactory {
	return &defaultAnnotatorFactory{}
}

type defaultSourceFactory struct{}

// Get implements SourceFactory.Get
func (ann *defaultSourceFactory) Get(ctx context.Context, dp etl.DataPath) (etl.TestSource, *ProcessingError) {
	label := dp.TableBase() // On error, this will be "invalid", so not all that useful.
	client, err := storage.GetStorageClient(false)
	if err != nil {
		log.Printf("Error getting storage client: %v\n", err)
		return nil, NewError(dp.DataType, "ServiceUnavailable",
			http.StatusServiceUnavailable, err)
	}

	// TODO - is this already handled upstream?
	dataType := dp.GetDataType()
	if dataType == etl.INVALID {
		return nil, NewError(dp.DataType, "InvalidDatatype",
			http.StatusInternalServerError, err)
	}

	tr, err := storage.NewTestSource(client, dp.URI, label)
	if err != nil {
		log.Printf("Error opening gcs file: %v", err)
		// TODO - anything better we could do here?
		return nil, NewError(dp.DataType, "ETLSourceError",
			http.StatusInternalServerError, err)
	}

	return tr, nil
}

// DefaultSourceFactory returns the default SourceFactory
func DefaultSourceFactory() SourceFactory {
	return &defaultSourceFactory{}
}
