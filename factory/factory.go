// Package factory provides factory definitions.
// It may cause import cycles and have to be broken up.S
package factory

import (
	"context"

	"github.com/m-lab/annotation-service/api/v2"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/row"
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
	Get(context.Context, etl.DataPath) (api.Annotator, *ProcessingError)
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
