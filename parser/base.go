package parser

import (
	"errors"
	"time"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/etl/annotation"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type Annotatable interface {
	GetLogTime() time.Time
	GetClientIP() string
	GetServerIP() string
	AnnotateClient(*api.GeoData) error
	AnnotateServer(*api.GeoData) error
}

type BaseRowBuffer struct {
	bufferSize int
	rows       []interface{} // Actually these are Annotatable, but we cast them later.
}

// AddRow simply inserts a row into the buffer.  Returns error if buffer is full.
// Not thread-safe.  Should only be called by owning thread.
func (buf *BaseRowBuffer) AddRow(row interface{}) error {
	for len(buf.rows) >= buf.bufferSize-1 {
		return etl.ErrBufferFull
	}
	buf.rows = append(buf.rows, row)
	return nil
}

// TakeRows returns all rows in the buffer, and clears the buffer.
// Not thread-safe.  Should only be called by owning thread.
func (buf *BaseRowBuffer) TakeRows() []interface{} {
	res := buf.rows
	buf.rows = make([]interface{}, 0, buf.bufferSize)
	return res
}

var (
	ErrAnnotationError = errors.New("Annotation error")
	ErrNotAnnotatable  = errors.New("object does not implement Annotatable")
)

// TODO update this to use local cache of high quality annotations.
func (buf *BaseRowBuffer) annotateServers() error {
	ipSlice := make([]string, len(buf.rows))
	logTime := time.Time{}
	for i := range buf.rows {
		r, ok := buf.rows[i].(Annotatable)
		if !ok {
			return ErrNotAnnotatable
		}
		ipSlice[i] = r.GetServerIP()
		if (logTime == time.Time{}) {
			logTime = r.GetLogTime()
		}
	}

	annSlice := annotation.FetchAllAnnotations(ipSlice, logTime)
	if annSlice == nil || len(annSlice) != len(ipSlice) {
		return ErrAnnotationError
	}

	for i := range buf.rows {
		r, ok := buf.rows[i].(Annotatable)
		if ok {
			r.AnnotateServer(annSlice[i])
		}
	}

	return nil
}

func (buf *BaseRowBuffer) annotateClients() error {
	ipSlice := make([]string, len(buf.rows))
	logTime := time.Time{}
	for i := range buf.rows {
		r, ok := buf.rows[i].(Annotatable)
		if !ok {
			return ErrNotAnnotatable
		}
		ipSlice[i] = r.GetClientIP()
		if (logTime == time.Time{}) {
			logTime = r.GetLogTime()
		}
	}

	annSlice := annotation.FetchAllAnnotations(ipSlice, logTime)
	if annSlice == nil || len(annSlice) != len(ipSlice) {
		return ErrAnnotationError
	}

	for i := range buf.rows {
		r, ok := buf.rows[i].(Annotatable)
		if ok {
			r.AnnotateClient(annSlice[i])
		}
	}

	return nil
}

// Annotate fetches annotations for all rows in the buffer.
// Not thread-safe.  Should only be called by owning thread.
// TODO should convert this to operate on the rows, instead of the buffer.
// Then we can do it after TakeRows().
func (buf *BaseRowBuffer) Annotate(metricLabel string) error {
	metrics.WorkerState.WithLabelValues(metricLabel, "annotate").Inc()
	defer metrics.WorkerState.WithLabelValues(metricLabel, "annotate").Dec()
	if len(buf.rows) == 0 {
		return nil
	}
	start := time.Now()

	// TODO Consider doing these in parallel?
	err := buf.annotateClients()
	if err != nil {
		return err
	}

	err = buf.annotateServers()
	if err != nil {
		return err
	}

	metrics.AnnotationTimeSummary.With(prometheus.Labels{"test_type": metricLabel}).Observe(float64(time.Since(start).Nanoseconds()))
	return nil
}

// Base provides common parser functionality.
type Base struct {
	etl.Inserter
	BaseRowBuffer
}

// NewBase creates a new sidestream parser.
func NewBase(ins etl.Inserter, bufSize int) *Base {
	buf := BaseRowBuffer{bufSize, make([]interface{}, 0, bufSize)}
	return &Base{ins, buf}
}

// TaskError return the task level error, based on failed rows, or any other criteria.
func (pb *Base) TaskError() error {
	return nil
}

// Flush flushes any pending rows.
// Caller should generally call Annotate first.
func (pb *Base) Flush() error {
	pb.Put(pb.TakeRows())
	return pb.Inserter.Flush()
}
