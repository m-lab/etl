package parser

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/m-lab/annotation-service/api"
	v2as "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// Errors that may be returned by BaseRowBuffer functions.
var (
	ErrAnnotationError = errors.New("Annotation error")
	ErrNotAnnotatable  = errors.New("object does not implement Annotatable")
	ErrRowNotPointer   = errors.New("Row should be a pointer type")
)

// Annotatable interface enables integration of annotation into a parser.
// The row type should implement the interface, and the annotations will be added
// prior to insertion.
type Annotatable interface {
	GetLogTime() time.Time
	GetClientIPs() []string // This is a slice to support mutliple hops in traceroute data.
	GetServerIP() string
	AnnotateClients(map[string]*api.Annotations) error
	AnnotateServer(*api.Annotations) error
}

// RowBuffer provides all basic functionality generally needed for buffering, annotating, and inserting
// rows that implement Annotatable.
type RowBuffer struct {
	bufferSize int
	rows       []interface{} // Actually these are Annotatable, but we cast them later.
	ann        v2as.Annotator
}

// AddRow simply inserts a row into the buffer.  Returns error if buffer is full.
// Not thread-safe.  Should only be called by owning thread.
func (buf *RowBuffer) AddRow(row interface{}) error {
	if reflect.TypeOf(row).Kind() != reflect.Ptr {
		return ErrRowNotPointer
	}
	for len(buf.rows) >= buf.bufferSize-1 {
		return etl.ErrBufferFull
	}
	buf.rows = append(buf.rows, row)
	return nil
}

// TakeRows returns all rows in the buffer, and clears the buffer.
// Not thread-safe.  Should only be called by owning thread.
func (buf *RowBuffer) TakeRows() []interface{} {
	res := buf.rows
	buf.rows = make([]interface{}, 0, buf.bufferSize)
	return res
}

// TODO update this to use local cache of high quality annotations.
func (buf *RowBuffer) annotateServers() error {
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

	response, err := buf.ann.GetAnnotations(context.Background(), logTime, ipSlice)
	if err != nil {
		return err
	}
	annMap := response.Annotations
	if annMap == nil {
		return ErrAnnotationError
	}

	for i := range buf.rows {
		r, ok := buf.rows[i].(Annotatable)
		if !ok {
			err = ErrNotAnnotatable
		} else {
			r.AnnotateServer(annMap[ipSlice[i]])
		}
	}

	return err
}

func (buf *RowBuffer) annotateClients() error {
	ipSlice := make([]string, 0, 2*len(buf.rows)) // This may be inadequate, but its a reasonable start.
	logTime := time.Time{}
	for i := range buf.rows {
		r, ok := buf.rows[i].(Annotatable)
		if !ok {
			return ErrNotAnnotatable
		}
		ipSlice = append(ipSlice, r.GetClientIPs()...)
		if (logTime == time.Time{}) {
			logTime = r.GetLogTime()
		}
	}

	response, err := buf.ann.GetAnnotations(context.Background(), logTime, ipSlice)
	if err != nil {
		return err
	}
	annMap := response.Annotations
	if annMap == nil {
		return ErrAnnotationError
	}

	for i := range buf.rows {
		r, ok := buf.rows[i].(Annotatable)
		if !ok {
			err = ErrNotAnnotatable
		} else {
			r.AnnotateClients(annMap)
		}
	}

	return err
}

// Annotate fetches annotations for all rows in the buffer.
// Not thread-safe.  Should only be called by owning thread.
// TODO should convert this to operate on the rows, instead of the buffer.
// Then we can do it after TakeRows().
func (buf *RowBuffer) Annotate(metricLabel string) error {
	metrics.WorkerState.WithLabelValues(metricLabel, "annotate").Inc()
	defer metrics.WorkerState.WithLabelValues(metricLabel, "annotate").Dec()
	if len(buf.rows) == 0 {
		return nil
	}
	start := time.Now()
	defer metrics.AnnotationTimeSummary.With(prometheus.Labels{"test_type": metricLabel}).Observe(float64(time.Since(start).Nanoseconds()))

	// TODO Consider doing these in parallel?
	clientErr := buf.annotateClients()
	serverErr := buf.annotateServers()

	if clientErr != nil {
		return clientErr
	}

	if serverErr != nil {
		return serverErr
	}

	return nil
}

// Base provides common parser functionality.
type Base struct {
	etl.Inserter
	RowBuffer
}

// NewBase creates a new sidestream parser.
func NewBase(ins etl.Inserter, bufSize int, ann v2as.Annotator) *Base {
	buf := RowBuffer{bufSize, make([]interface{}, 0, bufSize), ann}
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
