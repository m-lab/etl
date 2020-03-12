package parser

// TODO integrate this functionality into the parser.go code.
// Probably should have Base implement Parser.

import (
	"context"
	"errors"
	"log"
	"reflect"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	v2as "github.com/m-lab/annotation-service/api/v2"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
)

// Errors that may be returned by BaseRowBuffer functions.
var (
	ErrAnnotationError = errors.New("Annotation error")
	ErrNotAnnotatable  = errors.New("object does not implement Annotatable")
	ErrRowNotPointer   = errors.New("Row should be a pointer type")
)

// RowBuffer provides all basic functionality generally needed for buffering, annotating, and inserting
// rows that implement Annotatable.
type RowBuffer struct {
	bufferSize int
	rows       []interface{} // Actually these are Annotatable, but we cast them later.
	ann        v2as.Annotator
}

// AddRow simply inserts a row into the buffer.  Returns error if buffer is full.
// Not thread-safe.  Should only be called by owning thread.
func (buf *RowBuffer) AddRow(r interface{}) error {
	if !reflect.TypeOf(r).Implements(reflect.TypeOf((*row.Annotatable)(nil)).Elem()) {
		log.Println(reflect.TypeOf(r), "not Annotatable")
		return ErrNotAnnotatable
	}
	for len(buf.rows) > buf.bufferSize-1 {
		return etl.ErrBufferFull
	}
	buf.rows = append(buf.rows, r)
	return nil
}

// NumRowsForTest allows tests to find number of rows in buffer.
func (buf *RowBuffer) NumRowsForTest() int {
	return len(buf.rows)
}

// TakeRows returns all rows in the buffer, and clears the buffer.
// Not thread-safe.  Should only be called by owning thread.
func (buf *RowBuffer) TakeRows() []interface{} {
	res := buf.rows
	buf.rows = make([]interface{}, 0, buf.bufferSize)
	return res
}

// TODO update this to use local cache of high quality annotations.
// label is used to label metrics and errors in GetAnnotations
func (buf *RowBuffer) annotateServers(label string) error {
	serverIPs := make(map[string]struct{})
	logTime := time.Time{}
	for i := range buf.rows {
		r, ok := buf.rows[i].(row.Annotatable)
		if !ok {
			return ErrNotAnnotatable
		}

		// Only ask for the IP if it is non-empty.
		ip := r.GetServerIP()
		if ip != "" {
			serverIPs[ip] = struct{}{}
		}

		if (logTime == time.Time{}) {
			logTime = r.GetLogTime()
		}
	}

	ipSlice := make([]string, 0, len(buf.rows))
	for ip := range serverIPs {
		ipSlice = append(ipSlice, ip)
	}
	if len(ipSlice) == 0 {
		return nil
	}
	response, err := buf.ann.GetAnnotations(context.Background(), logTime, ipSlice, label)
	if err != nil {
		log.Println("error in server GetAnnotations: ", err)
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "Server IP: RPC err in GetAnnotations."}).Inc()
		return err
	}
	annMap := response.Annotations
	if annMap == nil {
		log.Println("empty server annotation response")
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "Server IP: empty response"}).Inc()
		return ErrAnnotationError
	}

	for i := range buf.rows {
		r, ok := buf.rows[i].(row.Annotatable)
		if !ok {
			err = ErrNotAnnotatable
		} else {
			ip := r.GetServerIP()
			if ip != "" {
				ann, ok := annMap[ip]
				if ok {
					r.AnnotateServer(ann)
				}
			}
		}
	}

	return err
}

// label is used to label metrics and errors in GetAnnotations
func (buf *RowBuffer) annotateClients(label string) error {
	ipSlice := make([]string, 0, 2*len(buf.rows)) // This may be inadequate, but its a reasonable start.
	logTime := time.Time{}
	for i := range buf.rows {
		r, ok := buf.rows[i].(row.Annotatable)
		if !ok {
			return ErrNotAnnotatable
		}
		ipSlice = append(ipSlice, r.GetClientIPs()...)
		if (logTime == time.Time{}) {
			logTime = r.GetLogTime()
		}
	}

	response, err := buf.ann.GetAnnotations(context.Background(), logTime, ipSlice, label)
	if err != nil {
		log.Println("error in client GetAnnotations: ", err)
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "Client IP: RPC err in GetAnnotations."}).Inc()
		return err
	}
	annMap := response.Annotations
	if annMap == nil {
		log.Println("empty client annotation response")
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "Client IP: empty response"}).Inc()
		return ErrAnnotationError
	}

	for i := range buf.rows {
		r, ok := buf.rows[i].(row.Annotatable)
		if !ok {
			err = ErrNotAnnotatable
		} else {
			// Will not error because we check for nil annMap above.
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
	clientErr := buf.annotateClients(metricLabel)
	serverErr := buf.annotateServers(metricLabel)

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

// NewBase creates a new parser.Base.  This will generally be embedded in a type specific parser.
func NewBase(ins etl.Inserter, bufSize int, ann v2as.Annotator) *Base {
	buf := RowBuffer{bufSize, make([]interface{}, 0, bufSize), ann}
	return &Base{ins, buf}
}

// TaskError return the task level error, based on failed rows, or any other criteria.
func (pb *Base) TaskError() error {
	return nil
}

// Flush synchronously flushes any pending rows.
// Caller should generally call Annotate first, or use AnnotateAndFlush.
func (pb *Base) Flush() error {
	rows := pb.TakeRows()
	pb.Put(rows)
	return pb.Inserter.Flush()
}

// AnnotateAndFlush annotates the rows in the buffer, and synchronously
// pushes them through Inserter.
func (pb *Base) AnnotateAndFlush(metricLabel string) error {
	annErr := pb.Annotate(metricLabel)
	flushErr := pb.Flush()

	if flushErr != nil {
		return flushErr
	}
	return annErr
}

// AnnotateAndPutAsync annotates the rows in the buffer (synchronously),
// and asynchronously pushes them to the Inserter.
func (pb *Base) AnnotateAndPutAsync(metricLabel string) error {
	annErr := pb.Annotate(metricLabel)
	rows := pb.TakeRows()
	pb.PutAsync(rows)
	return annErr
}
