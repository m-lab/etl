package row

// TODO integrate this functionality into the go code.
// Probably should have Base implement Parser.

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/m-lab/annotation-service/api"
	v2as "github.com/m-lab/annotation-service/api/v2"

	"github.com/m-lab/etl/metrics"
)

// Errors that may be returned by Buffer functions.
var (
	ErrAnnotationError = errors.New("Annotation error")
	ErrNotAnnotatable  = errors.New("object does not implement Annotatable")
	ErrBufferFull      = errors.New("Buffer full")
)

// Annotatable interface enables integration of annotation into parser.Base.
// The row type should implement the interface, and the annotations will be added
// prior to insertion.
type Annotatable interface {
	GetLogTime() time.Time
	GetClientIPs() []string // This is a slice to support mutliple hops in traceroute data.
	GetServerIP() string
	AnnotateClients(map[string]*api.Annotations) error // Must properly handle missing annotations.
	AnnotateServer(*api.Annotations) error             // Must properly handle nil parameter.
}

// Stats contains stats about buffer history.
type Stats struct {
	Pending   int
	Committed int
	Failed    int
	Total     int // total rows accepted (Pending+Committed+Failed)
}

// HasStats can provide stats
type HasStats interface {
	GetStats() Stats
}

// Sink defines the interface for committing rows.
// These should be threadsafe.
type Sink interface {
	Commit(rows []interface{}, label string) error
}

// Buffer provides all basic functionality generally needed for buffering, annotating, and inserting
// rows that implement Annotatable.
// Buffer functions are THREAD-SAFE
type Buffer struct {
	lock sync.Mutex
	size int // Number of rows before committing to
	rows []interface{}
}

// NewBuffer returns a new buffer of the desired size.
func NewBuffer(size int) *Buffer {
	return &Buffer{size: size, rows: make([]interface{}, 0, size)}
}

// Append simply appends a row to the buffer.
// If buffer is full, this returns the buffered rows, and saves provided row
// in new buffer.  Client MUST handle the returned rows.
func (buf *Buffer) Append(row interface{}) []interface{} {
	buf.lock.Lock()
	defer buf.lock.Unlock()
	if len(buf.rows) < buf.size {
		buf.rows = append(buf.rows, row)
		return nil
	}
	rows := buf.rows
	buf.rows = make([]interface{}, 0, buf.size)
	buf.rows = append(buf.rows, row)

	return rows
}

// Pending returns the number of pending rows in the buffer.
func (buf *Buffer) Pending() int {
	buf.lock.Lock()
	defer buf.lock.Unlock()
	return len(buf.rows)
}

// Reset clears the buffer, returning all pending rows.
func (buf *Buffer) Reset() []interface{} {
	buf.lock.Lock()
	defer buf.lock.Unlock()
	res := buf.rows
	buf.rows = make([]interface{}, 0, buf.size)
	return res
}

type annotator struct {
	v2 v2as.Annotator
}

// label is used to label metrics and errors in GetAnnotations
func (ann *annotator) annotateServers(rows []interface{}, label string) error {
	serverIPs := make(map[string]struct{})
	logTime := time.Time{}
	for i := range rows {
		r, ok := rows[i].(Annotatable)
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

	ipSlice := make([]string, 0, len(rows))
	for ip := range serverIPs {
		ipSlice = append(ipSlice, ip)
	}
	if len(ipSlice) == 0 {
		return nil
	}
	response, err := ann.v2.GetAnnotations(context.Background(), logTime, ipSlice, label)
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

	for i := range rows {
		r, ok := rows[i].(Annotatable)
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
func (ann *annotator) annotateClients(rows []interface{}, label string) error {
	ipSlice := make([]string, 0, 2*len(rows)) // This may be inadequate, but its a reasonable start.
	logTime := time.Time{}
	for i := range rows {
		r, ok := rows[i].(Annotatable)
		if !ok {
			return ErrNotAnnotatable
		}
		ipSlice = append(ipSlice, r.GetClientIPs()...)
		if (logTime == time.Time{}) {
			logTime = r.GetLogTime()
		}
	}

	response, err := ann.v2.GetAnnotations(context.Background(), logTime, ipSlice, label)
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

	for i := range rows {
		r, ok := rows[i].(Annotatable)
		if !ok {
			err = ErrNotAnnotatable
		} else {
			// Will not error because we check for nil annMap above.
			r.AnnotateClients(annMap)
		}
	}

	return err
}

// Annotate fetches and applies annotations for all rows
func (ann *annotator) Annotate(rows []interface{}, metricLabel string) error {
	metrics.WorkerState.WithLabelValues(metricLabel, "annotate").Inc()
	defer metrics.WorkerState.WithLabelValues(metricLabel, "annotate").Dec()
	if len(rows) == 0 {
		return nil
	}
	// TODO replace this with a histogram.
	defer func(label string, start time.Time) {
		metrics.AnnotationTimeSummary.With(prometheus.Labels{"test_type": label}).Observe(float64(time.Since(start).Nanoseconds()))
	}(metricLabel, time.Now())

	// TODO Consider doing these in parallel?
	clientErr := ann.annotateClients(rows, metricLabel)
	serverErr := ann.annotateServers(rows, metricLabel)

	if clientErr != nil {
		return clientErr
	}

	if serverErr != nil {
		return serverErr
	}

	return nil
}

// Base provides common parser functionality.
// Base is NOT THREAD-SAFE
type Base struct {
	sink  Sink
	ann   annotator
	buf   *Buffer
	label string // Used in metrics and errors.

	statsLock sync.Mutex
	stats     Stats
}

// NewBase creates a new Base.  This will generally be embedded in a type specific parser.
func NewBase(label string, sink Sink, bufSize int, ann v2as.Annotator) *Base {
	buf := NewBuffer(bufSize)
	return &Base{sink: sink, ann: annotator{ann}, buf: buf, label: label}
}

// GetStats returns the buffer/sink stats.
func (pb *Base) GetStats() Stats {
	pb.statsLock.Lock()
	defer pb.statsLock.Unlock()
	return pb.stats
}

// TaskError return the task level error, based on failed rows, or any other criteria.
func (pb *Base) TaskError() error {
	return nil
}

func (pb *Base) commit(rows []interface{}) error {
	// TODO - care about error?
	_ = pb.ann.Annotate(rows, pb.label)
	// TODO do we need these to be done in order.
	err := pb.sink.Commit(rows, pb.label)

	pb.statsLock.Lock()
	defer pb.statsLock.Unlock()
	pb.stats.Pending -= len(rows)
	if err != nil {
		pb.stats.Failed += len(rows)
		return err
	}
	pb.stats.Committed += len(rows)
	return nil
}

// Flush synchronously flushes any pending rows.
func (pb *Base) Flush() error {
	rows := pb.buf.Reset()
	return pb.commit(rows)
}

// Put adds a row to the buffer.
// Iff the buffer is already full the prior buffered rows are
// annotated and committed to the Sink.
// NOTE: There is no guarantee about ordering of writes resulting from
// sequential calls to Put.  However, once a block of rows is submitted
// to pb.commit, it should be written in the same order to the Sink.
// TODO improve Annotatable architecture.
func (pb *Base) Put(row Annotatable) {
	rows := pb.buf.Append(row)
	pb.statsLock.Lock()
	defer pb.statsLock.Unlock()
	pb.stats.Total++
	pb.stats.Pending++
	if rows != nil {
		go func(rows []interface{}) {
			// This allows pipelined parsing annotating, and writing.
			err := pb.commit(rows)
			if err != nil {
				log.Println(err)
			}
		}(rows)
	}
}
