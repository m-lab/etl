package row

// TODO integrate this functionality into the parser.go code.
// Probably should have Base implement Parser.

import (
	"context"
	"errors"
	"log"
	"reflect"
	"sync"
	"time"

	v2as "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/parser"
	"github.com/prometheus/client_golang/prometheus"
)

// Errors that may be returned by Buffer functions.
var (
	ErrBufferFull = errors.New("Buffer full")
)

// Stats contains stats about buffer history.
type Stats struct {
	Pending   int
	Committed int
	Failed    int
	Accepted  int // rows Accepted (Pending+Committed+Failed)
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
	lock       sync.Mutex
	bufferSize int // Size before returning ErrFull
	rows       []interface{}
}

// AddRow simply inserts a row into the buffer.
// If buffer is full, this returns the buffered rows, and saves provided row
// in new buffer.  Client MUST handle the returned rows.
func (buf *Buffer) AddRow(row interface{}) []interface{} {
	buf.lock.Lock()
	defer buf.lock.Unlock()
	if len(buf.rows) < buf.bufferSize {
		buf.rows = append(buf.rows, row)
		return nil
	}
	rows := buf.rows
	buf.rows = make([]interface{}, 0, buf.bufferSize)
	buf.rows = append(buf.rows, row)

	return rows
}

func (buf *Buffer) NumRowsForTest() int {
	buf.lock.Lock()
	defer buf.lock.Unlock()
	return len(buf.rows)
}

// TakeRows returns all rows in the buffer, and clears the buffer.
func (buf *Buffer) TakeRows() []interface{} {
	buf.lock.Lock()
	defer buf.lock.Unlock()
	res := buf.rows
	buf.rows = make([]interface{}, 0, buf.bufferSize)
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
		r, ok := rows[i].(parser.Annotatable)
		if !ok {
			return parser.ErrNotAnnotatable
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
		return parser.ErrAnnotationError
	}

	for i := range rows {
		r, ok := rows[i].(parser.Annotatable)
		if !ok {
			err = parser.ErrNotAnnotatable
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
		r, ok := rows[i].(parser.Annotatable)
		if !ok {
			return parser.ErrNotAnnotatable
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
		return parser.ErrAnnotationError
	}

	for i := range rows {
		r, ok := rows[i].(parser.Annotatable)
		if !ok {
			err = parser.ErrNotAnnotatable
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
	start := time.Now()
	defer metrics.AnnotationTimeSummary.With(prometheus.Labels{"test_type": metricLabel}).Observe(float64(time.Since(start).Nanoseconds()))

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

// NewBase creates a new parser.Base.  This will generally be embedded in a type specific parser.
func NewBase(label string, sink Sink, bufSize int, ann v2as.Annotator) *Base {
	buf := Buffer{bufferSize: bufSize, rows: make([]interface{}, 0, bufSize)}
	return &Base{sink: sink, ann: annotator{ann}, buf: &buf, label: label}
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
	pb.ann.Annotate(rows, pb.label)
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
	rows := pb.buf.TakeRows()
	return pb.commit(rows)
}

// AddRow adds row to buffer.
// Annotates and commits existing rows iff the buffer is full.
// TODO improve Annotatable architecture.  Maybe more Annotatable here??
func (pb *Base) AddRow(row interface{}) error {
	if !reflect.TypeOf(row).Implements(reflect.TypeOf((*parser.Annotatable)(nil)).Elem()) {
		log.Println(reflect.TypeOf(row), "not Annotatable")
		return parser.ErrNotAnnotatable
	}
	rows := pb.buf.AddRow(row)
	pb.statsLock.Lock()
	defer pb.statsLock.Unlock()
	pb.stats.Accepted++
	pb.stats.Pending++
	if rows != nil {
		go func(rows []interface{}) {
			pb.commit(rows)
		}(rows)
	}
	return nil
}
