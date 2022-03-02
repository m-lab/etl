package row

// TODO integrate this functionality into the go code.
// Probably should have Base implement Parser.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/m-lab/annotation-service/api"
	v2as "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/go/logx"

	"github.com/m-lab/etl/metrics"
)

// Errors that may be returned by Buffer functions.
var (
	ErrAnnotationError = errors.New("Annotation error")
	ErrNotAnnotatable  = errors.New("object does not implement Annotatable")
	ErrBufferFull      = errors.New("Buffer full")
	ErrInvalidSink     = errors.New("Not a valid row.Sink")
)

type ErrCommitRow struct {
	Err error
}

func (e ErrCommitRow) Error() string {
	return fmt.Sprintf("failed to commit row(s), error: %s", e.Err)
}

func (e ErrCommitRow) Unwrap() error {
	return e.Err
}

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
	Buffered  int // rows buffered but not yet sent.
	Pending   int // pending counts previously buffered rows that are being committed.
	Committed int
	Failed    int
}

// Total returns the total number of rows handled.
func (s Stats) Total() int {
	return s.Buffered + s.Pending + s.Committed + s.Failed
}

// ActiveStats is a stats object that supports updates.
type ActiveStats struct {
	lock sync.RWMutex // Protects all Stats fields.
	Stats
}

// GetStats implements HasStats()
func (as *ActiveStats) GetStats() Stats {
	as.lock.RLock()
	defer as.lock.RUnlock()
	return as.Stats
}

// MoveToPending increments the Pending field.
func (as *ActiveStats) MoveToPending(n int) {
	as.lock.Lock()
	defer as.lock.Unlock()
	as.Buffered -= n
	if as.Buffered < 0 {
		log.Println("BROKEN - negative buffered")
	}
	as.Pending += n
}

// Inc increments the Buffered field
func (as *ActiveStats) Inc() {
	logx.Debug.Println("IncPending")
	as.lock.Lock()
	defer as.lock.Unlock()
	as.Buffered++
}

// Done updates the pending to failed or committed.
func (as *ActiveStats) Done(n int, err error) {
	as.lock.Lock()
	defer as.lock.Unlock()
	as.Pending -= n
	if as.Pending < 0 {
		log.Println("BROKEN: negative Pending")
	}
	if err != nil {
		as.Failed += n
	} else {
		as.Committed += n
	}
	logx.Debug.Printf("Done %d->%d %v\n", as.Pending+n, as.Pending, err)
}

// HasStats can provide stats
type HasStats interface {
	GetStats() Stats
}

// Sink defines the interface for committing rows.
// Returns the number of rows successfully committed, and error.
// Implementations should be threadsafe.
type Sink interface {
	Commit(rows []interface{}, label string) (int, error)
	io.Closer
}

// Buffer provides all basic functionality generally needed for buffering, annotating, and inserting
// rows that implement Annotatable.
// Buffer functions are THREAD-SAFE
type Buffer struct {
	lock sync.Mutex
	size int // Number of rows before starting new buffer.
	rows []interface{}
}

// NewBuffer returns a new buffer of the desired size.
func NewBuffer(size int) *Buffer {
	return &Buffer{size: size, rows: make([]interface{}, 0, size)}
}

// Append appends a row to the buffer.
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

var logEmptyAnn = logx.NewLogEvery(nil, 60*time.Second)

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
		logEmptyAnn.Println("empty client annotation response")
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

	stats ActiveStats
}

// NewBase creates a new Base.  This will generally be embedded in a type specific parser.
func NewBase(label string, sink Sink, bufSize int, ann v2as.Annotator) *Base {
	buf := NewBuffer(bufSize)
	return &Base{sink: sink, ann: annotator{ann}, buf: buf, label: label}
}

// GetStats returns the buffer/sink stats.
func (pb *Base) GetStats() Stats {
	return pb.stats.GetStats()
}

// TaskError return the task level error, based on failed rows, or any other criteria.
func (pb *Base) TaskError() error {
	return nil
}

var logAnnError = logx.NewLogEvery(nil, 60*time.Second)

func (pb *Base) commit(rows []interface{}) error {
	err := pb.ann.Annotate(rows, pb.label)
	if err != nil {
		logAnnError.Println("annotation: ", err)
	}

	// TODO do we need these to be done in order.
	// This is synchronous, blocking, and thread safe.
	done, commitErr := pb.sink.Commit(rows, pb.label)
	if commitErr != nil {
		err = ErrCommitRow{commitErr}
	}

	if done > 0 {
		pb.stats.Done(done, nil)
	}
	if err != nil {
		log.Println(pb.label, err)
		pb.stats.Done(len(rows)-done, err)
	}
	return err
}

// Flush synchronously flushes any pending rows.
func (pb *Base) Flush() error {
	rows := pb.buf.Reset()
	pb.stats.MoveToPending(len(rows))
	return pb.commit(rows)
}

// Put adds a row to the buffer.
// Iff the buffer is already full the prior buffered rows are
// annotated and committed to the Sink.
// NOTE: There is no guarantee about ordering of writes resulting from
// sequential calls to Put.  However, once a block of rows is submitted
// to pb.commit, it should be written in the same order to the Sink.
// TODO improve Annotatable architecture.
func (pb *Base) Put(row Annotatable) error {
	rows := pb.buf.Append(row)
	pb.stats.Inc()

	if rows != nil {
		pb.stats.MoveToPending(len(rows))
		err := pb.commit(rows)
		if err != nil {
			// Note that error is likely associated with buffered rows, not the current
			// row.
			// When using GCS output, this may result in a corrupted json file.
			// In that event, the test count may become meaningless.
			metrics.TestTotal.WithLabelValues(pb.label, pb.label, "error").Inc()
			metrics.ErrorCount.WithLabelValues(
				pb.label, "", "put error").Inc()
			return err
		}
	}
	return nil
}

// NullAnnotator satisfies the Annotatable interface without actually doing
// anything.
type NullAnnotator struct{}

// GetLogTime returns current time rather than the actual row time.
func (row *NullAnnotator) GetLogTime() time.Time {
	return time.Now()
}

// GetClientIPs returns an empty array so nothing is annotated.
func (row *NullAnnotator) GetClientIPs() []string {
	return []string{}
}

// GetServerIP returns an empty string because there is nothing to annotate.
func (row *NullAnnotator) GetServerIP() string {
	return ""
}

// AnnotateClients does nothing.
func (row *NullAnnotator) AnnotateClients(map[string]*api.Annotations) error {
	return nil
}

// AnnotateServer does nothing.
func (row *NullAnnotator) AnnotateServer(*api.Annotations) error {
	return nil
}
