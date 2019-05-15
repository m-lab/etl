package parser

import (
	"log"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/etl/annotation"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/tcp-info/snapshot"
	"github.com/prometheus/client_golang/prometheus"
)

// Parser for parsing sidestream tests.

/********** This block of code is redundant with SSParser and should be refactored ********/

type Annotatable interface {
	GetClient() string
	AnnotateClient(*api.GeoData)
	AnnotateServer()
	LogTime() time.Time
}

type BaseRowBuffer struct {
	bufferSize int
	rows       []Annotatable
}

// AddRow simply inserts a row into the buffer.  Returns error if buffer is full.
// Not thread-safe.  Should only be called by owning thread.
func (buf *BaseRowBuffer) AddRow(row Annotatable) error {
	for len(buf.rows) >= buf.bufferSize-1 {
		return etl.ErrBufferFull
	}
	buf.rows = append(buf.rows, row)
	return nil
}

// TakeRows returns all rows in the buffer, and clears the buffer.
// Not thread-safe.  Should only be called by owning thread.
func (buf *BaseRowBuffer) TakeRows() []Annotatable {
	res := buf.rows
	buf.rows = make([]Annotatable, 0, buf.bufferSize)
	return res
}

func (buf *BaseRowBuffer) annotateClients() error {
	ipSlice := make([]string, len(buf.rows))
	for i := range buf.rows {
		ipSlice[i] = buf.rows[i].GetClient()
	}

	logTime := buf.rows[0].LogTime()
	annSlice := annotation.FetchAllAnnotations(ipSlice, logTime)
	// TODO - are there any errors we should process from Fetch?
	if annSlice == nil || len(annSlice) != len(ipSlice) {
		return nil // TODO return error
	}

	for i := range buf.rows {
		buf.rows[i].AnnotateClient(annSlice[i])
	}

	return nil
}

func (buf *BaseRowBuffer) annotateServers() error {
	return nil
}

// Annotate fetches annotations for all rows in the buffer.
// Not thread-safe.  Should only be called by owning thread.
func (buf *BaseRowBuffer) Annotate(tableBase string) {
	metrics.WorkerState.WithLabelValues(tableBase, "annotate").Inc()
	defer metrics.WorkerState.WithLabelValues(tableBase, "annotate").Dec()
	if len(buf.rows) == 0 {
		return
	}
	start := time.Now()

	err := buf.annotateClients()
	if err != nil {
		// TODO return error?
	}

	buf.annotateServers()

	metrics.AnnotationTimeSummary.With(prometheus.Labels{"test_type": "SS"}).Observe(float64(time.Since(start).Nanoseconds()))
}

// Base provides common parser functionality.
type Base struct {
	// TODO - maybe make this embedded
	inserter etl.Inserter
	etl.RowStats
	BaseRowBuffer
}

// NewBase creates a new sidestream parser.
func NewBase(ins etl.Inserter) *Base {
	bufSize := etl.TCPINFO.BQBufferSize()
	buf := BaseRowBuffer{bufSize, make([]Annotatable, 0, bufSize)}
	return &Base{ins, ins, buf}
}

// TaskError return the task level error, based on failed rows, or any other criteria.
func (pb *Base) TaskError() error {
	return nil
}

// TableName of the table that this Parser inserts into.
func (pb *Base) TableName() string {
	return pb.inserter.TableBase()
}

// FullTableName of the BQ table that the uploader pushes to,
// including $YYYYMMNN, or _YYYYMMNN
func (pb *Base) FullTableName() string {
	return pb.inserter.FullTableName()
}

// Flush flushes any pending rows.
func (pb *Base) Flush() error {
	pb.inserter.Put(pb.TakeRows())
	return pb.inserter.Flush()
}

/** End of redundant boilerplate ***********************************/

type TCPRow struct {
	UUID          string    // Top level just because
	TestTime      time.Time // Must be top level for partitioning
	TaskFileName  string    // The tar file containing this test.
	ParseTime     time.Time
	ParserVersion string
	Snapshots     []*snapshot.Snapshot
}

func (tcp *TCPRow) GetClient() string {
	return ""
}
func (tcp *TCPRow) AnnotateClient(*api.GeoData) {

}
func (tcp *TCPRow) AnnotateServer() {

}
func (tcp *TCPRow) LogTime() time.Time {
	return time.Now()
}

type TCPInfoParser struct {
	Base
}

// ParseAndInsert parses an entire TCPInfo record (all snapshots) and inserts it into bigquery.
func (tcp *TCPInfoParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, rawContent []byte) error {
	// TODO: for common metric states with constant labels, define global constants.
	metrics.WorkerState.WithLabelValues(tcp.TableName(), "ss").Inc()
	defer metrics.WorkerState.WithLabelValues(tcp.TableName(), "ss").Dec()

	// TODO validate IP addresses?
	test := TCPRow{}
	test.ParseTime = time.Now() // for map, use string(time.Now().MarshalText())
	test.ParserVersion = Version()
	if meta["filename"] != nil {
		test.TaskFileName = meta["filename"].(string)
	}
	// Add row to buffer, possibly flushing buffer if it is full.
	err := tcp.AddRow(&test)
	if err == etl.ErrBufferFull {
		// Flush asynchronously, to improve throughput.
		tcp.Annotate(tcp.inserter.TableBase())
		tcp.inserter.PutAsync(tcp.TakeRows())
		err = tcp.AddRow(&test)
	}
	if err != nil {
		metrics.ErrorCount.WithLabelValues(
			tcp.TableName(), "ss", "insert-err").Inc()
		metrics.TestCount.WithLabelValues(
			tcp.TableName(), "ss", "insert-err").Inc()
		log.Printf("insert-err: %v\n", err)
	}
	metrics.TestCount.WithLabelValues(tcp.TableName(), "tcpinfo", "ok").Inc()

	return nil
}
