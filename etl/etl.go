// Package etl provides all major interfaces used across packages.
package etl

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/civil"
)

// ProcessingError extends error to provide dataType and detail for metrics,
// and appropriate return codes for http handlers.
type ProcessingError interface {
	DataType() string
	Detail() string
	Code() int
	error
}

// RowStats interface defines some useful Inserter stats that will also be
// implemented by Parser.
// RowStats implementations should provide the invariants:
//
//	Accepted == Failed + Committed + RowsInBuffer
type RowStats interface {
	// RowsInBuffer returns the count of rows currently in the buffer.
	RowsInBuffer() int
	// Committed returns the count of rows successfully committed to BQ.
	Committed() int
	// Accepted returns the count of all rows received through InsertRow(s)
	Accepted() int
	// Failed returns the count of all rows that could not be committed.
	Failed() int
}

// Inserter is a data sink that writes to BigQuery tables.
// Inserters should provide the invariants:
//
//	After Flush() returns, RowsInBuffer == 0
type Inserter interface {
	// Put synchronously sends a slice of rows to BigQuery
	// This is THREADSAFE
	Put(rows []interface{}) error
	// PutAsync asynchronously sends a slice of rows to BigQuery.
	// It is THREADSAFE.
	// It may block if there is already a Put (or Flush) in progress.
	// To synchronize following PutAsync, call one of the stats functions,
	// e.g. Commited() or Failed()
	PutAsync(rows []interface{})

	// InsertRow inserts one row into the insert buffer.
	// Deprecated:  Please use AddRow and FlushAsync instead.
	InsertRow(data interface{}) error
	// InsertRows inserts multiple rows into the insert buffer.
	// Deprecated:  Please use AddRow and FlushAsync instead.
	InsertRows(data []interface{}) error

	// Flush flushes any rows in the buffer out to bigquery.
	// This is synchronous - on return, rows should be committed.
	// Deprecated:  Please use external buffer, Put, and PutAsync instead.
	Flush() error

	// TODO Replace this with Params()

	// Base Table name of the BQ table that the uploader pushes to.
	TableBase() string
	// Table name suffix of the BQ table that the uploader pushes to.
	TableSuffix() string
	// Full table name of the BQ table that the uploader pushes to,
	// including $YYYYMMNN, or _YYYYMMNN
	FullTableName() string
	// Dataset name of the BQ dataset containing the table.
	Dataset() string
	// Project name
	Project() string

	RowStats // Inserter must implement RowStats
}

// Inserter related constants.
var (
	// ErrBufferFull is returned when an InsertBuffer is full.
	ErrBufferFull = errors.New("insert buffer is full")

	// ErrBadDataType is returned when a path does not have a valid datatype.
	ErrBadDataType = errors.New("unknown data type")
)

// InserterParams for NewInserter
type InserterParams struct {
	// These specify the google cloud project:dataset.table to write to.
	Project string
	Dataset string
	Table   string

	// Suffix may be table suffix _YYYYMMDD or partition $YYYYMMDD
	Suffix string // Table name suffix for templated tables or partitions.

	BufferSize int // Number of rows to buffer before writing to backend.

	PutTimeout    time.Duration // max duration of bigquery Put ops.  (for context)
	MaxRetryDelay time.Duration // Maximum backoff time for Put retries.
}

// Metadata provides metadata about the parser and archive files.
type Metadata struct {
	Version     string
	ArchiveURL  string
	GitCommit   string
	Date        civil.Date
	Start       time.Time
	ArchiveSize int64
}

// ErrHighInsertionFailureRate should be returned by TaskError when there are more than 10% BQ insertion errors.
var ErrHighInsertionFailureRate = errors.New("too many insertion failures")

// Parser is the generic interface implemented by each experiment parser.
type Parser interface {
	// IsParsable reports a canonical file "kind" and whether the file appears to
	// be parsable based on the name and content size. A true result does not
	// guarantee that ParseAndInsert will succeed, but a false result means that
	// ParseAndInsert can be skipped.
	IsParsable(testName string, test []byte) (string, bool)

	// meta - metadata, e.g. from the original tar file name.
	// testName - Name of test file (typically extracted from a tar file)
	// test - binary test data
	ParseAndInsert(meta Metadata, testName string, test []byte) error

	// Flush flushes any pending rows.
	Flush() error

	// TableName of the table that this Parser inserts into.
	// Used for metrics and logging.
	TableName() string

	// FullTableName of the BQ table that the uploader pushes to,
	// including $YYYYMMNN, or _YYYYMMNN
	FullTableName() string

	// Task level error, based on failed rows, or any other criteria.
	TaskError() error

	RowStats // Parser must implement RowStats
}

// TestSource provides a source of test data.
type TestSource interface {
	// NextTest reads the next test object from the tar file.
	// Skips reading contents of any file larger than maxSize, returning empty data
	// and storage.ErrOversizeFile.
	// Returns io.EOF when there are no more tests.
	NextTest(maxSize int64) (string, []byte, error)
	Close() error
	GetSize() int64

	Detail() string   // Detail for logs.
	Type() string     // Data type for logs and metrics
	Date() civil.Date // Date associated with test source
}

//========================================================================
// Interface to allow fakes.
//========================================================================

// Uploader defines the BQ Uploader interface, for injection.
type Uploader interface {
	Put(ctx context.Context, src interface{}) error
}
