// The etl package provides all major interfaces used across packages.
package etl

import (
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/context"
)

// RowStats interface defines some useful Inserter stats that will also be
// implemented by Parser.
// RowStats implementations should provide the invariants:
//   Accepted == Failed + Committed + RowsInBuffer
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
//   After Flush() returns, RowsInBuffer == 0
type Inserter interface {
	// InsertRow inserts one row into the insert buffer.
	InsertRow(data interface{}) error
	// InsertRows inserts multiple rows into the insert buffer.
	InsertRows(data []interface{}) error
	// Flush flushes any rows in the buffer out to bigquery.
	Flush() error

	// Base Table name of the BQ table that the uploader pushes to.
	TableBase() string
	// Table name suffix of the BQ table that the uploader pushes to.
	TableSuffix() string
	// Full table name of the BQ table that the uploader pushes to,
	// including $YYYYMMNN, or _YYYYMMNN
	FullTableName() string
	// Dataset name of the BQ dataset containing the table.
	Dataset() string

	RowStats // Inserter must implement RowStats
}

// Params for NewInserter
type InserterParams struct {
	// The project comes from os.GetEnv("GCLOUD_PROJECT")
	// These specify the google cloud dataset/table to write to.
	Dataset string
	Table   string
	// Suffix may be an actual _YYYYMMDD or partition $YYYYMMDD
	Suffix     string        // Table name suffix for templated tables or partitions.
	Timeout    time.Duration // max duration of backend calls.  (for context)
	BufferSize int           // Number of rows to buffer before writing to backend.
}

type Parser interface {
	// meta - metadata, e.g. from the original tar file name.
	// testName - Name of test file (typically extracted from a tar file)
	// test - binary test data
	ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error

	// Flush flushes any pending rows.
	Flush() error

	// The name of the table that this Parser inserts into.
	// Used for metrics and logging.
	TableName() string

	// Full table name of the BQ table that the uploader pushes to,
	// including $YYYYMMNN, or _YYYYMMNN
	FullTableName() string

	RowStats // Parser must implement RowStats
}

//========================================================================
// Interfaces to allow fakes.
//========================================================================
type Uploader interface {
	Put(ctx context.Context, src interface{}) error
}
