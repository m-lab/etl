// The etl package provides all major interfaces used across packages.
package etl

import (
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/context"
)

type Inserter interface {
	// InsertRow inserts one row into the insert buffer.
	InsertRow(data interface{}) error
	// InsertRows inserts multiple rows into the insert buffer.
	InsertRows(data []interface{}) error
	// Flush flushes any rows in the buffer out to bigquery.
	Flush() error
	// TableName name of the BQ table that the uploader pushes to.
	TableName() string
	// Dataset name of the BQ dataset containing the table.
	Dataset() string
	// Count returns the count of rows currently in the buffer.
	Count() int
	// RowsInBuffer returns the count of rows currently in the buffer.
	RowsInBuffer() int
}

// Params for NewInserter
type InserterParams struct {
	// The project comes from os.GetEnv("GCLOUD_PROJECT")
	// These specify the google cloud dataset/table to write to.
	Dataset    string
	Table      string
	Suffix     string        // Table name suffix for templated tables.
	Timeout    time.Duration // max duration of backend calls.  (for context)
	BufferSize int           // Number of rows to buffer before writing to backend.
}

type Parser interface {
	// meta - metadata, e.g. from the original tar file name.
	// testName - Name of test file (typically extracted from a tar file)
	// test - binary test data
	ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error

	// The name of the table that this Parser inserts into.
	// Used for metrics and logging.
	TableName() string
}

//========================================================================
// Interfaces to allow fakes.
//========================================================================
type Uploader interface {
	Put(ctx context.Context, src interface{}) error
}
