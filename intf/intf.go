// etl package provides all interfaces that are used across other packages
// in the project.
package intf

import (
	"cloud.google.com/go/bigquery"
	"time"
)

// An Inserter provides:
//   InsertRows - inserts one or more rows into BQ (or the insert buffer).
//   Flush - flushes any rows in the buffer out to bigquery.
//   InsertCount - returns the total count of rows passed through InsertRow.
//   BufferSize - returns the count of rows currently in the buffer.
type Inserter interface {
	InsertRows(data interface{}) error
	Flush() error
	Count() int
	RowsInBuffer() int
}

// Params for NewInserter
type InserterParams struct {
	// These specify the google cloud project/dataset/table to write to.
	Project    string
	Dataset    string
	Table      string
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
