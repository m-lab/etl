package intf

import "time"

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
