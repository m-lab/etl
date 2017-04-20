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
	InsertCount() int
	BufferSize() int
}

// Params for NewInserter
type InserterParams struct {
	Project    string
	Dataset    string
	Table      string
	Timeout    time.Duration
	BufferSize int
}
