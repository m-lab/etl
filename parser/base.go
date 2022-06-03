package parser

// TODO integrate this functionality into the parser.go code.
// Probably should have Base implement Parser.

import (
	"errors"

	"github.com/m-lab/etl/etl"
)

// Errors that may be returned by BaseRowBuffer functions.
var (
	ErrAnnotationError = errors.New("annotation error")
	ErrNotAnnotatable  = errors.New("object does not implement Annotatable")
	ErrRowNotPointer   = errors.New("row should be a pointer type")
)

// RowBuffer provides all basic functionality generally needed for buffering, annotating, and inserting
// rows that implement Annotatable.
type RowBuffer struct {
	bufferSize int
	rows       []interface{} // Actually these are Annotatable, but we cast them later.
}

// AddRow simply inserts a row into the buffer.  Returns error if buffer is full.
// Not thread-safe.  Should only be called by owning thread.
func (buf *RowBuffer) AddRow(r interface{}) error {
	//if !reflect.TypeOf(r).Implements(reflect.TypeOf((*row.Annotatable)(nil)).Elem()) {
	//	log.Println(reflect.TypeOf(r), "not Annotatable")
	//	return ErrNotAnnotatable
	//}
	for len(buf.rows) > buf.bufferSize-1 {
		return etl.ErrBufferFull
	}
	buf.rows = append(buf.rows, r)
	return nil
}

// NumRowsForTest allows tests to find number of rows in buffer.
func (buf *RowBuffer) NumRowsForTest() int {
	return len(buf.rows)
}

// TakeRows returns all rows in the buffer, and clears the buffer.
// Not thread-safe.  Should only be called by owning thread.
func (buf *RowBuffer) TakeRows() []interface{} {
	res := buf.rows
	buf.rows = make([]interface{}, 0, buf.bufferSize)
	return res
}

// Base provides common parser functionality.
type Base struct {
	etl.Inserter
	RowBuffer
}

// NewBase creates a new parser.Base.  This will generally be embedded in a type specific parser.
func NewBase(ins etl.Inserter, bufSize int) *Base {
	buf := RowBuffer{
		bufferSize: bufSize,
		rows:       make([]interface{}, 0, bufSize),
	}
	return &Base{ins, buf}
}

// TaskError return the task level error, based on failed rows, or any other criteria.
func (pb *Base) TaskError() error {
	return nil
}

// Flush synchronously flushes any pending rows.
// Caller should generally call Annotate first, or use AnnotateAndFlush.
func (pb *Base) Flush() error {
	rows := pb.TakeRows()
	pb.Put(rows)
	return pb.Inserter.Flush()
}
