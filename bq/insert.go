// Package bq includes all code related to BigQuery.
//
// NB: NOTES ON MEMORY USE AND HTTP SIZE
// The bigquery library uses JSON encoding of data, which appears to be the only
// option at this time.  Furthermore, it uses intermediate data representations,
// eventually creating a map[string]Value (unless you pass that in to begin with).
// In general, when we start pumping large volumes of data, both the map and the
// JSON will cause some memory pressure, and likely pretty severe limits on the size
// of the insert we can send, likely on the order of a couple MB of actual row footprint
// in BQ.
//
// Passing in slice of structs makes memory pressure a bit worse, but probably isn't
// worth worrying about.

package bq

import (
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/context"
)

// An Inserter provides:
//   InsertRows - inserts one or more rows into the insert buffer.
//   Flush - flushes any rows in the buffer out to bigquery.
//   Count - returns the count of rows currently in the buffer.
type Inserter interface {
	InsertRows(data interface{}) error
	Flush() error
	Count() int
	RowsInBuffer() int
}

type BQInserter struct {
	Inserter
	params   InserterParams
	client   *bigquery.Client
	uploader *bigquery.Uploader
	timeout  time.Duration
	rows     []interface{}
	inserted int // Number of rows successfully inserted.
}

type InserterParams struct {
	// These specify the google cloud project/dataset/table to write to.
	Project    string
	Dataset    string
	Table      string
	Timeout    time.Duration // max duration of backend calls.  (for context)
	BufferSize int           // number of rows to buffer before writing to backend.
}

func NewInserter(params InserterParams) (Inserter, error) {

	ctx, _ := context.WithTimeout(context.Background(), params.Timeout)
	// Heavyweight!
	client, err := bigquery.NewClient(ctx, params.Project)
	if err != nil {
		return nil, err
	}

	uploader := client.Dataset(params.Dataset).Table(params.Table).Uploader()
	in := BQInserter{params: params, client: client, uploader: uploader, timeout: params.Timeout}
	return &in, nil
}

// Caller should check error, and take appropriate action before calling again.
func (in *BQInserter) InsertRows(data interface{}) error {
	in.rows = append(in.rows, data)
	if len(in.rows) >= in.params.BufferSize {
		return in.Flush()
	} else {
		return nil
	}
}

// TODO(dev) Should have a recovery mechanism for failed inserts.
func (in *BQInserter) Flush() error {
	if len(in.rows) == 0 {
		return nil
	}

	// This is heavyweight, and may run forever without a context deadline.
	ctx, _ := context.WithTimeout(context.Background(), in.timeout)
	err := in.uploader.Put(ctx, in.rows)
	if err == nil {
		in.inserted += len(in.rows)
		in.rows = make([]interface{}, in.params.BufferSize)
	}
	return err
}

func (in *BQInserter) RowsInBuffer() int {
	return len(in.rows)
}

func (in *BQInserter) Count() int {
	return in.inserted + len(in.rows)
}

type NullInserter struct {
	Inserter
}

func (in *NullInserter) InsertRows(data interface{}) error {
	return nil
}
func (in *NullInserter) Flush() error {
	return nil
}
