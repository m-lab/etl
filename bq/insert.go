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
	InsertCount() int
	BufferSize() int
}

type BQInserter struct {
	Inserter
	params   InserterParams
	client   *bigquery.Client
	uploader *bigquery.Uploader
	timeout  time.Duration
	rows     []interface{}
	count    int
}

type InserterParams struct {
	Project    string
	Dataset    string
	Table      string
	Timeout    time.Duration
	BufferSize int
}

// TODO - Consider injecting the Client here, to allow broader unit testing options.
// project, dataset, table - specifies the google cloud project/dataset/table
// timeout - determines how long operations to the backend are allowed before failing.
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

func (in *BQInserter) InsertRows(data interface{}) error {
	in.rows = append(in.rows, data)
	// TODO(dev) a sensible value should go here, but a quick estimate
	// of 10K per row times 100 results is 1MB, which is an order of
	// magnitude below our 10MB max, so 100 might not be such a bad
	// default.
	if len(in.rows) > 100 {
		return in.Flush()
	} else {
		return nil
	}
}

func (in *BQInserter) Flush() error {
	if len(in.rows) == 0 {
		return nil
	}
	outRows := in.rows
	in.rows = []interface{}{}
	in.count += len(outRows)

	// This is heavyweight, and may run forever without a context deadline.
	ctx, _ := context.WithTimeout(context.Background(), in.timeout)
	return in.uploader.Put(ctx, outRows)
}

func (in *BQInserter) BufferSize() int {
	return in.count + len(in.rows)
}

func (in *BQInserter) InsertCount() int {
	return in.count
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
