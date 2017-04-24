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
	"log"
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/context"

	"github.com/m-lab/etl/etl"
)

type BQInserter struct {
	etl.Inserter
	params   etl.InserterParams
	client   *bigquery.Client
	uploader etl.Uploader // May be a BQ Uploader, or a test Uploader
	timeout  time.Duration
	rows     []interface{}
	inserted int // Number of rows successfully inserted.
}

// Pass in nil uploader for normal use, custom uploader for custom behavior
func NewInserter(params etl.InserterParams, uploader etl.Uploader) (etl.Inserter, error) {
	var client *bigquery.Client
	if uploader == nil {
		ctx, _ := context.WithTimeout(context.Background(), params.Timeout)
		// Heavyweight!
		client, err := bigquery.NewClient(ctx, params.Project)
		if err != nil {
			return nil, err
		}

		uploader = client.Dataset(params.Dataset).Table(params.Table).Uploader()
	}
	in := BQInserter{params: params, client: client, uploader: uploader, timeout: params.Timeout}
	in.rows = make([]interface{}, 0, in.params.BufferSize)
	return &in, nil
}

// Caller should check error, and take appropriate action before calling again.
func (in *BQInserter) InsertRow(data interface{}) error {
	return in.InsertRows([]interface{}{data})
}

// Caller should check error, and take appropriate action before calling again.
// TODO - should this return a specific error to indicate that a flush is needed
// instead of flushing internally?  The "handle errors in the middle" would
// be easier, though other complications would ensue.
func (in *BQInserter) InsertRows(data []interface{}) error {
	for len(data)+len(in.rows) >= in.params.BufferSize {
		// space >= len(data)
		space := cap(in.rows) - len(in.rows)
		var add []interface{}
		add, data = data[:space], data[space:] // does this break?
		in.rows = append(in.rows, add...)
		err := in.Flush()
		if err != nil {
			// TODO - handle errors in middle better?
			return err
		}
	}
	in.rows = append(in.rows, data...)
	return nil
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
		in.rows = make([]interface{}, 0, in.params.BufferSize)
		return nil
	} else {
		log.Printf("Error on flush: %v\n", err)
		return err
	}
}

func (in *BQInserter) RowsInBuffer() int {
	return len(in.rows)
}

func (in *BQInserter) Count() int {
	return in.inserted + len(in.rows)
}

type NullInserter struct {
	etl.Inserter
}

func (in *NullInserter) InsertRow(data interface{}) error {
	return nil
}
func (in *NullInserter) InsertRows(data []interface{}) error {
	return nil
}
func (in *NullInserter) Flush() error {
	return nil
}

func (in *NullInserter) RowsInBuffer() int {
	return 0
}

func (in *NullInserter) Count() int {
	return 0
}
