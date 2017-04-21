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

	"github.com/m-lab/etl/intf"
)

type BQInserter struct {
	intf.Inserter
	params   intf.InserterParams
	client   *bigquery.Client
	uploader intf.Uploader // May be a BQ Uploader, or a test Uploader
	timeout  time.Duration
	rows     []interface{}
	inserted int // Number of rows successfully inserted.
}

// Pass in nil uploader for normal use, custom uploader for custom behavior
func NewInserter(params intf.InserterParams, uploader intf.Uploader) (intf.Inserter, error) {
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
	return &in, nil
}

// Caller should check error, and take appropriate action before calling again.
func (in *BQInserter) InsertRow(data interface{}) error {
	// TODO - this completely ignores the BufferSize, so may cause
	// oversized Insert requests.  Should fix, probably in Flush.
	in.rows = append(in.rows, data)
	if len(in.rows) >= in.params.BufferSize {
		return in.Flush()
	} else {
		return nil
	}
}

// Caller should check error, and take appropriate action before calling again.
func (in *BQInserter) InsertRows(data []interface{}) error {
	in.rows = append(in.rows, data...)
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
	intf.Inserter
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
