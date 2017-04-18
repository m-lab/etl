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

// An Inserter provides the InsertRows function.
type Inserter interface {
	InsertRows(data interface{}, timeout time.Duration) error
	Flush(timeout time.Duration) error
}

type BQInserter struct {
	Inserter
	client   *bigquery.Client
	uploader *bigquery.Uploader
	rows     []interface{}
}

// TODO - Consider injecting the Client here, to allow broader unit testing options.
func NewInserter(project string, dataset string, table string) (Inserter, error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	// Heavyweight!
	client, err := bigquery.NewClient(ctx, project)
	if err != nil {
		return nil, err
	}

	uploader := client.Dataset(dataset).Table(table).Uploader()
	in := BQInserter{client: client, uploader: uploader}
	return &in, nil
}

func (in *BQInserter) InsertRows(data interface{}, timeout time.Duration) error {
	in.rows = append(in.rows, data)
	// TODO(dev) a sensible value should go here, but a quick estimate
	// of 10K per row times 100 results is 1MB, which is an order of
	// magnitude below our 10MB max, so 100 might not be such a bad
	// default.
	if len(in.rows) > 100 {
		return in.Flush(timeout)
	} else {
		return nil
	}
}

func (in *BQInserter) Flush(timeout time.Duration) error {
	if len(in.rows) == 0 {
		return nil
	}
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	// This is heavyweight, and may run forever without a context deadline.
	var rows = in.rows
	in.rows = []interface{}{}
	return in.uploader.Put(ctx, rows)
}

type NullInserter struct {
	Inserter
}

func (in *NullInserter) InsertRows(data interface{}, timeout time.Duration) error {
	return nil
}
func (in *NullInserter) Flush(timeout time.Duration) error {
	return nil
}
