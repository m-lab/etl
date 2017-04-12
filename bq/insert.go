package bq

import (
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/context"
)

// An Inserter provides the InsertRows function.
type Inserter interface {
	InsertRows(data interface{}, timeout time.Duration) error
}

type BQInserter struct {
	Inserter
	client   *bigquery.Client
	uploader *bigquery.Uploader
}

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
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	// This is heavyweight, and may run forever without a context deadline.
	return in.uploader.Put(ctx, data)
}

type NullInserter struct {
	Inserter
}

func (in *NullInserter) InsertRows(data interface{}, timeout time.Duration) error {
	return nil
}
