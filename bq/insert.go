package bq

import (
	//"log"
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/context"
)

// An Inserter
type Inserter struct {
	client   *bigquery.Client
	uploader *bigquery.Uploader
}

func NewInserter(project string, dataset string, table string) (*Inserter, error) {

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	// Heavyweight!
	client, err := bigquery.NewClient(ctx, project)
	if err != nil {
		return nil, err
	}

	uploader := client.Dataset(dataset).Table(table).Uploader()
	in := Inserter{client: client, uploader: uploader}
	return &in, nil
}

func (in *Inserter) InsertRows(data interface{}, timeout time.Duration) error {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	// This is heavyweight, and may run forever without a context deadline.
	return in.uploader.Put(ctx, data)
}

func InsertRows(client *bigquery.Client, datasetID string, tableID string, data []interface{}) error {
	// This is lightweight.
	u := client.Dataset(datasetID).Table(tableID).Uploader()
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	// This is heavyweight, and may run forever without a context deadline.
	return u.Put(ctx, data)
}
