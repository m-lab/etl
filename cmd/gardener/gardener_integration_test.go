// +build integration

package main

import (
	"testing"

	"cloud.google.com/go/datastore"
	"golang.org/x/net/context"
)

func TestStartup(t *testing.T) {
	client, err := datastore.NewClient(context.Background(), "mlab-testing")
	if err != nil {
		t.Fatal(err)
	} else {
		dsClient = client
	}

	_, err = StartupBatch("base-", 2)
	if err != nil {
		t.Fatal(err)
	}

	// TODO add tests for BatchState content once it has some.
}
