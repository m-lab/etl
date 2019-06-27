package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"github.com/m-lab/etl/schema"
	"github.com/m-lab/go/rtx"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/go/bqx"
	"google.golang.org/api/googleapi"
)

// CreateOrUpdateTCP will update existing table, or create new table if update fails.
func CreateOrUpdateTCP(project string, dataset string, table string) error {
	row := schema.TCPRow{}
	schema, err := row.Schema()
	rtx.Must(err, "TCPRow.Schema")

	name := project + "." + dataset + "." + table
	log.Println("Using:", name)
	pdt, err := bqx.ParsePDT(name)
	rtx.Must(err, "ParsePDT")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client, err := bigquery.NewClient(ctx, pdt.Project)
	rtx.Must(err, "NewClient")

	err = pdt.UpdateTable(ctx, client, schema)
	if err == nil {
		log.Println("Successfully updated", pdt)
		return nil
	}
	log.Println("UpdateTable failed:", err)
	// TODO add specific error handling for incompatible schema change

	apiErr, ok := err.(*googleapi.Error)
	if !ok || apiErr.Code != 404 {
		// TODO - different behavior on specific error types?
	}

	err = pdt.CreateTable(ctx, client, schema, "description",
		&bigquery.TimePartitioning{ /*Field: "TestTime"*/ }, nil)
	if err == nil {
		log.Println("Successfully created", pdt)
		return nil
	}
	log.Println("Create failed:", err)
	return err
}

var (
	updateType = flag.String("updateType", "", "Short name of datatype to be updated (tcpinfo, scamper, ...).")
)

// For now, this just updates all known tables for the provided project.
func main() {
	flag.Parse()
	errCount := 0

	project := os.Getenv("GCLOUD_PROJECT")
	if project == "" {
		log.Fatal("Missing GCLOUD_PROJECT environment variable.")
	}

	switch *updateType {
	case "":
		fallthrough
	case "all": // Do everything
		if err := CreateOrUpdateTCP(project, "base_tables", "tcpinfo"); err != nil {
			errCount++
		}
		if err := CreateOrUpdateTCP(project, "batch", "tcpinfo"); err != nil {
			errCount++
		}

	case "tcpinfo":
		if err := CreateOrUpdateTCP(project, "base_tables", "tcpinfo"); err != nil {
			errCount++
		}
		if err := CreateOrUpdateTCP(project, "batch", "tcpinfo"); err != nil {
			errCount++
		}

	default:
		log.Fatal("invalid updateType")
	}

	os.Exit(errCount)
}
