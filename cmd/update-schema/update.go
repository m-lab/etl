package main

import (
	"context"
	"log"
	"time"

	"github.com/m-lab/go/rtx"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/go/bqx"
	"google.golang.org/api/googleapi"
)

func CreateOrUpdateTCP(project string) {
	row := parser.TCPRow{}
	schema, err := row.Schema()
	rtx.Must(err, "TCPRow.Schema")

	name := project + ".ndt.tcpinfo"
	log.Println("Using:", name)
	pdt, err := bqx.ParsePDT(name)
	rtx.Must(err, "ParsePDT")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client, err := bigquery.NewClient(ctx, pdt.Project)
	rtx.Must(err, "NewClient")

	log.Println("Trying Update")
	// Update non-existing table
	err = pdt.UpdateTable(ctx, client, schema)
	if err != nil {
		apiErr, ok := err.(*googleapi.Error)
		if !ok || apiErr.Code != 404 {
		}

		log.Println("UpdateTable failed:", err)

		log.Println("Trying Create")
		err = pdt.CreateTable(ctx, client, schema, "description",
			&bigquery.TimePartitioning{Field: "TestTime"}, nil)

		rtx.Must(err, "CreateTable")
	}
}

func main() {
	CreateOrUpdateTCP("mlab-testing")
	CreateOrUpdateTCP("mlab-sandbox")
	CreateOrUpdateTCP("mlab-staging")
}
