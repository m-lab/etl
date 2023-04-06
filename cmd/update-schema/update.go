package main

// update-schema manages the creation and update of schemas produced by the data
// pipeline.  Tables are created per `-project`, using the `-experiment` and
// `-datatype`.
//
// If `-standard=true` then all standard experiments and datatypes are created at once.
// If `-legacy=true` then all legacy datatypes are created at once.
// Use `-sidecars=true` to create only the sidecar datatypes for a new experiment.
//
// Examples:
//  # Make all supported standard column experiment tables.
//  update-schema -project mlab-sandbox -standard
//
//  # Make all sidecar tables for ndt experiments.
//  update-schema -project mlab-sandbox -experiment ndt -sidecars
//
//  # Make the scamper1 table for the wehe experiment.
//  update-schema -project mlab-sandbox -experiment wehe -datatype scamper1

import (
	"context"
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"

	"github.com/m-lab/etl/schema"
	"github.com/m-lab/go/cloud/bqx"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/rtx"
)

var (
	datatype   = flag.String("datatype", "", "Datatype name to be updated (tcpinfo, scamper1, ...).")
	experiment = flag.String("experiment", "", "Name of experiment to be updated (ndt, wehe, ...).")
	project    = flag.String("project", "", "GCP project to update")
	standard   = flag.Bool("standard", false, "Create or update default standard tables and datatypes")
	sidecars   = flag.Bool("sidecars", false, "Create or update sidecar tables for the given experiment")
	legacy     = flag.Bool("legacy", false, "Create or update legacy tables")

	schemas = map[string]schemaGenerator{
		"annotation2":    &schema.Annotation2Row{},
		"hopannotation2": &schema.HopAnnotation2Row{},
		"ndt5":           &schema.NDT5ResultRowV2{},
		"ndt7":           &schema.NDT7ResultRow{},
		"tcpinfo":        &schema.TCPInfoRow{},
		"pcap":           &schema.PCAPRow{},
		"scamper1":       &schema.Scamper1Row{},
		"switch":         &schema.SwitchRow{},
	}

	schemasLegacy = map[string]schemaGenerator{
		"traceroute": &schema.PTTest{},
		"sidestream": &schema.SS{},
		"ndt":        &schema.NDTWeb100{},
	}
)

type schemaGenerator interface {
	Schema() (bigquery.Schema, error)
}

// listLegacyTemplateTables finds all template tables for the given project, datatype, and base table name.
// Because this function must enumerate all tables in the dataset to find matching names, it may be slow.
func listLegacyTemplateTables(client *bigquery.Client, project, dataset, table string) ([]string, error) {
	tables := []string{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	it := client.Dataset(dataset).Tables(ctx)
	for {
		t, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(t.TableID, table+"_") {
			tables = append(tables, t.TableID)
		}
	}
	return tables, nil
}

// updateLegacyTemplateTables updates the schema on all template tables for the named dataset and table.
func updateLegacyTemplateTables(client *bigquery.Client, schema bigquery.Schema, project, dataset, table, partField string) int {
	// Find all template tables for this table.
	errCount := 0
	tables, err := listLegacyTemplateTables(client, project, dataset, table)
	if err != nil {
		log.Println(err)
		return 1
	}
	for i := range tables {
		// Update the template table. There is a small chance that a template
		// table may be deleted between the list and this call, such that the
		// table is recreated here. However, the table will be empty, and used
		// by the next pass of the parser. So, this is expected to be
		// unconditionally safe.
		errCount += CreateOrUpdate(client, schema, project, dataset, tables[i], partField)
	}
	return errCount
}

// CreateOrUpdate will update or create a table from the given schema.
func CreateOrUpdate(client *bigquery.Client, schema bigquery.Schema, project, dataset, table, partField string) int {
	name := project + "." + dataset + "." + table
	pdt, err := bqx.ParsePDT(name)
	rtx.Must(err, "ParsePDT")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// Assume if Metadata is available, that the dataset already exists.
	ds := client.Dataset(dataset)
	_, err = ds.Metadata(ctx)
	if err != nil {
		// Assume error indicates dataset does not yet exist.
		err = ds.Create(ctx, &bigquery.DatasetMetadata{
			Name:     dataset,
			Location: "US",
		})
		if err != nil {
			log.Printf("failed to create dataset: %v", dataset)
			return 1
		}
		log.Println("Successfully created dataset for", pdt)
	}

	err = pdt.UpdateTable(ctx, client, schema)
	if err == nil {
		log.Println("Successfully updated", pdt)
		return 0
	}
	log.Println("UpdateTable failed:", err)
	apiErr, ok := err.(*googleapi.Error)
	if !ok || apiErr.Code != 404 {
		// TODO - different behavior on specific error types?
		log.Printf("failed to update schema: %v", err)
		return 1
	}

	partitioning := &bigquery.TimePartitioning{
		Field: partField,
	}

	err = pdt.CreateTable(ctx, client, schema, "", partitioning, nil)
	if err == nil {
		log.Println("Successfully created", pdt)
		return 0
	}
	log.Println("Create failed:", err)
	return 1
}

// Only tables that support Standard Columns should be included here.
func updateStandardTables(client *bigquery.Client, project, experiment string) int {
	errCount := 0
	tables := []string{
		"annotation2",
		"hopannotation2",
		"pcap",
		"scamper1",
		"tcpinfo",
	}
	for _, datatype := range tables {
		errCount += makeTables(client, project, experiment, datatype)
	}
	return errCount
}

func updateLegacyTables(client *bigquery.Client, project string) int {
	errCount := 0
	tables := []string{
		"traceroute",
		"sidestream",
		"ndt",
	}
	for _, table := range tables {
		s, ok := schemasLegacy[table]
		if !ok {
			log.Printf("failed to find %v", table)
			errCount++
			continue
		}
		schema, err := s.Schema()
		rtx.Must(err, "failed to generate schema for %s", *datatype)
		errCount += CreateOrUpdate(client, schema, project, "base_tables", table, "")
		errCount += updateLegacyTemplateTables(client, schema, project, "batch", table, "")
		errCount += CreateOrUpdate(client, schema, project, "batch", table, "")
	}
	return errCount
}

func makeTables(client *bigquery.Client, project, experiment, datatype string) int {
	errCount := 0
	if _, ok := schemas[datatype]; !ok {
		log.Fatal("unsupported datatype:", datatype)
	}
	s := schemas[datatype]
	schema, err := s.Schema()
	rtx.Must(err, "failed to generate schema for %s", datatype)
	errCount += CreateOrUpdate(client, schema, project, "tmp_"+experiment, datatype, "date")
	errCount += CreateOrUpdate(client, schema, project, "raw_"+experiment, datatype, "date")
	return errCount
}

// For now, this just updates all known tables for the provided project.
func main() {
	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "failed to read flags from env")

	errCount := 0
	client, err := bigquery.NewClient(context.Background(), *project)
	rtx.Must(err, "failed to create bigquery.NewClient")

	// Project is required for all options.
	if *project == "" {
		log.Fatal("-project flag is required")
	}
	if *standard {
		// Create all supported tables.
		errCount += makeTables(client, *project, "ndt", "ndt7")
		errCount += makeTables(client, *project, "ndt", "ndt5")
		errCount += updateStandardTables(client, *project, "ndt")
		errCount += makeTables(client, *project, "utilization", "switch")
		os.Exit(errCount)
	}
	if *legacy {
		// Create all legacy tables.
		errCount += updateLegacyTables(client, *project)
		os.Exit(errCount)
	}

	// Experiment is required at this point.
	if *experiment == "" {
		log.Fatal("-experiment flag is required")
	}
	if *sidecars {
		errCount += updateStandardTables(client, *project, *experiment)
		os.Exit(errCount)
	}

	// Datatype is required at this point.
	if *datatype == "" {
		log.Fatal("-datatype flag is required")
	}
	// Create only the named experiment and datatype.
	errCount += makeTables(client, *project, *experiment, *datatype)
	os.Exit(errCount)
}
