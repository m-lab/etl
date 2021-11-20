package main

// This command requires the GCLOUD_PROJECT environment variable, and takes an optional
// single -updateType flag to specify "all" or "tcpinfo".
// Currently, it handles only the tcpinfo type.
// The specific table to update is currently hardcoded based on the updateType.
//
// Examples:
//  GCLOUD_PROJECT=mlab-sandbox go run cmd/update-schema/update.go
//  GCLOUD_PROJECT=mlab-sandbox go run cmd/update-schema/update.go -updateType=tcpinfo

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

	"github.com/m-lab/go/cloud/bqx"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/rtx"

	"github.com/m-lab/etl/schema"
)

// CreateOrUpdateTCPInfo will update existing TCPInfo table, or create new table if update fails.
func CreateOrUpdateTCPInfo(project string, dataset string, table string) error {
	row := schema.TCPRow{}
	schema, err := row.Schema()
	rtx.Must(err, "TCPRow.Schema")
	if dataset == "batch" {
		updateTemplateTables(schema, project, dataset, table, "")
	}
	return CreateOrUpdate(schema, project, dataset, table, "")
}

func CreateOrUpdatePT(project string, dataset string, table string) error {
	row := schema.PTTest{}
	schema, err := row.Schema()
	rtx.Must(err, "PTTest.Schema")
	if dataset == "batch" {
		updateTemplateTables(schema, project, dataset, table, "")
	}
	return CreateOrUpdate(schema, project, dataset, table, "")
}

func CreateOrUpdateSS(project string, dataset string, table string) error {
	row := schema.SS{}
	schema, err := row.Schema()
	rtx.Must(err, "SS.Schema")
	if dataset == "batch" {
		updateTemplateTables(schema, project, dataset, table, "")
	}
	return CreateOrUpdate(schema, project, dataset, table, "")
}
func CreateOrUpdateNDTWeb100(project string, dataset string, table string) error {
	row := schema.NDTWeb100{}
	schema, err := row.Schema()
	rtx.Must(err, "NDTWeb100.Schema")
	if dataset == "batch" {
		updateTemplateTables(schema, project, dataset, table, "")
	}
	return CreateOrUpdate(schema, project, dataset, table, "")
}

func CreateOrUpdateNDT5ResultRow(project string, dataset string, table string) error {
	row := schema.NDT5ResultRow{}
	schema, err := row.Schema()
	rtx.Must(err, "NDT5ResultRow.Schema")

	// NOTE: NDT5ResultRow does not support the TestTime field yet.
	return CreateOrUpdate(schema, project, dataset, table, "")
}

func CreateOrUpdateNDT5ResultRowStandardColumns(project string, dataset string, table string) error {
	row := schema.NDT5ResultRowStandardColumns{}
	schema, err := row.Schema()
	rtx.Must(err, "NDT5ResultRowStandardColumns.Schema")

	// NOTE: NDT5ResultRow does not support the TestTime field yet.
	return CreateOrUpdate(schema, project, dataset, table, "")
}

func CreateOrUpdateNDT7ResultRow(project string, dataset string, table string) error {
	row := schema.NDT7ResultRow{}
	schema, err := row.Schema()
	rtx.Must(err, "NDT7ResultRow.Schema")
	return CreateOrUpdate(schema, project, dataset, table, "Date")
}

func CreateOrUpdateAnnotationRow(project string, dataset string, table string) error {
	row := schema.AnnotationRow{}
	schema, err := row.Schema()
	rtx.Must(err, "Annotation.Schema")
	return CreateOrUpdate(schema, project, dataset, table, "Date")
}

func CreateOrUpdateSwitchStats(project string, dataset string, table string) error {
	row := schema.SwitchStats{}
	schema, err := row.Schema()
	rtx.Must(err, "SwitchStats.Schema")
	return CreateOrUpdate(schema, project, dataset, table, "")
}

func CreateOrUpdatePCAPRow(project string, dataset string, table string) error {
	row := schema.PCAPRow{}
	schema, err := row.Schema()
	rtx.Must(err, "PCAPRow.Schema")
	return CreateOrUpdate(schema, project, dataset, table, "Date")
}

func CreateOrUpdateHopAnnotation1Row(project string, dataset string, table string) error {
	row := schema.HopAnnotation1Row{}
	schema, err := row.Schema()
	rtx.Must(err, "HopAnnotation1Row.Schema")
	return CreateOrUpdate(schema, project, dataset, table, "Date")
}

func CreateOrUpdateScamper1Row(project string, dataset string, table string) error {
	row := schema.Scamper1Row{}
	schema, err := row.Schema()
	rtx.Must(err, "Scamper1Row.Schema")
	return CreateOrUpdate(schema, project, dataset, table, "Date")
}

// listTemplateTables finds all template tables for the given project, datatype, and base table name.
// Because this function must enumerate all tables in the dataset to find matching names, it may be slow.
func listTemplateTables(project, dataset, table string) ([]string, error) {
	tables := []string{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	client, err := bigquery.NewClient(ctx, project)
	if err != nil {
		return nil, err
	}
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

// updateTemplateTables updates the schema on all template tables for the named dataset and table.
func updateTemplateTables(schema bigquery.Schema, project, dataset, table, partField string) error {
	// Find all template tables for this table.
	tables, err := listTemplateTables(project, dataset, table)
	if err != nil {
		return err
	}
	for i := range tables {
		// Update the template table. There is a small chance that a template
		// table may be deleted between the list and this call, such that the
		// table is recreated here. However, the table will be empty, and used
		// by the next pass of the parser. So, this is expected to be
		// unconditionally safe.
		err = CreateOrUpdate(schema, project, dataset, tables[i], partField)
		if err != nil {
			return err
		}
	}
	return nil
}

// CreateOrUpdate will update or create a table from the given schema.
func CreateOrUpdate(schema bigquery.Schema, project, dataset, table, partField string) error {
	name := project + "." + dataset + "." + table
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

	partitioning := &bigquery.TimePartitioning{
		Field: partField,
	}

	err = pdt.CreateTable(ctx, client, schema, "description", partitioning, nil)
	if err == nil {
		log.Println("Successfully created", pdt)
		return nil
	}
	log.Println("Create failed:", err)
	return err
}

func updateNDT5SC(project string) int {
	errCount := 0
	if err := CreateOrUpdateNDT5ResultRowStandardColumns(project, "raw_ndt", "ndt5"); err != nil {
		errCount++
	}
	if err := CreateOrUpdateNDT5ResultRowStandardColumns(project, "tmp_ndt", "ndt5"); err != nil {
		errCount++
	}
	// TODO enable this after removing ndt.ndt5 views from etl-schema, and migrating
	// measurement-lab:ndt.ndt5 to point to mlab-oti:base_tables.ndt5
	//	if err := CreateOrUpdateNDT5ResultRowStandardColumns(project, "ndt", "ndt5"); err != nil {
	//		errCount++
	//	}
	return errCount
}

// Only tables that support Standard Columns should be included here.
func updateStandardTables(project string) int {
	errCount := 0
	if err := CreateOrUpdateNDT7ResultRow(project, "tmp_ndt", "ndt7"); err != nil {
		errCount++
	}
	if err := CreateOrUpdateNDT7ResultRow(project, "raw_ndt", "ndt7"); err != nil {
		errCount++
	}

	errCount += updateNDT5SC(project)

	if err := CreateOrUpdateAnnotationRow(project, "tmp_ndt", "annotation"); err != nil {
		errCount++
	}
	if err := CreateOrUpdateAnnotationRow(project, "raw_ndt", "annotation"); err != nil {
		errCount++
	}

	if err := CreateOrUpdatePCAPRow(project, "tmp_ndt", "pcap"); err != nil {
		errCount++
	}
	if err := CreateOrUpdatePCAPRow(project, "raw_ndt", "pcap"); err != nil {
		errCount++
	}

	if err := CreateOrUpdateHopAnnotation1Row(project, "tmp_ndt", "hopannotation1"); err != nil {
		errCount++
	}
	if err := CreateOrUpdateHopAnnotation1Row(project, "raw_ndt", "hopannotation1"); err != nil {
		errCount++
	}

	if err := CreateOrUpdateScamper1Row(project, "tmp_ndt", "scamper1"); err != nil {
		errCount++
	}
	if err := CreateOrUpdateScamper1Row(project, "raw_ndt", "scamper1"); err != nil {
		errCount++
	}

	return errCount
}

func updateLegacyTables(project string) int {
	errCount := 0
	if err := CreateOrUpdateTCPInfo(project, "base_tables", "tcpinfo"); err != nil {
		errCount++
	}
	if err := CreateOrUpdateTCPInfo(project, "batch", "tcpinfo"); err != nil {
		errCount++
	}
	if err := CreateOrUpdatePT(project, "base_tables", "traceroute"); err != nil {
		errCount++
	}
	if err := CreateOrUpdatePT(project, "batch", "traceroute"); err != nil {
		errCount++
	}
	if err := CreateOrUpdateSS(project, "base_tables", "sidestream"); err != nil {
		errCount++
	}
	if err := CreateOrUpdateSS(project, "batch", "sidestream"); err != nil {
		errCount++
	}
	if err := CreateOrUpdateNDTWeb100(project, "base_tables", "ndt"); err != nil {
		errCount++
	}
	if err := CreateOrUpdateNDTWeb100(project, "batch", "ndt"); err != nil {
		errCount++
	}
	if err := CreateOrUpdateNDT5ResultRow(project, "base_tables", "ndt5"); err != nil {
		errCount++
	}
	if err := CreateOrUpdateNDT5ResultRow(project, "batch", "ndt5"); err != nil {
		errCount++
	}
	if err := CreateOrUpdateSwitchStats(project, "base_tables", "switch"); err != nil {
		errCount++
	}
	if err := CreateOrUpdateSwitchStats(project, "batch", "switch"); err != nil {
		errCount++
	}
	return errCount
}

var (
	updateType = flag.String("updateType", "", "Short name of datatype to be updated (tcpinfo, scamper, ...).")
	project    = flag.String("gcloud_project", "", "GCP project to update")
)

// For now, this just updates all known tables for the provided project.
func main() {
	flag.Parse()
	flagx.ArgsFromEnv(flag.CommandLine)

	errCount := 0

	if *project == "" {
		log.Fatal("Missing GCLOUD_PROJECT environment variable.")
	}

	switch *updateType {
	case "":
		if len(os.Args) > 1 {
			log.Fatal("Invalid arguments - must include -updateType=...")
		}
		fallthrough
	case "all": // Do everything
		errCount += updateLegacyTables(*project)
		errCount += updateStandardTables(*project)

	case "legacy":
		errCount += updateLegacyTables(*project)

	case "standard":
		errCount += updateStandardTables(*project)

	case "tcpinfo":
		if err := CreateOrUpdateTCPInfo(*project, "base_tables", "tcpinfo"); err != nil {
			errCount++
		}
		if err := CreateOrUpdateTCPInfo(*project, "batch", "tcpinfo"); err != nil {
			errCount++
		}

	case "traceroute":
		if err := CreateOrUpdatePT(*project, "base_tables", "traceroute"); err != nil {
			errCount++
		}
		if err := CreateOrUpdatePT(*project, "batch", "traceroute"); err != nil {
			errCount++
		}
	case "sidestream":
		if err := CreateOrUpdateSS(*project, "base_tables", "sidestream"); err != nil {
			errCount++
		}
		if err := CreateOrUpdateSS(*project, "batch", "sidestream"); err != nil {
			errCount++
		}
	case "ndt":
		if err := CreateOrUpdateNDTWeb100(*project, "base_tables", "ndt"); err != nil {
			errCount++
		}
		if err := CreateOrUpdateNDTWeb100(*project, "batch", "ndt"); err != nil {
			errCount++
		}

	case "ndt5sc":
		errCount += updateNDT5SC(*project)

	case "ndt5":
		if err := CreateOrUpdateNDT5ResultRow(*project, "base_tables", "ndt5"); err != nil {
			errCount++
		}
		if err := CreateOrUpdateNDT5ResultRow(*project, "batch", "ndt5"); err != nil {
			errCount++
		}

	case "ndt7":
		if err := CreateOrUpdateNDT7ResultRow(*project, "tmp_ndt", "ndt7"); err != nil {
			errCount++
		}
		if err := CreateOrUpdateNDT7ResultRow(*project, "raw_ndt", "ndt7"); err != nil {
			errCount++
		}

	case "annotation":
		if err := CreateOrUpdateAnnotationRow(*project, "tmp_ndt", "annotation"); err != nil {
			errCount++
		}
		if err := CreateOrUpdateAnnotationRow(*project, "raw_ndt", "annotation"); err != nil {
			errCount++
		}

	case "switch":
		if err := CreateOrUpdateSwitchStats(*project, "base_tables", "switch"); err != nil {
			errCount++
		}
		if err := CreateOrUpdateSwitchStats(*project, "batch", "switch"); err != nil {
			errCount++
		}

	case "pcap":
		break // For sandbox, skip the update to allow coexistence with other schema changes.
		if err := CreateOrUpdatePCAPRow(*project, "tmp_ndt", "pcap"); err != nil {
			errCount++
		}
		if err := CreateOrUpdatePCAPRow(*project, "raw_ndt", "pcap"); err != nil {
			errCount++
		}

	case "hopannotation1":
		if err := CreateOrUpdateHopAnnotation1Row(*project, "tmp_ndt", "hopannotation1"); err != nil {
			errCount++
		}
		if err := CreateOrUpdateHopAnnotation1Row(*project, "raw_ndt", "hopannotation1"); err != nil {
			errCount++
		}

	case "scamper1":
		if err := CreateOrUpdateScamper1Row(*project, "tmp_ndt", "scamper1"); err != nil {
			errCount++
		}
		if err := CreateOrUpdateScamper1Row(*project, "raw_ndt", "scamper1"); err != nil {
			errCount++
		}

	default:
		log.Fatal("invalid updateType: ", *updateType)
	}

	os.Exit(errCount)
}
