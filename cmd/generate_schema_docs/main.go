package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/go/bqx"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/rtx"

	"github.com/m-lab/etl/schema"
)

var usage = `
SUMMARY
  Format BigQuery schema field descriptions as a Markdown table.

USAGE
  generate_schema_docs > ndt.md

`

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s\n", os.Args[0])
		fmt.Fprintf(os.Stderr, usage)
		fmt.Fprintln(os.Stderr, "Flags:")
		flag.PrintDefaults()
	}
}

func GenerateNDTResult() {
	row := schema.NDTResult{}
	schema, err := row.Schema()
	rtx.Must(err, "Failed to generate ndt schema")
	fmt.Println("| Field name       | Type       | Description    |")
	fmt.Println("| :----------------|:----------:|:---------------|")
	bqx.WalkSchema(schema, func(prefix []string, field *bigquery.FieldSchema) error {
		var path string
		if len(prefix) == 1 {
			path = ""
		} else {
			path = strings.Join(prefix[:len(prefix)-1], ".") + "."
		}
		fmt.Printf("| %s**%s** | %s | %s |\n", path, prefix[len(prefix)-1], field.Type, field.Description)
		return nil
	})
}

// For now, this just updates all known tables for the provided project.
func main() {
	flag.Parse()
	flagx.ArgsFromEnv(flag.CommandLine)

	errCount := 0

	GenerateNDTResult()

	os.Exit(errCount)
}
