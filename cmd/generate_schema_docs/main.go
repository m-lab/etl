// Copyright 2019 ETL Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//////////////////////////////////////////////////////////////////////////////

// generate_schema_docs uses ETL schema field descriptions to generate
// documentation in various formats.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"reflect"
	"strings"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/go/cloud/bqx"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/rtx"

	"github.com/m-lab/etl/schema"
)

var usage = `
SUMMARY
  Format BigQuery schema field descriptions as a Markdown table.

USAGE
  $ generate_schema_docs -doc.output ./include
  Writing include/schema_ndtresult.md

`

// Flags
var (
	outputFormat    string
	outputDirectory string
)

func init() {
	log.SetFlags(0)
	flag.StringVar(&outputFormat, "doc.format", "md", "Format for output files.")
	flag.StringVar(&outputDirectory, "doc.output", ".", "Write files to given directory.")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s\n", os.Args[0])
		fmt.Fprintf(os.Stderr, usage)
		fmt.Fprintln(os.Stderr, "Flags:")
		flag.PrintDefaults()
	}
}

func generateRichMarkdown(s bigquery.Schema, t schemaGenerator) []byte {
	// Load raw docs for the given schema type so that we can extract all fields.
	docs := schema.FindSchemaDocsFor(t)
	combo := map[string]map[string]string{}
	for _, doc := range docs {
		for k, v := range doc {
			combo[k] = v
		}
	}

	buf := &bytes.Buffer{}
	fmt.Fprintln(buf, "| Field name       | Type       | Description    |")
	fmt.Fprintln(buf, "| :----------------|:----------:|:---------------|")
	bqx.WalkSchema(
		s, func(prefix []string, field *bigquery.FieldSchema) error {

			// Search for the path in the given doc.
			var ok bool
			var d map[string]string
			// Starting with the longest prefix, stop looking for descriptions on first match.
			for start := 0; start < len(prefix) && !ok; start++ {
				path := strings.Join(prefix[start:], ".")
				d, ok = combo[path]
			}

			// We found relevant documentation, now concatenate the fields when found.
			richDesc := d["Description"]
			if val, ok := d["Discussion"]; ok && val != "" {
				richDesc += "<br>" + val
			}
			if val, ok := d["Kernel"]; ok && val != "" {
				richDesc += "<br>Kernel: " + val
			}

			var path string
			if len(prefix) == 1 {
				path = ""
			} else {
				path = strings.Join(prefix[:len(prefix)-1], ".") + "."
			}

			fmt.Fprintf(buf, "| %s**%s** | %s | %s |\n", path, prefix[len(prefix)-1], field.Type, richDesc)
			return nil
		},
	)
	return buf.Bytes()
}

// TODO: remove this function if it turns out to be replaced by generateRichMarkdown.
func generateMarkdown(schema bigquery.Schema) []byte {
	buf := &bytes.Buffer{}
	fmt.Fprintln(buf, "| Field name       | Type       | Description    |")
	fmt.Fprintln(buf, "| :----------------|:----------:|:---------------|")
	bqx.WalkSchema(schema, func(prefix []string, field *bigquery.FieldSchema) error {
		var path string
		if len(prefix) == 1 {
			path = ""
		} else {
			path = strings.Join(prefix[:len(prefix)-1], ".") + "."
		}
		fmt.Fprintf(buf, "| %s**%s** | %s | %s |\n", path, prefix[len(prefix)-1], field.Type, field.Description)
		return nil
	})
	return buf.Bytes()
}

// All record structs define a Schema method. This interface allows us to
// process each of them easily.
type schemaGenerator interface {
	Schema() (bigquery.Schema, error)
}

// shortNameOf returns the short type name of the underlying schemaGenerator type.
// NOTE: the generator must reference an underlying pointer type,
// e.g. `&schema.NDT5ResultRow{}` not `schema.NDT5ResultRow{}`
func shortNameOf(g schemaGenerator) string {
	return strings.ToLower(reflect.TypeOf(g).Elem().Name())
}

func main() {
	flag.Parse()
	flagx.ArgsFromEnv(flag.CommandLine)

	generators := []schemaGenerator{
		&schema.AnnotationRow{},
		&schema.NDT5ResultRow{},
		&schema.NDT7ResultRow{},
		&schema.TCPRow{},
		&schema.PTTest{},
		&schema.PCAPRow{},
		// TODO(https://github.com/m-lab/etl/issues/745): Add additional types once
		// "standard columns" are resolved.
	}

	for _, current := range generators {
		name := shortNameOf(current)
		schema, err := current.Schema()
		rtx.Must(err, "Failed to generate Schema for %s", name)

		var b []byte
		switch outputFormat {
		case "md":
			b = generateRichMarkdown(schema, current)
		default:
			log.Fatalf("Unsupported output format: %q", outputFormat)
		}

		file := path.Join(outputDirectory, "schema_"+name+"."+outputFormat)
		log.Printf("Writing %s", file)
		err = ioutil.WriteFile(file, b, 0644)
		rtx.Must(err, "Failed to write file: %q", file)
	}
}
