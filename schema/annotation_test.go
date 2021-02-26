package schema

import (
	"flag"
	"testing"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/go/cloud/bqx"
)

func TestAnnotationRow_Schema(t *testing.T) {
	flag.CommandLine.Set("schema.descriptions", "descriptions")
	row := &AnnotationRow{}
	got, err := row.Schema()
	if err != nil {
		t.Fatalf("AnnotationRow.Schema() unexpected error = %v", err)
		return
	}

	count := 0
	// The complete schema is large, so verify that field descriptions
	// are present for select fields by walking the schema and looking for them.
	bqx.WalkSchema(got, func(prefix []string, field *bigquery.FieldSchema) error {
		for _, name := range []string{"client", "server", "parser"} {
			if field.Name == name {
				if field.Description == "" {
					t.Errorf("AnnotationRow.Schema() missing field.Description for %q", field.Name)
				} else {
					count++
				}
			}
		}
		return nil
	})
	if count != 3 {
		t.Errorf("AnnotationRow.Schema() missing expected fields; got %d, want 3", count)
	}
}
