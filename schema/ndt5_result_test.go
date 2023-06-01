package schema

import (
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/go/cloud/bqx"
)

func TestNDT5ResultV2_Schema(t *testing.T) {
	row := &NDT5ResultRowV2{}
	got, err := row.Schema()
	if err != nil {
		t.Errorf("NDT5ResultV2.Schema() error = %v, expected nil", err)
		return
	}
	count := 0
	fields := 0
	descriptions := 0
	// The complete schema is large, so verify that field descriptions
	// are present for select fields by walking the schema and looking for them.
	bqx.WalkSchema(got, func(prefix []string, field *bigquery.FieldSchema) error {
		fields++
		if field.Description != "" {
			descriptions++
		} else if field.Type == bigquery.RecordFieldType {
			t.Logf("No description for %s", strings.Join(prefix, "."))
		} else {
			t.Logf("No description for %s.%s", strings.Join(prefix, "."), field.Name)
		}
		// a and parser are top level records
		// GitShortCommit is a field in raw
		for _, name := range []string{"id", "a", "parser", "GitShortCommit"} {
			if field.Name == name {
				if field.Description == "" {
					t.Errorf("NDT5ResultV2.Schema() missing field.Description for %q", field.Name)
				} else {
					count++
				}
			}
		}
		return nil
	})
	if count != 4 {
		t.Errorf("NDT5ResultV2.Schema() missing expected fields; got %d, want 4", count)
	}
	if descriptions != fields {
		// Log if there are missing descriptions
		t.Log(descriptions, "!=", fields)
	}
}
