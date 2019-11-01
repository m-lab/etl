package schema

import (
	"testing"

	"cloud.google.com/go/bigquery"
)

func TestNDTRow_Schema(t *testing.T) {
	row := &NDTRow{}
	got, err := row.Schema()
	if err != nil {
		t.Errorf("NDTRow.Schema() error = %v, expected nil", err)
		return
	}
	// The complete schema is large, so verify that field descriptions
	// are present for select fields.
	fields := []*bigquery.FieldSchema(got)
	for _, field := range fields {
		for _, name := range []string{"test_id", "ParseInfo", "GitShortCommit"} {
			if field.Name == name && field.Description == "" {
				t.Errorf("NDTRow.Schema() missing field.Description for %q", field.Name)
			}
		}
	}
}
