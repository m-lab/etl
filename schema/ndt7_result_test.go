package schema

import (
	"testing"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/go/cloud/bqx"
)

func TestNDT7Result_Schema(t *testing.T) {
	row := &NDT7ResultRow{}
	got, err := row.Schema()
	if err != nil {
		t.Errorf("NDT7Result.Schema() error = %v, expected nil", err)
		return
	}
	count := 0
	// The complete schema is large, so verify that field descriptions
	// are present for select fields by walking the schema and looking for them.
	bqx.WalkSchema(got, func(prefix []string, field *bigquery.FieldSchema) error {
		for _, name := range []string{"a", "parseInfo", "GitShortCommit"} {
			if field.Name == name {
				if field.Description == "" {
					t.Errorf("NDT7Result.Schema() missing field.Description for %q", field.Name)
				} else {
					count++
				}
			}
		}
		return nil
	})
	if count != 3 {
		t.Errorf("NDT7Result.Schema() missing expected fields; got %d, want 3", count)
	}
}
