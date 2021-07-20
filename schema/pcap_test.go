package schema

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/go/cloud/bqx"
)

func TestPCAPRow_Schema(t *testing.T) {
	row := &PCAPRow{}
	got, err := row.Schema()
	if err != nil {
		t.Errorf("PCAPRow.Schema() error %v, expected nil", err)
		return
	}
	count := 0
	bqx.WalkSchema(got, func(prefix []string, field *bigquery.FieldSchema) error {
		for _, name := range []string{"id", "parser", "date"} {
			if field.Name == name {
				if field.Description == "" {
					t.Errorf("PCAPRow.Schema() missing field.Description for %q", field.Name)
				} else {
					count++
				}
			}
		}
		return nil
	})
	if count != 3 {
		t.Errorf("PCAPRow.Schema() missing expected fields: got %d, want 3", count)
	}
}
