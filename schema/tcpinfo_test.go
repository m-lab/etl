package schema_test

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/go/cloud/bqx"
)

func TestTCPInfoRow_Schema(t *testing.T) {
	row := &schema.TCPInfoRow{}
	got, err := row.Schema()
	if err != nil {
		t.Errorf("TCPInfoRow.Schema() error %v, expected nil", err)
		return
	}
	count := 0
	bqx.WalkSchema(got, func(prefix []string, field *bigquery.FieldSchema) error {
		for _, name := range []string{"id", "a", "parser", "date", "raw"} {
			if field.Name == name {
				if field.Description == "" {
					t.Errorf("TCPInfoRow.Schema() missing field.Description for %q", field.Name)
				} else {
					count++
				}
			}
		}
		return nil
	})
	if count != 5 {
		t.Errorf("TCPInfoRow.Schema() missing expected fields: got %d, want 3", count)
	}
}
