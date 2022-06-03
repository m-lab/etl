package schema

import (
	"log"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/go/cloud/bqx"
)

func TestScamper1Row_Schema(t *testing.T) {
	row := &Scamper1Row{}
	got, err := row.Schema()
	if err != nil {
		t.Errorf("Scamper1.Schema() error %v, expected nil", err)
		return
	}
	count := 0
	bqx.WalkSchema(got, func(prefix []string, field *bigquery.FieldSchema) error {
		for _, name := range []string{"id", "parser", "date", "raw"} {
			if field.Name == name {
				if field.Description == "" {
					t.Errorf("Scamper1.Schema() missing field.Description for %q", field.Name)
				} else {
					log.Println(field.Name)
					count++
				}
			}
		}
		return nil
	})
	if count != 6 {
		t.Errorf("Scamper1.Schema() missing expected fields: got %d, want 4", count)
	}
}
