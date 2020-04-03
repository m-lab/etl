package schema

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/go/bqx"
)

func TestAnnotationRow_Schema(t *testing.T) {
	tests := []struct {
		name    string
		want    bigquery.Schema
		wantErr bool
	}{
		{
			name: "success",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := &AnnotationRow{}
			got, err := row.Schema()
			if (err != nil) != tt.wantErr {
				t.Errorf("AnnotationRow.Schema() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			count := 0
			// The complete schema is large, so verify that field descriptions
			// are present for select fields by walking the schema and looking for them.
			bqx.WalkSchema(got, func(prefix []string, field *bigquery.FieldSchema) error {
				for _, name := range []string{"client", "server", "parseInfo"} {
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
		})
	}
}
