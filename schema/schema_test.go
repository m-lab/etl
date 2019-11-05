package schema_test

import (
	"reflect"
	"testing"

	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/go/bqx"
)

func assertAnnotatable(r *schema.SS) {
	func(parser.Annotatable) {}(r)
}

type unsupportedType struct{}

func Test_findSchemaDocsFor(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  []bqx.SchemaDoc
	}{
		{
			name:  "literal",
			value: schema.NDTResultRow{},
			want: []bqx.SchemaDoc{
				bqx.NewSchemaDoc(schema.MustAsset("toplevel.yaml")),
				bqx.NewSchemaDoc(schema.MustAsset("NDTResultRow.yaml")),
			},
		},
		{
			name:  "pointer",
			value: &schema.NDTResultRow{},
			want: []bqx.SchemaDoc{
				bqx.NewSchemaDoc(schema.MustAsset("toplevel.yaml")),
				bqx.NewSchemaDoc(schema.MustAsset("NDTResultRow.yaml")),
			},
		},
		{
			name:  "unsupported-type-does-not-crash",
			value: &unsupportedType{},
			want: []bqx.SchemaDoc{
				bqx.NewSchemaDoc(schema.MustAsset("toplevel.yaml")),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := schema.FindSchemaDocsFor(tt.value); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("findSchemaDocsFor() = %v, want %v", got, tt.want)
			}
		})
	}
}
