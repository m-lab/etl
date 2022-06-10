package schema_test

import (
	"flag"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/m-lab/go/cloud/bqx"
	"github.com/m-lab/go/testingx"

	"github.com/m-lab/etl/schema"
)

type unsupportedType struct{}

func mustReadFile(t *testing.T, f string) []byte {
	b, err := ioutil.ReadFile(f)
	testingx.Must(t, err, "failed to read %s", f)
	return b
}

func Test_findSchemaDocsFor(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  []bqx.SchemaDoc
	}{
		{
			name:  "literal",
			value: schema.NDT5ResultRow{},
			want: []bqx.SchemaDoc{
				bqx.NewSchemaDoc(mustReadFile(t, "descriptions/toplevel.yaml")),
				bqx.NewSchemaDoc(mustReadFile(t, "descriptions/NDT5ResultRow.yaml")),
			},
		},
		{
			name:  "pointer",
			value: &schema.NDT5ResultRow{},
			want: []bqx.SchemaDoc{
				bqx.NewSchemaDoc(mustReadFile(t, "descriptions/toplevel.yaml")),
				bqx.NewSchemaDoc(mustReadFile(t, "descriptions/NDT5ResultRow.yaml")),
			},
		},
		{
			name:  "unsupported-type-does-not-crash",
			value: &unsupportedType{},
			want: []bqx.SchemaDoc{
				bqx.NewSchemaDoc(mustReadFile(t, "descriptions/toplevel.yaml")),
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

func TestMain(m *testing.M) {
	// This sets the flag globally for all "schema" package tests.
	flag.CommandLine.Set("schema.descriptions", "descriptions")
	os.Exit(m.Run())
}
