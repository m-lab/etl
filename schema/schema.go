package schema

import (
	"reflect"

	"github.com/m-lab/go/bqx"
)

// Requires go-bindata tool in environment:
//   go get -u github.com/go-bindata/go-bindata/go-bindata
//
//go:generate go-bindata -pkg schema -nometadata -prefix descriptions descriptions

// findSchemaDocsFor should be used by parser row types to associate bigquery
// field descriptions with a schema generated from a row type.
func findSchemaDocsFor(value interface{}) []bqx.SchemaDoc {
	docs := []bqx.SchemaDoc{}
	// Always include top level schema docs (should be common across row types).
	docs = append(docs, bqx.NewSchemaDoc(MustAsset("toplevel.yaml")))
	t := reflect.TypeOf(value)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	// Look for schema docs based on the given row type.
	docs = append(docs, bqx.NewSchemaDoc(MustAsset(t.Name()+".yaml")))
	return docs
}
