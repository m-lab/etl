package schema

import (
	"log"
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
	// Look for schema docs based on the given row type. Ignore missing schema docs.
	b, err := Asset(t.Name() + ".yaml")
	if err == nil {
		docs = append(docs, bqx.NewSchemaDoc(b))
	} else {
		log.Printf("WARNING: no file for schema field description: %s.yaml", t.Name())
	}
	return docs
}
