package schema

import (
	"flag"
	"io/ioutil"
	"log"
	"path"
	"reflect"
	"time"

	"github.com/m-lab/go/cloud/bqx"
	"github.com/m-lab/go/rtx"
)

// ParseInfo provides details about the parsed row. Uses 'Standard Column' names.
type ParseInfo struct {
	ParserVersion string
	ParseTime     time.Time
	ArchiveURL    string
	Filename      string
	Priority      int64
}

// Requires go-bindata tool in environment:
//   go get -u github.com/go-bindata/go-bindata/go-bindata
//
//go:generate go-bindata -pkg schema -nometadata -prefix descriptions descriptions

// FindSchemaDocsFor should be used by parser row types to associate bigquery
// field descriptions with a schema generated from a row type.
func FindSchemaDocsFor(value interface{}) []bqx.SchemaDoc {
	docs := []bqx.SchemaDoc{}
	// Always include top level schema docs (should be common across row types).
	b, err := readAsset("toplevel.yaml")
	rtx.Must(err, "Failed to read toplevel.yaml")
	docs = append(docs, bqx.NewSchemaDoc(b))
	t := reflect.TypeOf(value)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	// Look for schema docs based on the given row type. Ignore missing schema docs.
	b, err = readAsset(t.Name() + ".yaml")
	if err == nil {
		docs = append(docs, bqx.NewSchemaDoc(b))
	} else {
		log.Printf("WARNING: no file for schema field description: %s.yaml", t.Name())
	}
	return docs
}

// assetDir provides a mechanism to override the embedded schema files.
var assetDir string

func init() {
	flag.StringVar(&assetDir, "schema-asset-dir", "", "Read description files from the given directory instead of embedded files.")
}

func readAsset(name string) ([]byte, error) {
	if assetDir == "" {
		return Asset(name)
	}
	return ioutil.ReadFile(path.Join(assetDir, name))
}
