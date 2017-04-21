// TODO(soon) Implement good tests for the existing parsers.
//
package parser_test

import (
	"fmt"
	"testing"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/parser"
)

// Just test call to NullParser.Parser
func TestPlumbing(t *testing.T) {
	foo := [10]byte{1, 2, 3, 4, 5, 1, 2, 3, 4, 5}
	p := parser.NullParser{}
	_, err := p.Parse(nil, "foo", foo[:])
	if err != nil {
		fmt.Println(err)
	}
}

func foobar(vs bigquery.ValueSaver) {
	_, _, _ = vs.Save()
}

func TestSaverInterface(t *testing.T) {
	fns := parser.FileNameSaver{map[string]bigquery.Value{"filename": "foobar"}}
	foobar(&fns)
}
