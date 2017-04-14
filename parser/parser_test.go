package parser_test

import (
	"fmt"
	"log"
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/parser"
)

// Just test call to NullParser.HandleTest
func TestPlumbing(t *testing.T) {
	foo := [10]byte{1, 2, 3, 4, 5, 1, 2, 3, 4, 5}
	p := parser.NullParser{}
	_, err := p.HandleTest("foo", "table", foo[:])
	if err != nil {
		fmt.Println(err)
	}
}

func foobar(vs bigquery.ValueSaver) {
	a, b, c := vs.Save()
	log.Printf("%v", reflect.TypeOf(a))
	log.Printf("%v", reflect.TypeOf(b))
	log.Printf("%v", reflect.TypeOf(c))
}

func TestSaver(t *testing.T) {
	fn := "foobar"
	fns := parser.FileNameSaver{map[string]bigquery.Value{"filename": fn}}
	foobar(fns)
}
