package parser_test

import (
	"fmt"
	"github.com/m-lab/etl/parser"
	"testing"
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
