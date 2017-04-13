package parser

import (
	"fmt"
	"testing"
)

func TestPTParser(t *testing.T) {
	one_row, _ := PTParser("testdata/20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris")
	fmt.Printf("%v\n", one_row)
}
