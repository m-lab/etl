package parser

import (
	"testing"
)

func TestGetIPTuple(t *testing.T) {
	fn1 := FileName{name: "20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris"}
	ip1, port1, ip2, port2 := fn1.GetIPTuple()
	if ip1 != "98.162.212.214" || port1 != "53849" || ip2 != "64.86.132.75" || port2 != "42677" {
		t.Errorf("Wrong!\n")
		return
	}

}


func TestPTParser(t *testing.T) {
	one_row, _ := PTParser("testdata/20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris")
	fmt.Printf("%v\n", one_row)
}
