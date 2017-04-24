package parser

import (
	"io/ioutil"
	"testing"
)

// TODO: IPv6 tests
func TestGetIPTuple(t *testing.T) {
	fn1 := PTFileName{name: "20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris"}
	dest_ip, dest_port, server_ip, server_port := fn1.GetIPTuple()
	if dest_ip != "98.162.212.214" || dest_port != "53849" || server_ip != "64.86.132.75" || server_port != "42677" {
		t.Errorf("Wrong!\n")
		return
	}

}

func TestPTParser(t *testing.T) {
	rawData, err := ioutil.ReadFile("testdata/20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris")

	n := PTParser{tmpDir: "./"}
	_, err = n.Parse(nil, "testdata/20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris", "table", rawData)
	if err != nil {
		t.Fatalf(err.Error())
	}
}
