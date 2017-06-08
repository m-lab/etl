package parser_test

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/m-lab/etl/parser"
)

// Not complete, but verifies basic functionality.
func TestMetaParser(t *testing.T) {
	metaName := `20170509T13:45:13.590210000Z_eb.measurementlab.net:53000.meta`
	metaData, err := ioutil.ReadFile(`testdata/` + metaName)
	if err != nil {
		t.Fatalf(err.Error())
	}

	meta := parser.ProcessMetaFile("ndt", "suffix", metaName, metaData)

	if meta == nil {
		t.Error("metaFile has not been populated.")
	}
	timestamp, _ := time.Parse("20060102T15:04:05.999999999Z", "20170509T13:45:13.59021Z")
	if meta.DateTime != timestamp {
		t.Error("Incorrect time: ", meta.DateTime)
	}
	if meta.Tls {
		t.Error("Incorrect TLS: ", meta.Tls)
	}
	if !meta.Websockets {
		t.Error("Incorrect Websockets: ", meta.Websockets)
	}
	if meta.Fields["server hostname"] != "mlab3.vie01.measurement-lab.org" {
		t.Error("Incorrect hostname: ", meta.Fields["hostname"])
	}
}
