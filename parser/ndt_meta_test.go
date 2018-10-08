package parser_test

import (
	"io/ioutil"
	"syscall"
	"testing"
	"time"

	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
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

	// Check for the presenece of select connSpec fields.
	connSpec := schema.EmptyConnectionSpec()
	meta.PopulateConnSpec(connSpec)

	// This particular file is missing the server_ip address...
	if _, ok := connSpec["server_ip"]; ok {
		t.Error("expected server_ip to be empty")
	}

	// But the client_ip address (and client_af) should be fine.
	if v, ok := connSpec["client_ip"]; !ok {
		t.Logf("missing client ip address")
		for k, v := range meta.Fields {
			t.Logf("%s : %s\n", k, v)
		}
		t.Error("missing client ip address")
	} else {
		t.Logf("found client ip: %v\n", v)
	}

	if v, ok := connSpec["client_af"]; !ok {
		t.Logf("missing client_af annotation")
		t.Error("missing client_af")
	} else {
		if v.(int64) != syscall.AF_INET {
			t.Log("Wrong client_af value: ", v.(int64))
		}

	}

	if s, ok := connSpec["tls"]; ok {
		t.Errorf("Found field tls: got %q; want not found", s)
	}
	if connSpec["websockets"] != true {
		t.Errorf("Incorrect websockets: got %q; want true", connSpec["websockets"])
	}
	if connSpec["client_kernel_version"] != "3.14.0" {
		t.Errorf("Incorrect client_kernel_version: got %s; want 3.14.0",
			connSpec["client_kernel_version"])
	}
	if connSpec["client_version"] != "3.7.0" {
		t.Errorf("Incorrect client_version: got %s; want 3.7.0",
			connSpec["client_version"])
	}
	if connSpec["client_os"] != "CLIWebsockets" {
		t.Errorf("Incorrect client_os: got %s; want CLIWebsockets",
			connSpec["client_os"])
	}
}
