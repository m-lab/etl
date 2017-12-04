package testutil_test

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"

	"net/http/httptest"

	"github.com/m-lab/etl/testutil"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// Tests the LoggingClient
func TestLoggingClientBasic(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stdout)

	// Use a local test server.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello, client")
	}))
	defer ts.Close()

	// Use a logging client.
	client, err := testutil.LoggingClient(nil)
	if err != nil {
		t.Fatal(err)
	}

	// Send request through the client to the test URL.
	_, err = client.Get(ts.URL)
	if err != nil {
		t.Error(err)
	}

	// Check that the log buffer contains the expected output.
	if !strings.Contains(buf.String(), "Request:\n") {
		t.Error("Should contain Request: ", buf.String())
	}
	if !strings.Contains(buf.String(), "Response body:") {
		t.Error("Should contain response body")
	}
	if !strings.Contains(buf.String(), "Hello, client") {
		t.Error("Should contain Hello, client: ", buf.String())
	}
}

// Tests the ChannelClient, which pulls responses from a provided
// channel.
func TestChannelClientBasic(t *testing.T) {
	c := make(chan *http.Response, 10)
	client := testutil.ChannelClient(c)

	resp := &http.Response{}
	resp.StatusCode = http.StatusOK
	resp.Status = "OK"
	c <- resp
	resp, err := client.Get("http://foobar")
	log.Printf("%v\n", resp)
	if err != nil {
		t.Error(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Error("Response should be OK: ", resp.Status)
	}
}
