// +build integration

package main

import (
	"context"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/m-lab/etl/etl"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// Retries for up to 10 seconds.
func waitFor(url string) (resp *http.Response, err error) {
	for i := 0; i < 1000; i++ {
		time.Sleep(10 * time.Millisecond)
		resp, err = http.Get(url)
		if err == nil {
			break
		}
	}
	return resp, err
}

func TestMain(t *testing.T) {
	flag.Set("service_port", ":0")
	flag.Set("max_active", "200")
	flag.Set("prometheusx.listen-address", ":0")
	flag.Set("max_workers", "25")
	flag.Set("gcloud_project", "mlab-testing")
	mainCtx, mainCancel = context.WithCancel(context.Background())

	go main()
	defer mainCancel()

	// Wait for the server to start and publish address.
	mainSvr := <-mainServerAddr

	// Wait until the mainSvr is "ready"
	resp, err := waitFor("http://" + mainSvr + "/ready")
	if err != nil {
		t.Fatal(err)
	}

	// For now, the service comes up immediately serving "ok" for /ready
	data, err := ioutil.ReadAll(resp.Body)
	if string(data) != "ok" {
		t.Fatal(string(data))
	}
	resp.Body.Close()
	log.Println("ok")

	// Now get the status
	resp, err = waitFor("http://" + mainSvr)
	if err != nil {
		t.Fatal(err)
	}
	data, err = ioutil.ReadAll(resp.Body)
	if !strings.Contains(string(data), "Workers") {
		t.Error("Should contain 'Workers':\n", string(data))
	}
	if !strings.Contains(string(data), "BigQuery") {
		t.Error("Should contain 'BigQuery':\n", string(data))
	}
	resp.Body.Close()

	if *maxActiveTasks != 200 {
		t.Error("Expected 200:", *maxActiveTasks)
	}
}

func TestPollingMode(t *testing.T) {
	flag.Set("service_port", ":0")
	flag.Set("max_active", "200")
	flag.Set("prometheusx.listen-address", ":0")
	flag.Set("max_workers", "25")
	flag.Set("gcloud_project", "mlab-testing")
	flag.Set("gardener_host", "gardener")
	etl.GitCommit = "123456789ABCDEF"
	mainCtx, mainCancel = context.WithCancel(context.Background())

	go main()
	defer mainCancel()

	// Wait for the server to start and publish address.
	mainSvr := <-mainServerAddr

	// Wait until the mainSvr is "ready"
	resp, err := waitFor("http://" + mainSvr + "/ready")
	if err != nil {
		t.Fatal(err)
	}

	// For now, the service comes up immediately serving "ok" for /ready
	data, err := ioutil.ReadAll(resp.Body)
	if string(data) != "ok" {
		t.Fatal(string(data))
	}
	resp.Body.Close()
	log.Println("ok")

	// Now get the status
	resp, err = waitFor("http://" + mainSvr)
	if err != nil {
		t.Fatal(err)
	}
	data, err = ioutil.ReadAll(resp.Body)
	// Check expected GardenerAPI
	if !strings.Contains(string(data), "http://gardener:8080") {
		t.Error("Should contain 'Gardener API: http://gardener:8080':\n", string(data))
	}
	resp.Body.Close()

	if *maxActiveTasks != 200 {
		t.Error("Expected 200:", *maxActiveTasks)
	}

}
