// +build integration

package main

import (
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
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

func TestLocalRequest(t *testing.T) {
	outdir := t.TempDir()
	flag.Set("output", "local")
	flag.Set("output_dir", outdir)

	mainCtx, mainCancel = context.WithCancel(context.Background())

	go main()
	defer mainCancel()

	// Wait for the server to start and publish address.
	mainSvr := <-mainServerAddr

	// handleLocalRequest
	tests := []struct {
		name       string
		uri        string
		wantStatus int
	}{
		{
			name:       "error-empty-uri",
			uri:        "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "error-malformed-uri",
			uri:        "fake://bucket/path/not-found.tgz",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "error-valid-uri-does-not-exist",
			uri:        "gs://no-such-bucket/ndt/2018/05/09/20180509T101913Z-mlab1-mad03-ndt-0000.tgz",
			wantStatus: http.StatusInternalServerError,
		},
		{
			// Constructed archive with a single, truncated download measurement.
			name:       "success",
			uri:        "gs://archive-mlab-testing/ndt/ndt7/2021/06/17/20210617T003002.410133Z-ndt7-mlab1-foo01-ndt.tgz",
			wantStatus: http.StatusOK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := waitFor("http://" + mainSvr + "/local/worker?filename=" + tt.uri)
			if err != nil {
				t.Fatalf("http.Get returned unexpected error: %v", err)
			}
			if tt.wantStatus != resp.StatusCode {
				t.Errorf("local/worker returned wrong status; got %d, want %d", resp.StatusCode, tt.wantStatus)
			}
			if tt.wantStatus != http.StatusOK {
				return
			}

			// Check the outdir for the successful content of a localwriter.
			p := filepath.Join(outdir, "ndt/ndt7/2021/06/17/20210617T003002.410133Z-ndt7-mlab1-foo01-ndt.tgz.jsonl")
			f, err := os.Open(p)
			if err != nil {
				t.Fatalf("failed to read file: %v", err)
			}
			m := map[string]interface{}{}
			d := json.NewDecoder(f)
			count := 0
			for d.More() {
				err := d.Decode(&m)
				if err != nil {
					// This is not expected.
					t.Fatal(err)
				}
				count++
			}
			if count != 1 {
				t.Errorf("localwriter found multiple records; want 1, got %d", count)
			}
		})
	}
}
