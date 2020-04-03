package worker_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-lab/annotation-service/api"
	v2 "github.com/m-lab/annotation-service/api/v2"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/m-lab/etl/fake"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/worker"

	"github.com/fsouza/fake-gcs-server/fakestorage"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func counterValue(m prometheus.Metric) float64 {
	var mm dto.Metric
	m.Write(&mm)
	ctr := mm.GetCounter()
	if ctr == nil {
		log.Println(mm.GetUntyped())
		return math.Inf(-1)
	}

	return *ctr.Value
}

func checkCounter(t *testing.T, c chan prometheus.Metric, expected float64) {
	m := <-c
	v := counterValue(m)
	if v != expected {
		log.Output(2, fmt.Sprintln("For", m.Desc(), "expected:", expected, "got:", v))
		t.Error("For", m.Desc(), "expected:", expected, "got:", v)
	}
}

// Adds a path from testdata to bucket.
func add(t *testing.T, svr *fakestorage.Server, bucket string, fn string) *fakestorage.Server {
	data, err := ioutil.ReadFile("testdata/" + fn)
	if err != nil {
		t.Fatal(err)
	}
	svr.CreateObject(
		fakestorage.Object{
			BucketName: bucket,
			Name:       fn,
			Content:    data})
	return svr // For chaining
}

func setup(t *testing.T, bucket string, fn string) *fakestorage.Server {
	server := fakestorage.NewServer([]fakestorage.Object{})
	add(t, server, bucket, fn)
	return server
}

func tree(t *testing.T, client *storage.Client) {
	buckets := client.Buckets(context.Background(), "foobar")
	for b, err := buckets.Next(); err == nil; b, err = buckets.Next() {
		t.Log(b.Name)
		it := client.Bucket(b.Name).Objects(context.Background(), &storage.Query{Prefix: ""})
		for o, err := it.Next(); err == nil; o, err = it.Next() {
			t.Log(o.Name)
		}
	}
}

func TestProcessTask(t *testing.T) {
	if testing.Short() {
		t.Log("Skipping integration test")
	}

	fn := "ndt/2018/05/09/20180509T101913Z-mlab1-mad03-ndt-0000.tgz"
	server := setup(t, "test-bucket", fn)
	defer server.Stop()
	tree(t, server.Client())

	status, err := worker.ProcessTaskWithClient(server.Client(), "gs://test-bucket/"+fn)
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusOK {
		t.Fatal("Expected", http.StatusOK, "Got:", status)
	}

	// This section checks that prom metrics are updated appropriately.
	c := make(chan prometheus.Metric, 10)

	metrics.FileCount.Collect(c)
	checkCounter(t, c, 1)

	metrics.TaskCount.Collect(c)
	checkCounter(t, c, 1)

	metrics.TestCount.Collect(c)
	checkCounter(t, c, 1)

	metrics.FileCount.Reset()
	metrics.TaskCount.Reset()
	metrics.TestCount.Reset()
}

type fakeAnnotator struct{}

func (ann *fakeAnnotator) GetAnnotations(ctx context.Context, date time.Time, ips []string, info ...string) (*v2.Response, error) {
	return &v2.Response{AnnotatorDate: time.Now(), Annotations: make(map[string]*api.Annotations, 0)}, nil
}

// Enable this test when we have fixed the prom counter resets.
func TestProcessGKETask(t *testing.T) {
	if testing.Short() {
		t.Log("Skipping integration test")
	}

	server := setup(t, "test-bucket",
		"ndt/ndt5/2019/12/01/20191201T020011.395772Z-ndt5-mlab1-bcn01-ndt.tgz")
	defer server.Stop()

	filename := "gs://test-bucket/ndt/ndt5/2019/12/01/20191201T020011.395772Z-ndt5-mlab1-bcn01-ndt.tgz"
	up := fake.NewFakeUploader()
	client := server.Client()
	status, err := worker.ProcessGKETaskWithClient(client, filename, up, &fakeAnnotator{})
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusOK {
		t.Fatal("Expected", http.StatusOK, "Got:", status)
	}

	// This section checks that prom metrics are updated appropriately.
	c := make(chan prometheus.Metric, 10)

	metrics.FileCount.Collect(c)
	checkCounter(t, c, 488)

	metrics.TaskCount.Collect(c)
	checkCounter(t, c, 1)

	metrics.TestCount.Collect(c)
	checkCounter(t, c, 478)

	if up.Total != 478 {
		t.Error("Expected 478 tests, got", up.Total)
	}
	metrics.FileCount.Reset()
	metrics.TaskCount.Reset()
	metrics.TestCount.Reset()
}
