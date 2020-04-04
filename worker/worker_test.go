package worker_test

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-lab/annotation-service/api"
	v2 "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/go/rtx"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/m-lab/etl/fake"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/worker"

	"github.com/fsouza/fake-gcs-server/fakestorage"
)

var gcsClient *storage.Client

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	gcsClient = fromTar("test-bucket", "../testfiles/ndt.tar").Client()
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
func add(svr *fakestorage.Server, bucket string, fn string, rdr io.Reader) {
	data, err := ioutil.ReadAll(rdr)
	rtx.Must(err, "Error reading data for", fn)
	svr.CreateObject(
		fakestorage.Object{
			BucketName: bucket,
			Name:       fn,
			Content:    data})
}

func loadFromTar(svr *fakestorage.Server, bucket string, tf *tar.Reader) *fakestorage.Server {
	for h, err := tf.Next(); err != io.EOF; h, err = tf.Next() {
		if h.Typeflag == tar.TypeReg {
			add(svr, bucket, h.Name, tf)
		}
	}
	return svr
}

func fromTar(bucket string, fn string) *fakestorage.Server {
	server := fakestorage.NewServer([]fakestorage.Object{})
	f, err := os.Open(fn)
	rtx.Must(err, "opening tar file")
	defer f.Close()
	tf := tar.NewReader(f)
	return loadFromTar(server, bucket, tf)
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

func TestLoadTar(t *testing.T) {
	t.Skip("Useful for debugging")
	tree(t, gcsClient)
	t.Fatal()
}

func TestProcessTask(t *testing.T) {
	if testing.Short() {
		t.Log("Skipping integration test")
	}

	fn := "ndt/2018/05/09/20180509T101913Z-mlab1-mad03-ndt-0000.tgz"

	status, err := worker.ProcessTaskWithClient(gcsClient, "gs://test-bucket/"+fn)
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

	filename := "gs://test-bucket/ndt/ndt5/2019/12/01/20191201T020011.395772Z-ndt5-mlab1-bcn01-ndt.tgz"
	up := fake.NewFakeUploader()
	status, err := worker.ProcessGKETaskWithClient(filename, gcsClient, up, &fakeAnnotator{})
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
