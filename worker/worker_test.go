package worker_test

import (
	"archive/tar"
	"context"
	"errors"
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
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/m-lab/annotation-service/api"
	v2 "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/go/cloud/bqx"
	"github.com/m-lab/go/rtx"

	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/factory"
	"github.com/m-lab/etl/fake"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
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
	gcsClient := fromTar("test-bucket", "../testfiles/ndt.tar").Client()
	tree(t, gcsClient)
	t.Fatal()
}

func TestProcessTask(t *testing.T) {
	if testing.Short() {
		t.Log("Skipping integration test")
	}

	gcsClient := fromTar("test-bucket", "../testfiles/ndt.tar").Client()
	filename := "gs://test-bucket/ndt/2018/05/09/20180509T101913Z-mlab1-mad03-ndt-0000.tgz"

	status, err := worker.ProcessTaskWithClient(gcsClient, filename)
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

// This is also the annotator, so it just returns itself.
type fakeAnnotatorFactory struct{}

func (ann *fakeAnnotatorFactory) GetAnnotations(ctx context.Context, date time.Time, ips []string, info ...string) (*v2.Response, error) {
	return &v2.Response{AnnotatorDate: time.Now(), Annotations: make(map[string]*api.Annotations, 0)}, nil
}

func (ann *fakeAnnotatorFactory) Get(ctx context.Context, dp etl.DataPath) (v2.Annotator, etl.ProcessingError) {
	return ann, nil
}

type fakeSinkFactory struct {
	up etl.Uploader
}

func (fsf *fakeSinkFactory) Get(ctx context.Context, dp etl.DataPath) (row.Sink, etl.ProcessingError) {
	if fsf.up == nil {
		return nil, factory.NewError(dp.DataType, "fakeSinkFactory",
			http.StatusInternalServerError, errors.New("nil uploader"))
	}
	pdt := bqx.PDT{Project: "fake-project", Dataset: "fake-dataset", Table: "fake-table"}
	in, err := bq.NewColumnPartitionedInserterWithUploader(pdt, fsf.up)
	rtx.Must(err, "Bad SinkFactory")
	return in, nil
}

type fakeSourceFactory struct {
	client *storage.Client
}

func (sf *fakeSourceFactory) Get(ctx context.Context, dp etl.DataPath) (etl.TestSource, etl.ProcessingError) {
	// TODO simplify GetSource
	tr, _, _, err := worker.GetSource(sf.client, dp.URI)
	rtx.Must(err, "Bad TestSource")

	// TODO
	// defer tr.Close()

	return tr, nil
}

func NewSourceFactory() factory.SourceFactory {
	gcsClient := fromTar("test-bucket", "../testfiles/ndt.tar").Client()
	return &fakeSourceFactory{client: gcsClient}
}

func TestNilUploader(t *testing.T) {
	if testing.Short() {
		t.Log("Skipping integration test")
	}

	fakeFactory := worker.StandardTaskFactory{
		Annotator: &fakeAnnotatorFactory{},
		Sink:      &fakeSinkFactory{up: nil},
		Source:    NewSourceFactory(),
	}

	filename := "gs://test-bucket/ndt/ndt5/2019/12/01/20191201T020011.395772Z-ndt5-mlab1-bcn01-ndt.tgz"
	path, err := etl.ValidateTestPath(filename)
	if err != nil {
		t.Fatal(err, filename)
	}
	// TODO create a TaskFactory and use ProcessGKETask
	pErr := worker.ProcessGKETask(path, &fakeFactory)
	if pErr == nil || pErr.Code() != http.StatusInternalServerError {
		t.Fatal("Expected error with", http.StatusInternalServerError, "Got:", pErr)
	}

	metrics.FileCount.Reset()
	metrics.TaskCount.Reset()
	metrics.TestCount.Reset()
}

func TestProcessGKETask(t *testing.T) {
	if testing.Short() {
		t.Log("Skipping integration test")
	}

	up := fake.NewFakeUploader()
	fakeFactory := worker.StandardTaskFactory{
		Annotator: &fakeAnnotatorFactory{},
		Sink:      &fakeSinkFactory{up: up},
		Source:    NewSourceFactory(),
	}

	filename := "gs://test-bucket/ndt/ndt5/2019/12/01/20191201T020011.395772Z-ndt5-mlab1-bcn01-ndt.tgz"
	path, err := etl.ValidateTestPath(filename)
	if err != nil {
		t.Fatal(err, filename)
	}
	// TODO create a TaskFactory and use ProcessGKETask
	pErr := worker.ProcessGKETask(path, &fakeFactory)
	if pErr != nil {
		t.Fatal("Expected", http.StatusOK, "Got:", pErr)
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
