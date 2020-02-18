package worker_test

import (
	"archive/tar"
	"log"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/m-lab/etl/storage"
	"github.com/m-lab/go/rtx"

	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/fake"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/worker"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

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

func counterVecValue(m *prometheus.CounterVec) float64 {
	c := make(chan prometheus.Metric, 1) // buffer to allow Collect to complete before reading.
	// This may block if the metric hasn't been measured yet?
	m.Collect(c)
	return counterValue(<-c)
}

func checkCounter(t *testing.T, c chan prometheus.Metric, expected float64) {
	m := <-c
	v := counterValue(m)
	if v != expected {
		t.Error("For", m.Desc(), "expected:", expected, "got:", v)
	}
}

func TestProcessTask(t *testing.T) {
	if testing.Short() {
		t.Log("Skipping integration test")
	}
	/*
		files := counterVecValue(metrics.FileCount)
		tasks := counterVecValue(metrics.TaskCount)
		tests := counterVecValue(metrics.TestCount)
	*/
	metrics.FileCount.Reset()
	metrics.TaskCount.Reset()
	metrics.TestCount.Reset()

	filename := "gs://archive-mlab-testing/ndt/2018/05/09/20180509T101913Z-mlab1-mad03-ndt-0000.tgz"
	status, err := worker.ProcessTask(filename)
	if err != nil {
		t.Error(err)
	}
	if status != http.StatusOK {
		t.Error("Expected", http.StatusOK, "Got:", status)
	}

	// This section checks that prom metrics are updated appropriately.
	// Unfortunately, these are cumulative over all tests!!
	c := make(chan prometheus.Metric, 10)

	metrics.FileCount.Collect(c)
	checkCounter(t, c, 1)

	metrics.TaskCount.Collect(c)
	checkCounter(t, c, 1)

	metrics.TestCount.Collect(c)
	checkCounter(t, c, 1)
}

// Item represents a row item.
type Item struct {
	Name   string
	Count  int
	Foobar int `json:"foobar"`
}

func standardInsertParams(bufferSize int) etl.InserterParams {
	return etl.InserterParams{
		Project: "mlab-testing", Dataset: "dataset", Table: "table",
		Suffix:        "",
		BufferSize:    bufferSize,
		PutTimeout:    10 * time.Second,
		MaxRetryDelay: 1 * time.Second,
	}
}

func fakeETLSource(rdr *tar.Reader) (*storage.ETLSource, error) {

	baseTimeout := 16 * time.Millisecond
	return &storage.ETLSource{rdr, nil, baseTimeout, "invalid"}, nil
}

func TestProcessSource(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping tests that access GCS")
	}
	metrics.FileCount.Reset()
	metrics.TaskCount.Reset()
	metrics.TestCount.Reset()

	fn := "gs://archive-mlab-testing/ndt/2018/05/01/20180501T142423Z-mlab1-iad05-ndt-0000.tgz"
	path, err := etl.ValidateTestPath(fn)
	rtx.Must(err, "path")

	// Move this into Validate function
	dt := path.GetDataType()
	if dt == etl.INVALID {
		t.Fatal("Invalid datatype")
	}

	client, err := storage.GetStorageClient(false)
	rtx.Must(err, "client")

	src, err := storage.NewETLSource(client, fn)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	// Set up an Inserter with a fake Uploader backend for testing.
	// Buffer 3 rows, so that we can test the buffering.
	uploader := fake.NewFakeUploader()
	ins, err := bq.NewBQInserter(standardInsertParams(3), uploader)
	if err != nil {
		t.Fatalf("%v\n", err)
	}

	got, err := worker.ProcessSource(fn, *path, dt, src, ins)
	if err != nil {
		t.Fatalf("process() error = %v", err)
	}
	if got != http.StatusOK {
		t.Error(http.StatusText(got))
	}

	// This section checks that prom metrics are updated appropriately.
	c := make(chan prometheus.Metric, 10)

	metrics.FileCount.Collect(c)
	checkCounter(t, c, 4)

	metrics.TaskCount.Collect(c)
	checkCounter(t, c, 1)

	metrics.TestCount.Collect(c)
	checkCounter(t, c, 1)

	if len(uploader.Rows) != 1 {
		t.Error(len(uploader.Rows))
	}
}
