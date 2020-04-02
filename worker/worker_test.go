package worker_test

import (
	"context"
	"fmt"
	"log"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/api"
	v2 "github.com/m-lab/annotation-service/api/v2"

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

func checkCounter(t *testing.T, c chan prometheus.Metric, expected float64) {
	m := <-c
	v := counterValue(m)
	if v != expected {
		log.Output(2, fmt.Sprintln("For", m.Desc(), "expected:", expected, "got:", v))
		t.Error("For", m.Desc(), "expected:", expected, "got:", v)
	}
}

func TestProcessTask(t *testing.T) {
	if testing.Short() {
		t.Log("Skipping integration test")
	}
	filename := "gs://archive-mlab-testing/ndt/2018/05/09/20180509T101913Z-mlab1-mad03-ndt-0000.tgz"
	status, err := worker.ProcessTask(filename)
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
	// TODO load this from local file?
	filename := "gs://archive-mlab-testing/ndt/ndt5/2019/12/01/20191201T020011.395772Z-ndt5-mlab1-bcn01-ndt.tgz"
	up := fake.NewFakeUploader()
	status, err := worker.ProcessGKETask(filename, up, &fakeAnnotator{})
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
