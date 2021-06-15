package metrics_test

import (
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"testing"

	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/go/prometheusx/promtest"
)

func panicAndRecover() (err error) {
	defer func() {
		err = metrics.PanicToErr(nil, recover(), "foobar")
	}()
	a := []int{1, 2, 3}
	log.Println(a[4])
	// This is never reached.
	return
}

func errorWithoutPanic(prior error) (err error) {
	err = prior
	defer func() {
		err = metrics.PanicToErr(err, recover(), "foobar")
	}()
	return
}

func TestHandlePanic(t *testing.T) {
	err := panicAndRecover()
	log.Println("Actually did recover")
	if err == nil {
		t.Fatal("Should have errored")
	}
}

func TestNoPanic(t *testing.T) {
	err := errorWithoutPanic(nil)
	if err != nil {
		t.Error(err)
	}

	err = errorWithoutPanic(errors.New("prior"))
	if err.Error() != "prior" {
		t.Error("Should have returned prior error.")
	}
}

func rePanic() {
	defer func() {
		metrics.CountPanics(recover(), "foobar")
	}()
	a := []int{1, 2, 3}
	log.Println(a[4])
}

func TestCountPanics(t *testing.T) {
	// When we call RePanic, the panic should cause a log and a metric
	// increment, but should still panic.  This intercepts the panic,
	// and errors if the panic doesn't happen.
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
		fmt.Printf("%s\n", debug.Stack())
	}()

	rePanic()
}

func TestMetrics(t *testing.T) {
	// Currently just lints. There are too many lint errors for me to feel
	// comfortable fixing everything without further discussion.
	//
	// TODO: turn the lint warnings into errors and resolve all the errors.
	metrics.AnnotationErrorCount.WithLabelValues("x")
	metrics.AnnotationTimeSummary.WithLabelValues("x")
	metrics.AnnotationWarningCount.WithLabelValues("x")
	metrics.BackendFailureCount.WithLabelValues("x", "x")
	metrics.DeltaNumFieldsHistogram.WithLabelValues("x")
	metrics.DurationHistogram.WithLabelValues("x", "x")
	metrics.EntryFieldCountHistogram.WithLabelValues("x")
	metrics.ErrorCount.WithLabelValues("x", "x", "x")
	metrics.FileCount.WithLabelValues("x", "x")
	metrics.FileSizeHistogram.WithLabelValues("x", "x", "x")
	metrics.GCSRetryCount.WithLabelValues("x", "x", "x", "x")
	metrics.InsertionHistogram.WithLabelValues("x", "x")
	metrics.PanicCount.WithLabelValues("x")
	metrics.PTBitsAwayFromDestV4.WithLabelValues("x")
	metrics.PTBitsAwayFromDestV6.WithLabelValues("x")
	metrics.PTHopCount.WithLabelValues("x", "x", "x")
	metrics.PTMoreHopsAfterDest.WithLabelValues("x")
	metrics.PTNotReachDestCount.WithLabelValues("x")
	metrics.PTPollutedCount.WithLabelValues("x")
	metrics.PTTestCount.WithLabelValues("x")
	metrics.RowSizeHistogram.WithLabelValues("x")
	metrics.TaskCount.WithLabelValues("x", "x")
	metrics.TestCount.WithLabelValues("x", "x", "x")
	metrics.WarningCount.WithLabelValues("x", "x", "x")
	metrics.WorkerCount.WithLabelValues("x")
	metrics.WorkerState.WithLabelValues("x", "x")
	if !promtest.LintMetrics(nil) {
		t.Log("There are lint errors in the prometheus metrics.")
	}
}
