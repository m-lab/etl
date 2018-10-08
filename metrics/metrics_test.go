package metrics_test

import (
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"testing"

	"github.com/m-lab/etl/metrics"
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
	// This is never reached.
	return
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
