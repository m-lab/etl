// The metrics package defines prometheus metric types and provides
// convenience methods to add accounting to various parts of the pipeline.
//
// When defining new operations or metrics, these are helpful values to track:
//  - things coming into or go out of the system: requests, files, tests, api calls.
//  - the success or error status of any of the above.
//  - the distribution of processing latency.
package metrics

import (
	"math"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	// Register the metrics defined with Prometheus's default registry.
	prometheus.MustRegister(DurationHistogram)
	prometheus.MustRegister(TestInput)
	prometheus.MustRegister(BigQueryInsert)
}

var (
	// Counts the number of tests read into the pipeline.
	//
	// Provides metrics:
	//   etl_worker_test_input_total{status="..."}
	// Example usage:
	//   metrics.TestInput.WithLabelValues("ndt", "ok").Inc()
	TestInput = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_worker_test_input_total",
			Help: "Number of test files read from archive files add sent to parsers.",
		},
		// Worker type, e.g. ndt, sidestream, ptr, etc.
		[]string{"worker", "status"},
	)

	// Counts the number of into BigQuery insert operations.
	//
	// Provides metrics:
	//   etl_worker_bigquery_insert_total{worker="..."}
	// Usage example:
	//   metrics.BigQueryInsert.WithLabelValues("ndt", "200").Inc()
	BigQueryInsert = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_worker_bigquery_insert_total",
			Help: "Number of BigQuery insert operations.",
		},
		// Worker type, e.g. ndt, sidestream, ptr, etc.
		[]string{"worker", "status"},
	)

	// A histogram of worker processing times. The buckets should use
	// periods that are intuitive for people.
	//
	// Provides metrics:
	//   etl_worker_duration_seconds_bucket{worker="...", le="..."}
	//   ...
	//   etl_worker_duration_seconds_sum{worker="..."}
	//   etl_worker_duration_seconds_count{worker="..."}
	// Usage example:
	//   t := time.Now()
	//   // do some stuff.
	//   metrics.DurationHistogram.WithLabelValues(name).Observe(time.Since(t).Seconds())
	DurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "etl_worker_duration_seconds",
			Help: "Worker execution time distributions.",
			Buckets: []float64{
				0.001, 0.01, 0.1, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
				600.0, 1800.0, 3600.0, 7200.0, math.Inf(+1),
			},
		},
		// Worker type, e.g. ndt, sidestream, ptr, etc.
		// TODO(soltesz): support a status field based on HTTP status.
		[]string{"worker"},
	)
)

// DurationHandler wraps the call of an inner http.HandlerFunc and records the runtime.
func DurationHandler(name string, inner http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		t := time.Now()
		inner.ServeHTTP(w, r)
		// TODO(soltesz): collect success or failure status.
		DurationHistogram.WithLabelValues(name).Observe(time.Since(t).Seconds())
	}
}
