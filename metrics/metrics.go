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
	prometheus.MustRegister(WorkerCount)
	prometheus.MustRegister(WorkerState)
	prometheus.MustRegister(TaskCount)
	prometheus.MustRegister(TestCount)
	prometheus.MustRegister(BigQueryInsert)
	prometheus.MustRegister(DurationHistogram)
	prometheus.MustRegister(InsertionHistogram)
	prometheus.MustRegister(FileSizeHistogram)
}

// TODO
// Want a goroutine that monitors the workers, and updates metrics to indicate how long the
// workers have been working, and perhaps what their state is.
// How about a gauge, broken down by state?  The state transitions will be triggered by the
// worker code.
//

var (
	// Counts the number of tasks processed by the pipeline.
	//
	// Provides metrics:
	//   etl_worker_count
	// Example usage:
	//   metrics.WorkerCount.Inc() / .Dec()
	WorkerCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "etl_worker_count",
		Help: "Number of active workers.",
	})

	// Counts the number of tasks processed by the pipeline.
	//
	// Provides metrics:
	//   etl_worker_count
	// Example usage:
	//   metrics.WorkerState.WithLabelValues("flush").Inc() / .Dec()
	WorkerState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "etl_worker_state",
		Help: "Number of workers in different states.",
	},
		// Worker state, e.g. create task, read, parse, insert
		[]string{"state"},
	)

	// Counts the number of tasks processed by the pipeline.
	//
	// Provides metrics:
	//   etl_task_count{module, status}
	// Example usage:
	//   metrics.TaskCount.WithLabelValues("Task", "ok").Inc()
	TaskCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_task_count",
			Help: "Number of tasks/archive files processed.",
		},
		// Module and Status
		[]string{"module", "status"},
	)

	// Counts the number of tests processed by the parsers..
	//
	// Provides metrics:
	//   etl_test_count{type}
	// Example usage:
	//   metrics.TaskCount.WithLabelValues("s2c").Inc()
	TestCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_test_count",
			Help: "Number of tests processed.",
		},
		// Test type, e.g. s2c, c2s, reject, traceroute, sidestream
		[]string{"table", "type"},
	)

	// Counts the number of into BigQuery insert operations.
	//
	// Provides metrics:
	//   etl_worker_bigquery_insert_total{worker, status}
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

	// A histogram of bigquery insertion times. The buckets should use
	// periods that are intuitive for people.
	//
	// Provides metrics:
	//   etl_insertion_time_seconds_bucket{type="...", le="..."}
	//   ...
	//   etl_insertion_time_seconds_sum{type="..."}
	//   etl_insertion_time_seconds_count{type="..."}
	// Usage example:
	//   t := time.Now()
	//   // do some stuff.
	//   metrics.InsertionHistogram.WithLabelValues(
	//           "ndt_test", "ok").Observe(time.Since(t).Seconds())
	InsertionHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "etl_insertion_time_seconds",
			Help: "Insertion time distributions.",
			Buckets: []float64{
				0.001, 0.003, 0.01, 0.03, 0.1, 0.2, 0.5, 1.0, 2.0,
				5.0, 10.0, 20.0, 50.0, 100.0, math.Inf(+1),
			},
		},
		// Worker type, e.g. ndt, sidestream, ptr, etc.
		[]string{"table", "status"},
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
	//   metrics.DurationHistogram.WithLabelValues(
	//           "ndt").Observe(time.Since(t).Seconds())
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

	// TODO(dev): generalize this metric for size of any file type.
	FileSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "etl_web100_snaplog_file_size_bytes",
			Help: "Size of individual snaplog files.",
			Buckets: []float64{
				0,
				400000,     // 400k
				500000,     // 500k
				600000,     // 600k
				700000,     // 700k
				800000,     // 800k
				900000,     // 900k
				1000000,    // 1 mb
				1100000,    // 1.1 mb
				1200000,    // 1.2 mb
				1400000,    // 1.4 mb
				1600000,    // 1.6 mb
				1800000,    // 1.8 mb
				2000000,    // 2.0 mb
				2400000,    // 2.4 mb
				2800000,    // 2.8 mb
				3200000,    // 3.2 mb
				3600000,    // 3.6 mb
				4000000,    // 4 mb
				6000000,    // 6 mb
				8000000,    // 8 mb
				10000000,   // 10 mb
				20000000,   // 20
				40000000,   // 40
				80000000,   // 80
				100000000,  // 100 mb
				200000000,  // 200
				400000000,  // 400
				800000000,  // 800
				1000000000, // 1 gb
				math.Inf(+1),
			},
		},
		[]string{"range"},
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
