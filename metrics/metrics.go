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
	prometheus.MustRegister(ErrorCount)
	prometheus.MustRegister(WarningCount)
	prometheus.MustRegister(BackendFailureCount)
	prometheus.MustRegister(GCSRetryCount)
	prometheus.MustRegister(BigQueryInsert)
	prometheus.MustRegister(RowSizeHistogram)
	prometheus.MustRegister(DeltaNumFieldsHistogram)
	prometheus.MustRegister(EntryFieldCountHistogram)
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
	//   etl_worker_count{state}
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
	//   etl_task_count{package, status}
	// Example usage:
	//   metrics.TaskCount.WithLabelValues("Task", "ok").Inc()
	TaskCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_task_count",
			Help: "Number of tasks/archive files processed.",
		},
		// Go package or filename, and Status
		[]string{"package", "status"},
	)

	// Counts the number of tests successfully processed by the parsers.
	//
	// Provides metrics:
	//   etl_test_count{table, filetype, status}
	// Example usage:
	// metrics.TestCount.WithLabelValues(
	//	tt.Inserter.TableBase(), "s2c", "ok").Inc()
	TestCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_test_count",
			Help: "Number of tests processed.",
		},
		// ndt/pt/ss, s2c/c2s/meta, ok/reject/error/
		[]string{"table", "filetype", "status"},
	)

	// Counts the all warnings that do NOT result in test loss.
	//
	// Provides metrics:
	//   etl_warning_count{table, filetype, kind}
	// Example usage:
	//   metrics.WarningCount.WithLabelValues(TableName(), "s2c", "small test").Inc()
	WarningCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_warning_count",
			Help: "Warnings that do not result in test loss.",
		},
		// Parser type, error description.
		[]string{"table", "filetype", "kind"},
	)

	// Counts the all errors that result in test loss.
	//
	// Provides metrics:
	//   etl_error_count{table, filetype, kind}
	// Example usage:
	//   metrics.ErrorCount.WithLabelValues(TableName(), s2c, "insert").Inc()
	ErrorCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_error_count",
			Help: "Errors that cause test loss.",
		},
		// Parser type, error description.
		[]string{"table", "filetype", "kind"},
	)

	// Counts the all bulk backend failures.  This does not count, e.g.
	// single row errors.
	//
	// Provides metrics:
	//   etl_backend_failure_count{table, kind}
	// Example usage:
	//   metrics.BackendFailureCount.WithLabelValues(TableName(), "insert").Inc()
	BackendFailureCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_backend_failure_count",
			Help: "Backend failures, whether or not recoverable.",
		},
		// Parser type, error description.
		[]string{"table", "kind"},
	)

	// Counts the number of retries on GCS read operations.
	//
	// Provides metrics:
	//   etl_gcs_retry_count{type}
	// Example usage:
	// metrics.GCSRetryCount.WithLabelValues(
	//	TableName(), retries, "ok").Inc()
	GCSRetryCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_gcs_retry_count",
			Help: "Number of retries on GCS reads.",
		},
		// open/read/zip, num_retries, ok/error/
		[]string{"phase", "retries", "status"},
	)

	// Counts the number of into BigQuery insert operations.
	//
	// Provides metrics:
	//   etl_worker_bigquery_insert_total{table, status}
	// Usage example:
	//   metrics.BigQueryInsert.WithLabelValues("ndt", "200").Inc()
	BigQueryInsert = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_worker_bigquery_insert_total",
			Help: "Number of BigQuery insert operations.",
		},
		// Worker type, e.g. ndt, sidestream, ptr, etc.
		[]string{"table", "status"},
	)

	RowSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "etl_row_json_size",
			Help: "Row json size distributions.",
			Buckets: []float64{
				100, 200, 400, 800, 1600, 3200, 6400, 10000, 20000,
				40000, 80000, 160000, 320000, 500000, 600000, 700000,
				800000, 900000, 1000000, 1200000, 5000000, 10000000, 20000000,
			},
		},
		[]string{"table"},
	)

	DeltaNumFieldsHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "etl_delta_num_field",
			Help: "Number of fields in delta distribution.",
			Buckets: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
				14, 16, 18, 20, 22, 24, 28, 32, 36, 40, 50, 60,
			},
		},
		[]string{"table"},
	)

	EntryFieldCountHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "etl_entry_field_count",
			Help: "total snapshot field count distributions.",
			Buckets: []float64{10, 12, 14, 16, 20, 24,
				28, 32, 40, 48, 64, 72, 80, 88, 96, 102, 110, 118, 126,
				132, 150, 200, 250, 300, 350, 400, 500, 600, 700, 800, 900, 1000,
				1250, 1500, 1750, 2000, 2500, 3000, 3500, 4000, 5000, 6000, 8000,
				10000, 15000, 20000, 30000, 40000, 50000, 70000,
				100000, 200000, 300000, 400000, 500000,
			},
		},
		[]string{"table"},
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
