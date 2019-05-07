// Package metrics defines prometheus metric types and provides convenience
// methods to add accounting to various parts of the pipeline.
//
// When defining new operations or metrics, these are helpful values to track:
//  - things coming into or go out of the system: requests, files, tests, api calls.
//  - the success or error status of any of the above.
//  - the distribution of processing latency.
package metrics

import (
	"fmt"
	"log"
	"math"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// TODO
// Want a goroutine that monitors the workers, and updates metrics to indicate how long the
// workers have been working, and perhaps what their state is.
// How about a gauge, broken down by state?  The state transitions will be triggered by the
// worker code.
//

var (
	// AnnotationTimeSummary measures the latencies of requests to the Annotation Service as measured by the pipeline
	// Provides metrics:
	//    etl_annotator_Annotation_Time_Summary
	// Example usage:
	//    metrics.AnnotationTimeSummary.observe(float64)
	AnnotationTimeSummary = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Name: "etl_annotator_Annotation_Time_Summary",
		Help: "The total time to annotate, in nanoseconds.",
	}, []string{"test_type"})

	// AnnotationRequestCount measures the number of annotation requests
	// Provides metrics:
	//    etl_annotator_Request_Count
	// Example usage:
	//    metrics.AnnotationRequestCount.Inc()
	AnnotationRequestCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "etl_annotator_Request_Count",
		Help: "The current number of annotation requests",
	})

	// AnnotationErrorCount measures the number of annotation errors
	// Provides metrics:
	//    etl_annotator_Error_Count
	// Example usage:
	//    metrics.AnnotationErrorCount.Inc()
	AnnotationErrorCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_annotator_Error_Count",
			Help: "The current number of errors encountered while attempting to add annotation data.",
		}, []string{"source"})

	// AnnotationWarningCount measures the number of annotation warnings
	// Provides metrics:
	//    etl_annotator_Warning_Count
	// Example usage:
	//    metrics.AnnotationWarningCount.Inc()
	AnnotationWarningCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_annotator_Warning_Count",
			Help: "The current number of Warnings encountered while attempting to add annotation data.",
		}, []string{"source"})

	// AnnotationMissingCount measures the number of IPs with missing annotation.
	// The type could be "asn", "geo", or "both".
	// Provides metrics:
	//    etl_annotator_Missing_Count
	// Example usage:
	//    metrics.AnnotationMarningCount.Inc()
	AnnotationMissingCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_annotator_Missing_Count",
			Help: "The current number of IPs with missing annotation data.",
		}, []string{"type"})

	// PanicCount counts the number of panics encountered in the pipeline.
	//
	// Provides metrics:
	//   etl_panic_count{source}
	// Example usage:
	//   metrics.PanicCount.WithLabelValues("worker").Inc()
	PanicCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_panic_count",
			Help: "Number of panics encountered.",
		},
		// Tag indicating where the panic was recovered.
		[]string{"source"},
	)

	// WorkerCount counts the number of workers currently active.
	//
	// Provides metrics:
	//   etl_worker_count
	// Example usage:
	//   metrics.WorkerCount.WithLabelValues("ndt").Inc()
	WorkerCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "etl_worker_count",
			Help: "Number of active workers.",
		},
		// Output bigquery base table name, e.g. "ndt".
		[]string{"table"})

	// WorkerState counts the number of workers in each worker state..
	//
	// Provides metrics:
	//   etl_worker_count{table="ndt", state="insert"}
	// Example usage:
	//   metrics.WorkerState.WithLabelValues("ndt", "flush").Inc() / .Dec()
	WorkerState = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "etl_worker_state",
			Help: "Number of workers in different states.",
		},
		// Output bigquery base table name, e.g. "ndt".
		// Worker state, e.g. create task, read, parse, insert
		[]string{"table", "state"},
	)

	// FileCount counts the number of files processed by machine, rsync module, and day.
	//
	// Provides metrics:
	//   etl_files_processed{rsync_host_module, day_of_week}
	// Example usage:
	//   metrics.FileCount.WithLabelValues("mlab1-atl01-ndt", "Sunday").Inc()
	FileCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_files_processed",
			Help: "Number of files processed.",
		},
		[]string{"rsync_host_module", "day_of_week"},
	)

	// TaskCount counts the number of tasks processed by the pipeline.
	//
	// Provides metrics:
	//   etl_task_count{table, package, status}
	// Example usage:
	//   metrics.TaskCount.WithLabelValues("ndt", "Task", "ok").Inc()
	TaskCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_task_count",
			Help: "Number of tasks/archive files processed.",
		},
		// Go package or filename, and Status
		[]string{"table", "package", "status"},
	)

	// TestCount counts the number of tests successfully processed by the parsers.
	//
	// Provides metrics:
	//   etl_test_count{table, filetype, status}
	// Example usage:
	// metrics.TestCount.WithLabelValues(
	//	tt.Inserter.TableBase(), "s2c", "ok").Inc()
	TestCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_test_count",
			Help: "Number of tests processed.",
		},
		// ndt/pt/ss, s2c/c2s/meta, ok/reject/error/
		[]string{"table", "filetype", "status"},
	)

	// PTHopCount counts the number of hops in PT tests successfully processed by the parsers.
	//
	// Provides metrics:
	//   etl_pthop_count{table, filetype, status}
	// Example usage:
	// metrics.PTHopCount.WithLabelValues(
	//	tt.Inserter.TableBase(), "hop", "ok").Inc()
	PTHopCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_pthop_count",
			Help: "Number of hops for PT tests processed.",
		},
		// pt, pt, ok/reject/error/
		[]string{"table", "filetype", "status"},
	)

	// PTTestCount counts the PT tests per metro.
	//
	// Provides metrics:
	//   etl_pt_test_count_per_metro{metro}
	// Example usage:
	//   metrics.PTTestCountPerSite.WithLabelValues("sea").Inc()
	PTTestCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_pt_test_count_per_metro",
			Help: "Count how many PT tests per metro.",
		},
		// sea
		[]string{"metro"},
	)

	// PTNotReachDestCount counts the PT tests that did not reach the expected destination IP
	// at the last hop per metro.
	//
	// Provides metrics:
	//   etl_pt_not_reach_dest_count{metro}
	// Example usage:
	//   metrics.PTNotReachDestCount.WithLabelValues("sea").Inc()
	PTNotReachDestCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_pt_not_reach_dest_count",
			Help: "Count how many PT tests did not reach expected destination at the last hop per metro.",
		},
		// sea
		[]string{"metro"},
	)

	// PTMoreHopsAfterDest counts the PT tests that reach the expected destination IP
	// in the middle of a test per metro, but do more hops afterwards instead of ending there.
	//
	// Provides metrics:
	//   etl_pt_more_hops_after_dest_count{metro}
	// Example usage:
	//   metrics.PTMoreHopsAfterDest.WithLabelValues("sea").Inc()
	PTMoreHopsAfterDest = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_pt_more_hops_after_dest_count",
			Help: "Count how many PT tests reach expected destination in middle but do more hops afterwards instead of ending there.",
		},
		// sea
		[]string{"metro"},
	)

	// PTBitsAwayFromDestV4 provides a histogram of number of bits difference between
	// last hop and expected destination IP for the PT tests that did not reach the expected destination IP.
	// This metric is only for IPv4.
	//
	// Provides metrics:
	//   etl_pt_bits_away_from_dest_v4{metro}
	// Usage example:
	//   metrics.PTBitsAwayFromDestV4.WithLabelValues("sea").Observe(bitsdiff)
	PTBitsAwayFromDestV4 = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "etl_pt_bits_away_from_dest_v4",
			Help: "Bits diff distribution between last hop and expected destination IP for IPv4.",
			Buckets: []float64{
				0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
			},
		},
		[]string{"metro"},
	)

	// PTBitsAwayFromDestV6 provides a histogram of number of bits difference between last hop and expected destination IP
	// for the PT tests that did not reach the expected destination IP.
	// This metric is only for IPv6.
	//
	// Provides metrics:
	//   etl_pt_bits_away_from_dest_v6{metro}
	// Usage example:
	//   metrics.PTNotReachBitsDiffV6.WithLabelValues("sea").Observe(bitsdiff)
	PTBitsAwayFromDestV6 = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "etl_pt_bits_away_from_dest_v6",
			Help: "Bits diff distribution between last hop and expected destination IP for IPv6.",
			Buckets: []float64{
				0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
			},
		},
		[]string{"metro"},
	)

	// PTPollutedCount counts the PT polluted tests per metro.
	//
	// Provides metrics:
	//   etl_pt_polluted_total{metro}
	// Example usage:
	//   metrics.PTPollutedCount.WithLabelValues("sea").Inc()
	PTPollutedCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_pt_polluted_total",
			Help: "Count how many PT tests polluted per metro.",
		},
		// sea
		[]string{"metro"},
	)

	// WarningCount counts the all warnings that do NOT result in test loss.
	//
	// Provides metrics:
	//   etl_warning_count{table, filetype, kind}
	// Example usage:
	//   metrics.WarningCount.WithLabelValues(TableName(), "s2c", "small test").Inc()
	WarningCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_warning_count",
			Help: "Warnings that do not result in test loss.",
		},
		// Parser type, error description.
		[]string{"table", "filetype", "kind"},
	)

	// ErrorCount counts the all errors that result in test loss.
	//
	// Provides metrics:
	//   etl_error_count{table, filetype, kind}
	// Example usage:
	//   metrics.ErrorCount.WithLabelValues(TableName(), s2c, "insert").Inc()
	ErrorCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_error_count",
			Help: "Errors that cause test loss.",
		},
		// Parser type, error description.
		[]string{"table", "filetype", "kind"},
	)

	// BackendFailureCount counts the all bulk backend failures.  This does not count, e.g.
	// single row errors.
	//
	// Provides metrics:
	//   etl_backend_failure_count{table, kind}
	// Example usage:
	//   metrics.BackendFailureCount.WithLabelValues(TableName(), "insert").Inc()
	BackendFailureCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_backend_failure_count",
			Help: "Backend failures, whether or not recoverable.",
		},
		// Parser type, error description.
		[]string{"table", "kind"},
	)

	// GCSRetryCount counts the number of retries on GCS read operations.
	//
	// Provides metrics:
	//   etl_gcs_retry_count{table, phase, retries, status}
	// Example usage:
	// metrics.GCSRetryCount.WithLabelValues(
	//	TableName(), "open", retries, "ok").Inc()
	GCSRetryCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_gcs_retry_count",
			Help: "Number of retries on GCS reads.",
		},
		// ndt/traceroute, open/read/zip, num_retries, ok/error/
		[]string{"table", "phase", "retries", "status"},
	)

	// TODO(dev): bytes/row - generalize this metric for any file type.
	//
	// RowSizeHistogram provides a histogram of bq row json sizes.  It is intended primarily for
	// NDT, so the bins are fairly large.  NDT average json is around 200K
	//
	// Provides metrics:
	//   etl_row_json_size_bucket{table="...", le="..."}
	//   ...
	//   etl_row_json_size_sum{table="...", le="..."}
	//   etl_row_json_size_count{table="...", le="..."}
	// Usage example:
	//   metrics.RowSizeHistogram.WithLabelValues(
	//           "ndt").Observe(len(json))
	RowSizeHistogram = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "etl_row_json_size",
			Help: "Row json size distributions.",
			Buckets: []float64{
				100, 200, 400, 800, 1600, 3200, 6400, 10000, 20000,
				40000, 80000, 160000, 320000, 500000, 600000, 700000,
				800000, 900000, 1000000, 1200000, 1500000, 2000000, 5000000,
			},
		},
		[]string{"table"},
	)

	// TODO(dev): fields/row - generalize this metric for any file type.
	//
	// DeltaNumFieldsHistogram provides a histogram of snapshot delta field counts.  It is intended primarily for
	// NDT.  Typical is about 13, but max might be up to 120 or so.
	//
	// Provides metrics:
	//   etl_delta_num_field_bucket{table="...", le="..."}
	//   ...
	//   etl_delta_num_field_sum{table="...", le="..."}
	//   etl_delta_num_field_count{table="...", le="..."}
	// Usage example:
	//   metrics.DeltaNumFieldsHistogram.WithLabelValues(
	//           "ndt").Observe(fieldCount)
	DeltaNumFieldsHistogram = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "etl_delta_num_field",
			Help: "Number of fields in delta distribution.",
			Buckets: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
				14, 16, 18, 20, 22, 24, 28, 32, 36, 40, 50, 60, 70, 80, 90,
				100, 110, 120, 150,
			},
		},
		[]string{"table"},
	)

	// TODO(dev): rows/test - generalize this metric for any file type.
	//
	// EntryFieldCountHistogram provides a histogram of (approximate) row field counts.  It is intended primarily for
	// NDT, so the bins are fairly large.  NDT snapshots typically total about 10k
	// fields, 99th percentile around 35k fields, and occasionally as many as 50k.
	// Smaller field count bins included so that it is possibly useful for other
	// parsers.
	//
	// Provides metrics:
	//   etl_entry_field_count_bucket{table="...", le="..."}
	//   ...
	//   etl_entry_field_count_sum{table="...", le="..."}
	//   etl_entry_field_count_count{table="...", le="..."}
	// Usage example:
	//   metrics.EntryFieldCountHistogram.WithLabelValues(
	//           "ndt").Observe(fieldCount)
	EntryFieldCountHistogram = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "etl_entry_field_count",
			Help: "total snapshot field count distributions.",
			Buckets: []float64{
				1, 2, 3, 4, 6, 8,
				10, 12, 15, 20, 24, 30, 40, 48, 60, 80,
				100, 120, 150, 200, 240, 300, 400, 480, 600, 800,
				1000, 1200, 1500, 2000, 2400, 3000, 4000, 4800, 6000, 8000,
				10000, 12000, 15000, 20000, 24000, 30000, 40000, 48000, 60000, 80000,
				100000, 120000, 150000, 200000, 240000, 300000, 400000, 480000,
			},
		},
		[]string{"table"},
	)

	// InsertionHistogram provides a histogram of bigquery insertion times. The buckets should use
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
	InsertionHistogram = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "etl_insertion_time_seconds",
			Help: "Insertion time distributions.",
			Buckets: []float64{
				0.001, 0.003, 0.01, 0.03, 0.1, 0.2, 0.5, 1.0, 2.0,
				5.0, 10.0, 20.0, 50.0, 100.0, 200.0, math.Inf(+1),
			},
		},
		// Worker type, e.g. ndt, sidestream, ptr, etc.
		[]string{"table", "status"},
	)

	// DurationHistogram provides a histogram of worker processing times. The buckets should use
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
	DurationHistogram = promauto.NewHistogramVec(
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
		[]string{"worker", "status"},
	)

	// FileSizeHistogram provides a histogram of source file sizes. The bucket
	// sizes should cover a wide range of input file sizes.
	//
	// Example usage:
	//   metrics.FileSizeHistogram.WithLabelValues(
	//       "ndt", "c2s_snaplog", "parsed").Observe(size)
	FileSizeHistogram = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "etl_test_file_size_bytes",
			Help: "Size of individual test files.",
			Buckets: []float64{
				0,
				1000,       // 1k
				5000,       // 5k
				10000,      // 10k
				25000,      // 25k
				50000,      // 50k
				75000,      // 75k
				100000,     // 100k
				200000,     // 200k
				300000,     // 300k
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
		[]string{"table", "kind", "group"},
	)
)

// catchStatus wraps the native http.ResponseWriter and captures any written HTTP
// status codes.
type catchStatus struct {
	http.ResponseWriter
	status int
}

// WriteHeader wraps the http.ResponseWriter.WriteHeader method, and preserves the
// status code.
func (cw *catchStatus) WriteHeader(code int) {
	cw.ResponseWriter.WriteHeader(code)
	cw.status = code
}

// DurationHandler wraps the call of an inner http.HandlerFunc and records the runtime.
func DurationHandler(name string, inner http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		t := time.Now()
		cw := &catchStatus{w, http.StatusOK} // Default status is OK.
		inner.ServeHTTP(cw, r)
		// TODO(soltesz): change 'name' to 'table' label based on request parameter.
		DurationHistogram.WithLabelValues(name, http.StatusText(cw.status)).Observe(
			time.Since(t).Seconds())
	}
}

// CountPanics updates the PanicCount metric, then repanics.
// It must be wrapped in a defer.
// Examples:
//  For function that returns an error:
//    func foobar() () {
//        defer func() {
//		      etl.AddPanicMetric(recover(), "foobar")
// 	      }()
//        ...
//        ...
//    }
// TODO - possibly move this to metrics.go
func CountPanics(r interface{}, tag string) {
	if r != nil {
		err, ok := r.(error)
		if !ok {
			log.Println("bad recovery conversion")
			err = fmt.Errorf("pkg: %v", r)
		}
		log.Println("Adding metrics for panic:", err)
		PanicCount.WithLabelValues(tag).Inc()
		debug.PrintStack()
		panic(r)
	}
}

// PanicToErr captures panics and converts them to
// errors.  Use with extreme care, as panic may mean that
// state is corrupted, and continuing to execut may result
// in undefined behavior.
// It must be wrapped in a defer.
// Example:
//    // err must be a named return value to be captured.
//    func foobar() (err error) {
//        defer func() {
//			  // Possibly do something with existing error
//            // before calling PanicToErr
//		      err = etl.PanicToErr(err, recover(), "foobar")
// 	      }()
//        ...
//        ...
//    }
func PanicToErr(err error, r interface{}, tag string) error {
	if r != nil {
		var ok bool
		err, ok = r.(error)
		// TODO - Check if err == runtime.Error, and treat
		// differently ?
		if !ok {
			log.Println("bad recovery conversion")
			err = fmt.Errorf("pkg: %v", r)
		}
		log.Println("Recovered from panic:", err)
		PanicCount.WithLabelValues(tag).Inc()
		debug.PrintStack()
	}
	return err
}
