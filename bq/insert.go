// Package bq includes all code related to BigQuery.
//
// NB: NOTES ON MEMORY USE AND HTTP SIZE
// The bigquery library uses JSON encoding of data, which appears to be the only
// option at this time.  Furthermore, it uses intermediate data representations,
// eventually creating a map[string]Value (unless you pass that in to begin with).
// In general, when we start pumping large volumes of data, both the map and the
// JSON will cause some memory pressure, and likely pretty severe limits on the size
// of the insert we can send, likely on the order of a couple MB of actual row footprint
// in BQ.
//
// Passing in slice of structs makes memory pressure a bit worse, but probably isn't
// worth worrying about.

package bq

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
)

// insertsBeforeRowJSONCount controls how often we perform a wasted JSON marshal
// on a row before inserting it into BQ. The smaller the number the more wasted
// CPU time. -- 1% of inserts.
const insertsBeforeRowJSONCount = 100

// tableInsertCounts tracks the number of inserts to each table. Keys are table
// names and values are *uint32.
var tableInsertCounts = sync.Map{}

func init() {
}

const (
	// MaxPutRetryDelay is one minute.  So aggregate delay will be around 2 minutes.
	// Beyond this point, we likely have a serious problem so no point in continuing.
	maxPutRetryDelay = time.Minute
	// PutContextTimeout limits how long we allow a single BQ Put call to take.  These
	// typically complete in one or two seconds.
	putContextTimeout = 60 * time.Second
)

// NewInserter creates a new BQInserter with appropriate characteristics.
func NewInserter(dt etl.DataType, partition time.Time) (etl.Inserter, error) {
	suffix := ""
	if etl.IsBatchService() || time.Since(partition) > 30*24*time.Hour {
		// If batch, or too far in the past, we use a templated table, and must merge it later.
		suffix = "_" + partition.Format("20060102")
	} else {
		// Otherwise, we can stream directly to correct partition.
		suffix = "$" + partition.Format("20060102")
	}

	bqProject := dt.BigqueryProject()
	dataset := dt.Dataset()
	table := dt.Table()

	return NewBQInserter(
		etl.InserterParams{Project: bqProject, Dataset: dataset, Table: table, Suffix: suffix,
			PutTimeout: putContextTimeout, MaxRetryDelay: maxPutRetryDelay,
			BufferSize: dt.BQBufferSize()},
		nil)
}

// NewBQInserter initializes a new BQInserter
// Pass in nil uploader for normal use, custom uploader for custom behavior
// TODO - improve the naming between here and NewInserter.
func NewBQInserter(params etl.InserterParams, uploader etl.Uploader) (etl.Inserter, error) {
	if uploader == nil {
		client, err := GetClient(params.Project)
		if err != nil {
			return nil, err
		}
		table := params.Table
		if params.Suffix[0] == '$' {
			// Suffix starting with $ is just a partition spec.
			table += params.Suffix
		}
		u := client.Dataset(params.Dataset).Table(table).Uploader()
		if params.Suffix[0] == '_' {
			// Suffix starting with _ is a template suffix.
			u.TableTemplateSuffix = params.Suffix
		}
		// This avoids problems when a single row of the insert has invalid
		// data.  We then have to carefully parse the returned error object.
		u.SkipInvalidRows = true
		uploader = u
	}
	token := make(chan struct{}, 1)
	token <- struct{}{}
	rows := make([]interface{}, 0, params.BufferSize)
	in := BQInserter{params: params, uploader: uploader, putTimeout: params.PutTimeout, rows: rows, token: token}
	return &in, nil
}

//===============================================================================

// GetClient returns an appropriate bigquery client.
func GetClient(project string) (*bigquery.Client, error) {
	// We do this here, instead of in init(), because we only want to do it
	// when we actually want to access the bigquery backend.

	// So apparently the client holds on to the context, but doesn't care if it
	// expires.  Best to just pass in Background.
	return bigquery.NewClient(context.Background(), project)
}

//===============================================================================

// MapSaver is a generic implementation of bq.ValueSaver, based on maps.  This avoids extra
// conversion steps in the bigquery library (except for the JSON conversion).
// IMPLEMENTS: bigquery.ValueSaver
type MapSaver map[string]bigquery.Value

// Save implements the bigquery.ValueSaver interface
func (s MapSaver) Save() (row map[string]bigquery.Value, insertID string, err error) {
	return s, "", nil
}

func assertSaver(ms MapSaver) {
	func(bigquery.ValueSaver) {}(ms)
}

//----------------------------------------------------------------------------

type BQInserter struct {
	// These are either constant or threadsafe.
	params     etl.InserterParams
	uploader   etl.Uploader  // May be a BQ Uploader, or a test Uploader
	putTimeout time.Duration // Timeout used for BQ put operations.

	// Rows must be accessed only by struct owner.
	rows []interface{}

	// The metrics are accessed by both the struct owner, and the flusher goroutine.
	// Those accesses are protected by the token.
	// We use a token instead of a mutex, because it is acquired in FlushAsync,
	// but released by the flusher goroutine.
	token chan struct{} // Token required for metric updates.

	// TODO: Consider making some of these atomics?
	pending  int // Number of rows being flushed.
	inserted int // Number of rows successfully inserted.
	badRows  int // Number of row failures, including rows in full failures.
	failures int // Number of complete insert failures.
}

// Caller should check error, and take appropriate action before calling again.
// Not threadsafe.  Should only be called by owning thread.
// Deprecated:  Please use external buffer, Put, and PutAsync instead.
func (in *BQInserter) InsertRow(data interface{}) error {
	return in.InsertRows([]interface{}{data})
}

// maybeCountRowSize periodically converts a row to JSON to measure its size.
func (in *BQInserter) maybeCountRowSize(data []interface{}) {
	if len(data) == 0 {
		return
	}
	counter, ok := tableInsertCounts.Load(in.TableBase())
	if !ok {
		tableInsertCounts.Store(in.TableBase(), new(uint32))
		counter, ok = tableInsertCounts.Load(in.TableBase())
		if !ok {
			log.Println("No counter for", in.TableBase())
			return
		}
	}
	count := atomic.AddUint32(counter.(*uint32), 1)
	// Do this just once in a while, so it doesn't take much resource.
	if count%insertsBeforeRowJSONCount != 0 {
		return
	}
	// Note: this estimate works very well for map[]bigquery.Value types. And, we
	// believe it is an okay estimate for struct types.
	jsonRow, _ := json.Marshal(data[0])
	metrics.RowSizeHistogram.WithLabelValues(
		in.TableBase()).Observe(float64(len(jsonRow)))
}

// Caller should check error, and take appropriate action before calling again.
// Not threadsafe.  Should only be called by owning thread.
// Deprecated:  Please use external buffer, Put, and PutAsync instead.
func (in *BQInserter) InsertRows(data []interface{}) error {
	metrics.WorkerState.WithLabelValues(in.TableBase(), "insert").Inc()
	defer metrics.WorkerState.WithLabelValues(in.TableBase(), "insert").Dec()

	in.maybeCountRowSize(data)

	for len(data)+len(in.rows) >= in.params.BufferSize {
		// space >= len(data)
		space := cap(in.rows) - len(in.rows)
		var add []interface{}
		add, data = data[:space], data[space:] // does this break?
		in.rows = append(in.rows, add...)
		err := in.Flush()
		if err != nil {
			// TODO - handle errors in middle better?
			return err
		}
	}
	in.rows = append(in.rows, data...)
	return nil
}

func (in *BQInserter) updateMetrics(err error) error {
	if in.pending == 0 {
		log.Println("Unexpected state error!!")
	}
	in.inserted += in.pending

	switch typedErr := err.(type) {
	case bigquery.PutMultiError:
		if len(typedErr) > in.pending {
			log.Println("Inconsistent state error!!")
		}
		if len(typedErr) == in.pending {
			log.Printf("%v\n", err)
			metrics.BackendFailureCount.WithLabelValues(
				in.TableBase(), "failed insert").Inc()
			in.failures++
		}
		// If ALL rows failed, and number of rows is large, just report single failure.
		if len(typedErr) > 10 && len(typedErr) == in.pending {
			log.Printf("Insert error: %v\n", err)
			metrics.ErrorCount.WithLabelValues(
				in.TableBase(), "PutMultiError", "insert row error").
				Add(float64(len(typedErr)))
		} else {
			// Handle each error individually.
			// TODO Should we try to handle large numbers of row errors?
			for _, rowError := range typedErr {
				// These are rowInsertionErrors
				log.Printf("%v\n", rowError)
				// rowError.Errors is a MultiError
				for _, oneErr := range rowError.Errors {
					log.Printf("Insert error: %v\n", oneErr)
					metrics.ErrorCount.WithLabelValues(
						in.TableBase(), "PutMultiError", "insert row error").Inc()
				}
			}
		}
		in.inserted -= len(typedErr)
		in.badRows += len(typedErr)
		err = nil
	case *url.Error:
		log.Printf("Unhandled url.Error on insert %v Project: %s, Dataset: %s, Table: %s\n",
			typedErr, in.Project(), in.Dataset(), in.FullTableName())
		metrics.BackendFailureCount.WithLabelValues(
			in.TableBase(), "failed insert").Inc()
		metrics.ErrorCount.WithLabelValues(
			in.TableBase(), "url.Error", "UNHANDLED insert error").Inc()
		// TODO - Conservative, but possibly not correct.
		// This at least preserves the count invariance.
		in.inserted -= in.pending
		in.badRows += in.pending
		err = nil
	case *googleapi.Error:
		log.Printf("Unhandled googleapi.Error on insert %v\n", typedErr)
		metrics.BackendFailureCount.WithLabelValues(
			in.TableBase(), "failed insert").Inc()
		metrics.ErrorCount.WithLabelValues(
			in.TableBase(), "googleapi.Error", "UNHANDLED insert error").Inc()
		// TODO - Conservative, but possibly not correct.
		// This at least preserves the count invariance.
		in.inserted -= in.pending
		in.badRows += in.pending
		err = nil

	default:
		// With Elem(), this was causing panics.
		log.Printf("Unhandled %v on insert %v Project: %s, Table: %s\n", reflect.TypeOf(typedErr),
			typedErr, in.Project(), in.FullTableName())
		metrics.BackendFailureCount.WithLabelValues(
			in.TableBase(), "failed insert").Inc()
		metrics.ErrorCount.WithLabelValues(
			in.TableBase(), "unknown", "UNHANDLED insert error").Inc()
		// TODO - Conservative, but possibly not correct.
		// This at least preserves the count invariance.
		in.inserted -= in.pending
		in.badRows += in.pending
		err = nil
	}
	in.pending = 0
	return err
}

// acquire and release handle the single token that protects the FlushSlice and
// access to the metrics.
func (in *BQInserter) acquire() {
	<-in.token
}
func (in *BQInserter) release() {
	in.token <- struct{}{} // return the token.
}

// Put sends a slice of rows to BigQuery, processes any
// errors, and updates row stats. It uses a token to serialize with any previous
// calls to PutAsync, to ensure that when Put() returns, all flushes
// have completed and row stats reflect PutAsync requests.  (Of course races
// may occur if calls are made from multiple goroutines).
// It is THREAD-SAFE.
// It may block if there is already a Put or Flush in progress.
func (in *BQInserter) Put(rows []interface{}) error {
	in.acquire()
	err := in.flushSlice(rows)
	in.release()
	return err
}

// PutAsync asynchronously sends a slice of rows to BigQuery, processes any
// errors, and updates row stats. It uses a token to serialize with other
// (likely synchronous) calls, to ensure that when Put() returns, all flushes
// have completed and row stats reflect PutAsync requests.  (Of course races
// may occur if these are called from multiple goroutines).
// It is THREAD-SAFE.
// It may block if there is already a Put or Flush in progress.
func (in *BQInserter) PutAsync(rows []interface{}) {
	in.acquire()
	go func() {
		in.flushSlice(rows)
		in.release()
	}()
}

// Flush synchronously flushes the rows in the row buffer up to BigQuery
// It is NOT threadsafe, as it touches the row buffer, so should only be called
// by the owning thread.
// Deprecated:  Please use external buffer, Put, and PutAsync instead.
func (in *BQInserter) Flush() error {
	rows := in.rows
	// Allocate new slice of rows.  Any failed rows are lost.
	in.rows = make([]interface{}, 0, in.params.BufferSize)
	return in.Put(rows)
}

// flushSlice flushes a slice of rows to BigQuery.
// It is NOT threadsafe.
func (in *BQInserter) flushSlice(rows []interface{}) error {
	metrics.WorkerState.WithLabelValues(in.TableBase(), "flush").Inc()
	defer metrics.WorkerState.WithLabelValues(in.TableBase(), "flush").Dec()

	if len(rows) == 0 {
		return nil
	}

	in.pending = len(rows)

	// If we exceed the quota, this basically backs off and tries again.  When
	// operating near quota, this will fire enough times to slow down each task
	// enough to stay within quota.  It may result in AppEngine reducing the
	// number of workers, but that is fine - it will also result in staying
	// under the quota.
	// Analysis:
	//  We can handle a minimum of 10 inserts per second, because
	//   the default quota is 100MB/sec, and the limit on requests
	//   is 10MB per request.  Since generally inserts are smaller,
	//   the typical number is more like 20 inserts/sec.
	//  The net effect we need to see is that, if the pipeline capacity
	//   exceeds the quota by 10%, then the pipeline needs to slow down
	//   by roughly 10% to fit within the quota.  The incoming request
	//   rate is dictated by the task queue, and ultimately the handler
	//   must reject 10% of the incoming requests.  This only happens
	//   when 10% of the instances have hit MAX_WORKERS.
	//  If the capacity of the pipeline is, e.g., 2X the task queue rate,
	//   then each task will need to be slowed down to the point that it
	//   takes roughly 2.2X longer than it would without any Quota exceeded
	//   errors.  For NDT, the 100MB tasks require about 35 concurrent tasks
	//   to process 60 tasks/min, indicating that they require about 35
	//   seconds per task.  There are about 70 tests/task, so this is about
	//   7 buffer flushes per second (of 10 tests each), or on average, about
	//   one buffer flush every 5 seconds for each task.
	//  The batch job might have 50 instances, and process 900 tasks
	//   concurrently.  If this had to be scaled back to 50%, the tasks
	//   would have to spend 50% of their time sleeping between Put requests.
	//   Since each task typically takes about 35 seconds, each task would
	//   on average experience just over one 'Quota exceeded' error in order
	//   to slow the pipeline down by 50%.
	//  Note that a secondary effect is to reduce the CPU utilization, which
	//   will likely trigger a reduction in the number of instances running.
	//   Under these conditions, AppEngine would reduce the number of instances
	//   until the target utilization is reaches, reducing the number of
	//   concurrent tasks, and thus the frequency at which the tasks would
	//   experience 'Quota error' events.

	start := time.Now()
	var err error
	for backoff := 10 * time.Millisecond; backoff < in.params.MaxRetryDelay; backoff *= 2 {
		// This is heavyweight, and may run forever without a context deadline.
		ctx, cancel := context.WithTimeout(context.Background(), in.putTimeout)
		err = in.uploader.Put(ctx, rows)
		cancel()

		if err == nil || !strings.Contains(err.Error(), "Quota exceeded:") {
			break
		}
		metrics.WarningCount.WithLabelValues(in.TableBase(), "", "Quota Exceeded").Inc()

		// Use some randomness to reduce risk of synchronization across tasks.
		delayNanos := float32(backoff.Nanoseconds()) * (0.5 + rand.Float32()) // between 0.5 and 1.5 * RetryDelay
		// Duration is int64 in nanoseconds, so this converts back to a Duration.
		time.Sleep(time.Duration(delayNanos))
	}

	// If there is still an error, then handle it.
	if err == nil {
		in.inserted += in.pending
		in.pending = 0
		metrics.InsertionHistogram.WithLabelValues(
			in.TableBase(), "succeed").Observe(time.Since(start).Seconds())
	} else {
		size := len(rows)
		apiError, ok := err.(*googleapi.Error)
		if !ok || size <= 1 || apiError.Code != 400 || !strings.Contains(apiError.Error(), "Request payload size exceeds the limit:") {
			// This adjusts the inserted count, failure count, and updates in.rows.
			log.Printf("%s %v", in.TableBase(), err)
			metrics.InsertionHistogram.WithLabelValues(
				in.TableBase(), "fail").Observe(time.Since(start).Seconds())
			return in.updateMetrics(err)
		}

		// Explicitly handle "Request payload size exceeds ..."
		// NOTE: This splitting behavior may cause repeated encoding of the same data.  Worst case,
		// all the data may be encoded log2(size) times.  So best if this only happens infrequently.
		log.Printf("Splitting %d rows to avoid size limit for %s\n", size, in.TableBase())
		metrics.WarningCount.WithLabelValues(in.TableBase(), "", "Splitting buffer").Inc()
		in.pending = 0
		err1 := in.flushSlice(rows[:size/2])
		err2 := in.flushSlice(rows[size/2:])
		// The recursive calls will have added various InsertionHistogram results, included successes
		// and failures, so we don't need to add those here.
		// Recursive calls also will have handled accounting for any errors not resolved by splitting,
		// but we want to return any non-nil error up the stack WITHOUT repeating the accounting.
		if err1 != nil {
			err = err1
		} else {
			err = err2
		}
	}
	return err
}

func (in *BQInserter) FullTableName() string {
	return in.TableBase() + in.TableSuffix()
}
func (in *BQInserter) TableBase() string {
	return in.params.Table
}

// The $ or _ suffix.
func (in *BQInserter) TableSuffix() string {
	return in.params.Suffix
}
func (in *BQInserter) Project() string {
	return in.params.Project
}
func (in *BQInserter) Dataset() string {
	return in.params.Dataset
}

func (in *BQInserter) RowsInBuffer() int {
	return len(in.rows)
}
func (in *BQInserter) Accepted() int {
	in.acquire()
	defer in.release()
	return in.inserted + in.badRows + in.pending + len(in.rows)
}
func (in *BQInserter) Committed() int {
	in.acquire()
	defer in.release()
	return in.inserted
}
func (in *BQInserter) Failed() int {
	in.acquire()
	defer in.release()
	return in.badRows
}
