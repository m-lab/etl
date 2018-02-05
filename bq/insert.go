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
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/context"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
)

// NewInserter creates a new BQInserter with appropriate characteristics.
// TODO(P3) Include the project name in the parameters.
func NewInserter(dataset string, dt etl.DataType, partition time.Time) (etl.Inserter, error) {
	suffix := ""
	table := etl.DataTypeToTable[dt]
	if etl.IsBatchService() || time.Since(partition) > 30*24*time.Hour {
		// If batch, or too far in the past, we use a templated table, and must merge it later.
		suffix = "_" + partition.Format("20060102")
	} else {
		// Otherwise, we can stream directly to correct partition.
		suffix = "$" + partition.Format("20060102")
	}

	return NewBQInserter(
		etl.InserterParams{Dataset: dataset, Table: table, Suffix: suffix,
			Timeout: 15 * time.Minute, BufferSize: dt.BQBufferSize(), RetryDelay: 30 * time.Second},
		nil)
}

// NewBQInserter initializes a new BQInserter
// Pass in nil uploader for normal use, custom uploader for custom behavior
// TODO - improve the naming between here and NewInserter.
func NewBQInserter(params etl.InserterParams, uploader etl.Uploader) (etl.Inserter, error) {
	if uploader == nil {
		client := MustGetClient(params.Timeout)
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
	in := BQInserter{params: params, uploader: uploader, timeout: params.Timeout}
	in.rows = make([]interface{}, 0, in.params.BufferSize)
	return &in, nil
}

//===============================================================================
var (
	clientOnce sync.Once // This avoids a race on setting bqClient.
	bqClient   *bigquery.Client
)

// MustGetClient returns the Singleton bigquery client for this process.
// TODO - is there any advantage to using more than one client?
func MustGetClient(timeout time.Duration) *bigquery.Client {
	// We do this here, instead of in init(), because we only want to do it
	// when we actually want to access the bigquery backend.
	clientOnce.Do(func() {
		ctx, _ := context.WithTimeout(context.Background(), timeout)
		project, ok := os.LookupEnv("BIGQUERY_PROJECT")
		if !ok {
			project = os.Getenv("GCLOUD_PROJECT")
		}

		log.Printf("Using project: %s\n", project)
		// Heavyweight!
		var err error
		bqClient, err = bigquery.NewClient(ctx, project)
		if err != nil {
			panic(err.Error())
		}
	})
	return bqClient
}

//===============================================================================

// MapSaver is a generic implementation of bq.ValueSaver, based on maps.  This avoids extra
// conversion steps in the bigquery library (except for the JSON conversion).
// IMPLEMENTS: bigquery.ValueSaver
type MapSaver struct {
	Values map[string]bigquery.Value
}

func (s *MapSaver) Save() (row map[string]bigquery.Value, insertID string, err error) {
	return s.Values, "", nil
}

//----------------------------------------------------------------------------

type BQInserter struct {
	etl.Inserter
	params   etl.InserterParams
	uploader etl.Uploader // May be a BQ Uploader, or a test Uploader
	timeout  time.Duration
	rows     []interface{}
	inserted int // Number of rows successfully inserted.
	badRows  int // Number of row failures, including rows in full failures.
	failures int // Number of complete insert failures.
}

// Caller should check error, and take appropriate action before calling again.
func (in *BQInserter) InsertRow(data interface{}) error {
	return in.InsertRows([]interface{}{data})
}

// Caller should check error, and take appropriate action before calling again.
// TODO - should this return a specific error to indicate that a flush is needed
// instead of flushing internally?  The "handle errors in the middle" would
// be easier, though other complications would ensue.
func (in *BQInserter) InsertRows(data []interface{}) error {
	metrics.WorkerState.WithLabelValues(in.TableBase(), "insert").Inc()
	defer metrics.WorkerState.WithLabelValues(in.TableBase(), "insert").Dec()

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

func (in *BQInserter) HandleInsertErrors(err error) error {
	switch typedErr := err.(type) {
	case bigquery.PutMultiError:
		if len(typedErr) == len(in.rows) {
			log.Printf("%v\n", err)
			metrics.BackendFailureCount.WithLabelValues(
				in.TableBase(), "failed insert").Inc()
			in.failures += 1
		}
		// If ALL rows failed, and number of rows is large, just report single failure.
		if len(typedErr) > 10 && len(typedErr) == len(in.rows) {
			log.Printf("Insert error: %v\n", err)
			metrics.ErrorCount.WithLabelValues(
				in.TableBase(), "unknown", "insert row error").
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
						in.TableBase(), "unknown", "insert row error").Inc()
				}
			}
		}
		in.inserted += len(in.rows) - len(typedErr)
		in.badRows += len(typedErr)
		err = nil
	default:
		log.Printf("Unhandled insert error %v\n", typedErr)
		metrics.BackendFailureCount.WithLabelValues(
			in.TableBase(), "failed insert").Inc()
		metrics.ErrorCount.WithLabelValues(
			in.TableBase(), "unknown", "UNHANDLED insert error").Inc()
		// TODO - Conservative, but possibly not correct.
		// This at least preserves the count invariance.
		in.badRows += len(in.rows)
		err = nil
	}
	return err
}

// TODO(dev) Should have a recovery mechanism for failed inserts.
func (in *BQInserter) Flush() error {
	metrics.WorkerState.WithLabelValues(in.TableBase(), "flush").Inc()
	defer metrics.WorkerState.WithLabelValues(in.TableBase(), "flush").Dec()

	if len(in.rows) == 0 {
		return nil
	}

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

	var err error
	for i := 0; i < 10; i++ {
		// This is heavyweight, and may run forever without a context deadline.
		ctx, _ := context.WithTimeout(context.Background(), in.timeout)
		err = in.uploader.Put(ctx, in.rows)
		if err == nil || !strings.Contains(err.Error(), "Quota exceeded:") {
			break
		}
		metrics.WarningCount.WithLabelValues(in.TableBase(), "", "Quota Exceeded").Inc()
		// Use some randomness to reduce risk of synchronization across tasks.
		t := in.params.RetryDelay.Seconds() * (0.5 + rand.Float64()) // between 0.5 and 1.5 * RetryDelay
		time.Sleep(time.Duration(1000000*t) * time.Microsecond)
	}

	// If there is still an error, then handle it.
	if err == nil {
		in.inserted += len(in.rows)
	} else {
		// This adjusts the inserted count, failure count, and updates in.rows.
		err = in.HandleInsertErrors(err)
	}
	// Allocate new slice of rows.  Any failed rows are lost.
	in.rows = make([]interface{}, 0, in.params.BufferSize)
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
func (in *BQInserter) Dataset() string {
	return in.params.Dataset
}
func (in *BQInserter) RowsInBuffer() int {
	return len(in.rows)
}
func (in *BQInserter) Accepted() int {
	return in.inserted + in.badRows + len(in.rows)
}
func (in *BQInserter) Committed() int {
	return in.inserted
}
func (in *BQInserter) Failed() int {
	return in.badRows
}

//----------------------------------------------------------------------------

// Inserter wrapper that handles flush metrics.
// TODO - add prometheus counters for attempts, number of rows.
type DurationWrapper struct {
	etl.Inserter
}

func (dw DurationWrapper) Flush() error {
	t := time.Now()
	status := "succeed"
	err := dw.Inserter.Flush()
	if err != nil {
		status = "fail"
	}
	metrics.InsertionHistogram.WithLabelValues(
		dw.TableBase(), status).Observe(time.Since(t).Seconds())
	return err
}
