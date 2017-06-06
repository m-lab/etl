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
	//"os"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/context"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
)

// TODO(dev) Use a more thoughtful setting for buffer size.
// For now, 10K per row times 100 results is 1MB, which is an order of
// magnitude below our 10MB max, so 100 might not be such a bad
// default.
func NewInserter(dataset string, dt etl.DataType, partition time.Time) (etl.Inserter, error) {
	suffix := ""
	table := etl.DataTypeToTable[dt]
	if time.Since(partition) < 30*24*time.Hour {
		// If within past 30 days, we can stream directly to partition.
		suffix = "$" + partition.Format("20060102")
	} else {
		// Otherwise, we use a templated table, and must merge it later.
		suffix = "_" + partition.Format("20060102")
	}

	return NewBQInserter(
		etl.InserterParams{Dataset: dataset, Table: table, Suffix: suffix,
			Timeout: 15 * time.Minute, BufferSize: 500}, nil)
}

// TODO - improve the naming between here and NewInserter.
// Pass in nil uploader for normal use, custom uploader for custom behavior
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

// Returns the Singleton bigquery client for this process.
// TODO - is there any advantage to using more than one client?
func MustGetClient(timeout time.Duration) *bigquery.Client {
	// We do this here, instead of in init(), because we only want to do it
	// when we actually want to access the bigquery backend.
	clientOnce.Do(func() {
		ctx, _ := context.WithTimeout(context.Background(), timeout)
		// Heavyweight!
		var err error
		//bqClient, err = bigquery.NewClient(ctx, os.Getenv("GCLOUD_PROJECT"))
                bqClient, err = bigquery.NewClient(ctx, "mlab-sandbox")
		if err != nil {
			panic(err.Error())
		}
	})
	return bqClient
}

//===============================================================================

// Generic implementation of bq.ValueSaver, based on map.  This avoids extra
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
	badRows  int // Number of row failures.
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
	metrics.WorkerState.WithLabelValues("insert").Inc()
	defer metrics.WorkerState.WithLabelValues("insert").Dec()
	for len(data)+len(in.rows) >= in.params.BufferSize {
		// space >= len(data)
		space := cap(in.rows) - len(in.rows)
		var add []interface{}
		add, data = data[:space], data[space:] // does this break?
		in.rows = append(in.rows, add...)
		err := in.Flush()
		if err != nil {
			// TODO - handle errors in middle better?
                        log.Printf("%v\n", err)
			return err
		}
	}
	in.rows = append(in.rows, data...)
	return nil
}

func (in *BQInserter) HandleInsertErrors(err error) error {
	switch typedErr := err.(type) {
	case bigquery.PutMultiError:
		// If ALL rows failed, and number of rows is large, just report single failure.
		if len(typedErr) > 10 && len(typedErr) == len(in.rows) {
			log.Printf("%v\n", err)
			metrics.ErrorCount.WithLabelValues(
				in.TableBase(), "failed insert").Inc()
			in.failures += 1
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
						in.TableBase(), "insert row error").Inc()
				}
			}
		}
		in.inserted += len(in.rows) - len(typedErr)
		in.badRows += len(typedErr)
		err = nil
	default:
		log.Printf("Unhandled insert error %v\n", typedErr)
		metrics.ErrorCount.WithLabelValues(
			in.TableBase(), "other insert error").Inc()
		err = nil
	}
	// Allocate new slice of rows.  Any failed rows are lost.
	in.rows = make([]interface{}, 0, in.params.BufferSize)
	return err
}

// TODO(dev) Should have a recovery mechanism for failed inserts.
func (in *BQInserter) Flush() error {
	metrics.WorkerState.WithLabelValues("flush").Inc()
	defer metrics.WorkerState.WithLabelValues("flush").Dec()

	if len(in.rows) == 0 {
		return nil
	}

	// This is heavyweight, and may run forever without a context deadline.
	ctx, _ := context.WithTimeout(context.Background(), in.timeout)
	err := in.uploader.Put(ctx, in.rows)
	if err == nil {
		in.inserted += len(in.rows)
		in.rows = make([]interface{}, 0, in.params.BufferSize)
	} else {
		// This adjusts the inserted count, failure count, and updates in.rows.
                log.Printf("%v\n", err)
		err = in.HandleInsertErrors(err)
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
func (in *BQInserter) Dataset() string {
	return in.params.Dataset
}
func (in *BQInserter) RowsInBuffer() int {
	return len(in.rows)
}
func (in *BQInserter) Count() int {
	return in.inserted + len(in.rows)
}

//----------------------------------------------------------------------------

type NullInserter struct {
	etl.Inserter
}

func (in *NullInserter) InsertRow(data interface{}) error {
	return nil
}
func (in *NullInserter) InsertRows(data []interface{}) error {
	return nil
}
func (in *NullInserter) Flush() error {
	return nil
}
func (in *NullInserter) FullTableName() string {
	return "base_suffix"
}
func (in *NullInserter) TableBase() string {
	return "base"
}
func (in *NullInserter) TableSuffix() string {
	return "_suffix"
}
func (in *NullInserter) Dataset() string {
	return ""
}
func (in *NullInserter) RowsInBuffer() int {
	return 0
}
func (in *NullInserter) Count() int {
	return 0
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
