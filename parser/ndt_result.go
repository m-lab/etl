package parser

// WARNING:
// WARNING: Parser for a deprecated format.
// WARNING: After 2019-08-01 this parser should be removed or unit tests added.
// WARNING: TODO: https://github.com/m-lab/etl/issues/697
// WARNING:

// This file defines the Parser subtype that handles NDTResult data.

import (
	"bytes"
	"encoding/json"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/go/rtx"
)

//=====================================================================================
//                       NDTResult Parser
//=====================================================================================
type NDTResultParser struct {
	inserter     etl.Inserter
	etl.RowStats // RowStats implemented for NDTResultParser with an embedded struct.
}

func NewNDTResultParser(ins etl.Inserter) etl.Parser {
	return &NDTResultParser{
		inserter: ins,
		RowStats: ins} // Delegate RowStats functions to the Inserter.
}

func (dp *NDTResultParser) TaskError() error {
	return nil
}

// IsParsable returns the canonical test type and whether to parse data.
func (dp *NDTResultParser) IsParsable(testName string, data []byte) (string, bool) {
	// Files look like: "<UUID>.json"
	if strings.HasSuffix(testName, "json") {
		return "ndt_result", true
	}
	return "unknown", false
}

// ValueMap implements the bigquery.ValueSaver interface. Rows as ValueMaps can
// be directly written to BigQuery tables.
type ValueMap map[string]bigquery.Value

// Save converts the ValueMap to the underlying type. It never fails.
func (s ValueMap) Save() (row map[string]bigquery.Value, insertID string, err error) {
	return s, "", nil
}

func assertSaver(ms ValueMap) {
	func(bigquery.ValueSaver) {}(ms)
}

// convert translates a map[string]interface{} (such as from an unmarshalled
// JSON object) to a ValueMap.
func convert(orig map[string]interface{}) ValueMap {
	c := ValueMap{}
	for name, v := range orig {
		switch v.(type) {
		case map[string]interface{}:
			c[name] = convert(v.(map[string]interface{}))
		default:
			c[name] = bigquery.Value(v)
		}
	}
	return c
}

// convertMapToNameValue replaces map objects to an array of NameValue structs.
func convertMapToNameValue(result map[string]interface{}, control, metadata string) {
	if result[control] == nil {
		return
	}
	c := result[control].(map[string]interface{})
	md := c[metadata]
	delete(c, metadata)
	nv := []schema.NameValue{}
	if md != nil {
		for name, value := range md.(map[string]interface{}) {
			nv = append(nv, schema.NameValue{Name: name, Value: value.(string)})
		}
	}
	c[metadata] = nv
}

// NOTE: NDTResult data is a JSON object that should be pushed directly into BigQuery.
// We read the value into a struct, for compatibility with current inserter
// backend and to eventually rely on the schema inference in m-lab/go/bqx.CreateTable().

// ParseAndInsert decodes the NDT Result JSON data and inserts it into BQ.
func (dp *NDTResultParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error {
	// TODO: derive 'ndt5' (or 'ndt7') labels from testName.
	metrics.WorkerState.WithLabelValues(dp.TableName(), "ndt_result").Inc()
	defer metrics.WorkerState.WithLabelValues(dp.TableName(), "ndt_result").Dec()

	rdr := bytes.NewReader(test)
	dec := json.NewDecoder(rdr)
	rowCount := 0

	for dec.More() {
		stats := map[string]interface{}{
			"test_id": testName,
			"ParseInfo": map[string]interface{}{
				"TaskFileName":  meta["filename"].(string),
				"ParseTime":     time.Now(),
				"ParserVersion": Version(),
			},
		}
		result := map[string]interface{}{}
		err := dec.Decode(&result)
		if err != nil {
			metrics.TestCount.WithLabelValues(
				dp.TableName(), "ndt_result", "Decode").Inc()
			return err
		}
		rowCount++

		convertMapToNameValue(result, "Control", "ClientMetadata")
		convertMapToNameValue(result, "Upload", "ClientMetadata")
		convertMapToNameValue(result, "Download", "ClientMetadata")

		stats["result"] = result
		// Set the LogTime to the Result.StartTime
		t, err := time.Parse(time.RFC3339Nano, result["StartTime"].(string))
		rtx.Must(err, "Failed to parse: %s", result["StartTime"].(string))
		stats["log_time"] = t.Unix()

		// Estimate the row size based on the input JSON size.
		metrics.RowSizeHistogram.WithLabelValues(
			dp.TableName()).Observe(float64(len(test)))

		rtx.Must(err, "Failed to convert to valuesaver")

		err = dp.inserter.InsertRow(convert(stats))
		if err != nil {
			switch t := err.(type) {
			case bigquery.PutMultiError:
				// TODO improve error handling??
				metrics.TestCount.WithLabelValues(
					dp.TableName(), "ndt_result", "insert-multi").Inc()
				log.Printf("%v\n", t[0].Error())
			default:
				metrics.TestCount.WithLabelValues(
					dp.TableName(), "ndt_result", "insert-other").Inc()
			}
			return err
		}
		// Count successful inserts.
		metrics.TestCount.WithLabelValues(dp.TableName(), "ndt_result", "ok").Inc()
	}

	return nil
}

// NB: These functions are also required to complete the etl.Parser interface.
// For NDTResult, we just forward the calls to the Inserter.

func (dp *NDTResultParser) Flush() error {
	return dp.inserter.Flush()
}

func (dp *NDTResultParser) TableName() string {
	return dp.inserter.TableBase()
}

func (dp *NDTResultParser) FullTableName() string {
	return dp.inserter.FullTableName()
}
