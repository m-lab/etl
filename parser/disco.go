// Package parser defines the Parser interface and implementations for the different
// test types, NDT, Paris Traceroute, and SideStream.
package parser

// This file defines the Parser subtype that handles DISCO data.

import (
	"bytes"
	"encoding/json"
	"log"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
)

//=====================================================================================
//                       Disco Parser
//=====================================================================================

type PortStats struct {
	// TODO - replace these with standard meta data.
	Meta struct {
		FileName  string `json:"filename, string"`
		TestName  string `json:"testname, string"`
		ParseTime int64  `json:"parsetime, int64"`
	} `json:"meta"`

	Sample []struct { //    []Sample `json: "sample"`
		Timestamp int64   `json:"timestamp, int64"`
		Value     float32 `json:"value, float32"`
	} `json:"sample"`
	Metric     string `json:"metric"`
	Hostname   string `json:"hostname"`
	Experiment string `json:"experiment"`

	// bigquery doesn't handle maps within structs.  8-(
	// Meta       map[string]bigquery.Value `json:"meta"`
}

// TODO(dev) add tests
type DiscoParser struct {
	inserter     etl.Inserter
	etl.RowStats // RowStats implemented for DiscoParser with an embedded struct.
}

func NewDiscoParser(ins etl.Inserter) etl.Parser {
	return &DiscoParser{
		inserter: ins,
		RowStats: ins} // Delegate RowStats functions to the Inserter.
}

// Disco data a JSON representation that should be pushed directly into BigQuery.
// For now, though, we parse into a struct, for compatibility with current inserter
// backend.
//
// Returns:
//   error on Decode error
//   error on InsertRows error
//   nil on success
//
// TODO - optimize this to use the JSON directly, if possible.
func (dp *DiscoParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error {
	meta["testname"] = testName
	ms := struct {
		FileName  string `json:"filename, string"`
		TestName  string `json:"testname, string"`
		ParseTime int64  `json:"parsetime, int64"`
	}{meta["filename"].(string), meta["testname"].(string), meta["parsetime"].(time.Time).Unix()}

	rdr := bytes.NewReader(test)
	dec := json.NewDecoder(rdr)
	for dec.More() {
		var ps PortStats
		ps.Meta = ms
		err := dec.Decode(&ps)
		if err != nil {
			metrics.TestCount.WithLabelValues(
				dp.TableName(), "disco", "Decode").Inc()
			// TODO(dev) Should accumulate errors, instead of aborting?
			return err
		}
		err = dp.inserter.InsertRow(ps)
		if err != nil {
			switch t := err.(type) {
			case bigquery.PutMultiError:
				// TODO improve error handling??
				metrics.TestCount.WithLabelValues(
					dp.TableName(), "disco", "insert-multi").Inc()
				log.Printf("%v\n", t[0].Error())
			default:
				metrics.TestCount.WithLabelValues(
					dp.TableName(), "disco", "insert-other").Inc()
			}
			// TODO(dev) Should accumulate errors, instead of aborting?
			return err
		}
	}
	metrics.TestCount.WithLabelValues(dp.TableName(), "disco", "ok").Inc()

	return nil
}

// These functions are also required to complete the etl.Parser interface.  For Disco,
// we just forward the calls to the Inserter.
func (dp *DiscoParser) Flush() error {
	return dp.inserter.Flush()
}

func (dp *DiscoParser) TableName() string {
	return dp.inserter.TableBase()
}

func (dp *DiscoParser) FullTableName() string {
	return dp.inserter.FullTableName()
}
