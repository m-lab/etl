package schema

import (
	"cloud.google.com/go/bigquery"

	"github.com/m-lab/go/cloud/bqx"

	"time"
)

// Sample is an individual measurement taken by DISCO.
// NOTE: the types of the fields in this struct differ from the types used
// natively by the structs in DISCOv2. In DiSCOv2 Value is a uint64, but must
// be a float here because DISCOv1 outputs floats. float64 should be able to
// accommodate both types of input values safely. For Counter, DISCOv2 uses a
// uint64, but BigQuery does not support the notion of unsigned integers, so we
// use int64 here, which should be safe, too.
type Sample struct {
	Timestamp int64   `json:"timestamp" bigquery:"timestamp"`
	Value     float64 `json:"value" bigquery:"value"`
	Counter   int64   `json:"counter" bigquery:"counter"`
}

// SwitchStats represents a row of data taken from the raw DISCO export file.
type SwitchStats struct {
	TaskFilename  string    `json:"task_filename" bigquery:"task_filename"`
	TestID        string    `json:"test_id" bigquery:"test_id"`
	ParseTime     time.Time `json:"parse_time" bigquery:"parse_time"`
	ParserVersion string    `json:"parser_version" bigquery:"parser_version"`
	LogTime       int64     `json:"log_time" bigquery:"log_time"`
	Sample        []Sample  `json:"sample" bigquery:"sample"`
	Metric        string    `json:"metric" bigquery:"metric"`
	Hostname      string    `json:"hostname" bigquery:"hostname"`
	Experiment    string    `json:"experiment" bigquery:"experiment"`
}

// Size estimates the number of bytes in the SwitchStats object.
func (row *SwitchStats) Size() int {
	return (len(row.TaskFilename) + len(row.TestID) + 8 +
		12*len(row.Sample) + len(row.Metric) + len(row.Hostname) + len(row.Experiment))
}

// Schema returns the BigQuery schema for SwitchStats.
func (row *SwitchStats) Schema() (bigquery.Schema, error) {
	sch, err := bigquery.InferSchema(row)
	if err != nil {
		return bigquery.Schema{}, err
	}
	docs := FindSchemaDocsFor(row)
	for _, doc := range docs {
		bqx.UpdateSchemaDescription(sch, doc)
	}
	rr := bqx.RemoveRequired(sch)
	return rr, err
}
