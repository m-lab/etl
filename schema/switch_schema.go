package schema

import (
	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/row"
	"github.com/m-lab/go/cloud/bqx"

	"time"
)

// SwitchRow represents a single row of Switch data, taken from the raw DISCO
// exported file.
type SwitchRow struct {
	// Parser contains metadata about the parsing of this row.
	Parser ParseInfo `bigquery:"parser"`
	// Date is the collection timestamp of the samples.
	Date time.Time `bigquery:"date"`
	// A is the SwitchSummary containing the parsed metrics.
	A *SwitchSummary
	// Raw is the raw data from the DISCO export file.
	Raw *RawData `json:",omitempty" bigquery:"raw"`

	// NOT part of struct schema. Included only to provide a fake annotator interface.
	row.NullAnnotator `bigquery:"-"`
}

// SwitchSummary contains the parsed metrics, plus the machine/switch pair.
type SwitchSummary struct {
	// Machine is the hostname of the machine that collected the metrics.
	Machine string

	// Switch is the switch's hostname.
	Switch string

	// The following fields are parsed from the raw data.
	// Note: Counters are only available in DISCOv2 data. For DISCOv1, only
	// deltas are stored in the raw files, so the counters are set to 0.
	SwitchOctetsUplinkRxCounter   int64
	SwitchOctetsUplinkRx          int64
	SwitchOctetsUplinkTxCounter   int64
	SwitchOctetsUplinkTx          int64
	SwitchOctetsLocalRxCounter    int64
	SwitchOctetsLocalRx           int64
	SwitchOctetsLocalTxCounter    int64
	SwitchOctetsLocalTx           int64
	SwitchUnicastUplinkRxCounter  int64
	SwitchUnicastUplinkRx         int64
	SwitchUnicastUplinkTxCounter  int64
	SwitchUnicastUplinkTx         int64
	SwitchUnicastLocalRxCounter   int64
	SwitchUnicastLocalRx          int64
	SwitchUnicastLocalTxCounter   int64
	SwitchUnicastLocalTx          int64
	SwitchErrorsUplinkRxCounter   int64
	SwitchErrorsUplinkRx          int64
	SwitchErrorsUplinkTxCounter   int64
	SwitchErrorsUplinkTx          int64
	SwitchErrorsLocalRxCounter    int64
	SwitchErrorsLocalRx           int64
	SwitchErrorsLocalTxCounter    int64
	SwitchErrorsLocalTx           int64
	SwitchDiscardsUplinkRxCounter int64
	SwitchDiscardsUplinkRx        int64
	SwitchDiscardsUplinkTxCounter int64
	SwitchDiscardsUplinkTx        int64
	SwitchDiscardsLocalRxCounter  int64
	SwitchDiscardsLocalRx         int64
	SwitchDiscardsLocalTxCounter  int64
	SwitchDiscardsLocalTx         int64
}

// RawData wraps a slice of SwitchStats objects.
type RawData struct {
	Metrics []*SwitchStats
}

// Estimate the size of the RawData BQ row in bytes.
// Note: This assumes all the SwitchStats objects in the RawData have the
// same number of samples. This is generally true within a single DISCO file.
func (r *RawData) Size() int {
	if len(r.Metrics) == 0 {
		return 0
	}
	return (r.Metrics[0].Size() * len(r.Metrics))
}

// Schema returns the Bigquery schema for SwitchRow.
func (row *SwitchRow) Schema() (bigquery.Schema, error) {
	sch, err := bigquery.InferSchema(row)
	if err != nil {
		return bigquery.Schema{}, err
	}

	// The raw data from DISCO stores the timestamp of a sample as an integer (a
	// UNIX timestamp), but BigQuery represent the value as type TIMESTAMP.
	subs := map[string]bigquery.FieldSchema{
		"timestamp": {
			Name:        "timestamp",
			Description: "",
			Repeated:    false,
			Required:    false,
			Type:        "TIMESTAMP"},
	}
	c := bqx.Customize(sch, subs)

	docs := FindSchemaDocsFor(row)
	for _, doc := range docs {
		bqx.UpdateSchemaDescription(c, doc)
	}
	rr := bqx.RemoveRequired(sch)
	return rr, err
}

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
	Metric     string   `json:"metric" bigquery:"metric"`
	Hostname   string   `json:"hostname" bigquery:"hostname"`
	Experiment string   `json:"experiment" bigquery:"experiment"`
	Sample     []Sample `json:"sample" bigquery:"sample"`
}

// Size estimates the number of bytes in the SwitchStats object.
func (row *SwitchStats) Size() int {
	return (24*len(row.Sample) + len(row.Metric) + len(row.Hostname) +
		len(row.Experiment))
}
