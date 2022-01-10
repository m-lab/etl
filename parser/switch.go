package parser

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/iancoleman/strcase"
	v2as "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/etl/schema"
)

var InvalidMetricName = errors.New("invalid metric name")

//=====================================================================================
//                       Switch Datatype Parser
//=====================================================================================

// SwitchParser handles parsing for the switch datatype.
type SwitchParser struct {
	*row.Base
	table  string
	suffix string
}

// NewSwitchParser returns a new parser for the switch archives.
func NewSwitchParser(sink row.Sink, table, suffix string, ann v2as.Annotator) etl.Parser {
	bufSize := etl.SW.BQBufferSize()
	if ann == nil {
		ann = v2as.GetAnnotator(etl.BatchAnnotatorURL)
	}

	return &SwitchParser{
		Base:   row.NewBase(table, sink, bufSize, ann),
		table:  table,
		suffix: suffix,
	}
}

// IsParsable returns the canonical test type and whether to parse data.
func (p *SwitchParser) IsParsable(testName string, data []byte) (string, bool) {
	// Files look like: "<date>-to-<date>-switch.json.gz"
	// Notice the "-" before switch.
	// Look for JSON and JSONL files.
	if strings.HasSuffix(testName, "switch.json") ||
		strings.HasSuffix(testName, "switch.jsonl") ||
		strings.HasSuffix(testName, "switch.json.gz") ||
		strings.HasSuffix(testName, "switch.jsonl.gz") {
		return "switch", true
	}
	return "", false
}

// ParseAndInsert decodes the switch data and inserts it into BQ.
func (p *SwitchParser) ParseAndInsert(fileMetadata map[string]bigquery.Value, testName string, rawContent []byte) error {
	metrics.WorkerState.WithLabelValues(p.TableName(), string(etl.SW)).Inc()
	defer metrics.WorkerState.WithLabelValues(p.TableName(), string(etl.SW)).Dec()

	reader := bytes.NewReader(rawContent)
	dec := json.NewDecoder(reader)
	rowCount := 0

	// Each file contains multiple samples referring to the same hostname, but
	// different timestamps. This map groups samples in rows by timestamp.
	timestampToRow := make(map[int64]*schema.SwitchRow)

	for dec.More() {
		// Unmarshal the raw JSON into a SwitchStats.
		// This can hold both DISCOv1 and DISCOv2 data.
		tmp := &schema.SwitchStats{}
		err := dec.Decode(tmp)
		if err != nil {
			metrics.TestCount.WithLabelValues(
				p.TableName(), string(etl.SW), "Decode").Inc()
			// TODO(dev) Should accumulate errors, instead of aborting?
			return err
		}

		// For collectd in the "utilization" experiment, by design, the raw data
		// time range starts and ends on the hour. This means that the raw
		// dataset inclues 361 time bins (360 + 1 extra). Originally, this was
		// so the last sample of the current time range would overlap with the
		// first sample of the next time range. However, this parser does not
		// use the extra sample, so we unconditionally ignore it here. However,
		// this is not the case for DISCOv2, so we use the whole sample from
		// DISCOv2. DISCOv2 can be differentiated from collectd by the "jsonl"
		// suffix.
		if len(tmp.Sample) > 0 {
			if !strings.HasSuffix(testName, "switch.jsonl") &&
				!strings.HasSuffix(testName, "switch.jsonl.gz") {
				tmp.Sample = tmp.Sample[:len(tmp.Sample)-1]
				// DISCOv1's Timestamp field in each sample represents the
				// *beginning* of a 10s sample window, while v2's Timestamp
				// represents the time at which the sample was taken, which is
				// representative of the previous 10s. Since v2's behavior is
				// what we want, we add 10s to all v1 Timestamps so that the
				// timestamps represent the same thing for v1 and v2.
				for i, v := range tmp.Sample {
					tmp.Sample[i].Timestamp = v.Timestamp + 10
				}
			}
		}

		// Iterate over the samples in the JSON.
		for _, sample := range tmp.Sample {
			// If a row for this timestamp does not exist already, create one.
			var row *schema.SwitchRow
			var ok bool
			if row, ok = timestampToRow[sample.Timestamp]; !ok {
				row = &schema.SwitchRow{
					Parser: schema.ParseInfo{
						Version:    Version(),
						Time:       time.Now(),
						ArchiveURL: fileMetadata["filename"].(string),
						Filename:   testName,
						GitCommit:  GitCommit(),
					},
					A: &schema.SwitchSummary{
						Machine: tmp.Hostname,
						Switch:  tmp.Experiment,
					},
					Raw: &schema.RawData{
						Metrics: []*schema.SwitchStats{},
					},
				}
				timestampToRow[sample.Timestamp] = row
			}

			// Set the SwitchRow's timestamp from the sample's timestamp.
			row.Date = time.Unix(sample.Timestamp, 0)

			// Create a Model containing only this sample and append it to
			// the current SwitchRow's Raw.Metrics field.
			model := &schema.SwitchStats{
				Experiment: tmp.Experiment,
				Hostname:   tmp.Hostname,
				Metric:     tmp.Metric,
				Sample:     []schema.Sample{sample},
			}
			row.Raw.Metrics = append(row.Raw.Metrics, model)
			// Read the sample to extract the summary.
			getSummaryFromSample(tmp.Metric, &sample, row)
		}
	}

	for _, row := range timestampToRow {
		rowCount++

		// Count the number of samples per record.
		metrics.DeltaNumFieldsHistogram.WithLabelValues(
			p.TableName()).Observe(float64(len(row.Raw.Metrics)))

		// TODO: estimate row size.
		// metrics.RowSizeHistogram.WithLabelValues(
		// 	p.TableName()).Observe(float64(row.Size()))

		// Insert the row.
		err := p.Base.Put(row)
		if err != nil {
			metrics.TestCount.WithLabelValues(
				p.TableName(), string(etl.SW), "put-error").Inc()
			return err
		}
		// Count successful inserts.
		metrics.TestCount.WithLabelValues(p.TableName(), string(etl.SW), "ok").Inc()
	}

	// Measure the distribution of records per file.
	metrics.EntryFieldCountHistogram.WithLabelValues(
		p.TableName()).Observe(float64(rowCount))

	return nil
}

// getSummaryFromSample reads the raw Sample and fills the corresponding
// fields in the SwitchRow.
func getSummaryFromSample(metric string, sample *schema.Sample, row *schema.SwitchRow) error {
	// Convert the metric name to its corresponding CamelCase field name.
	val := strcase.ToCamel(metric)
	counter := val + "Counter"

	// Use the "reflect" package to dinamically access the fields of the
	// summary struct. This is a bit hacky, but it works better than a lengthy
	// switch statement.
	v := reflect.ValueOf(row.A).Elem()
	valField := v.FieldByName(val)
	counterField := v.FieldByName(counter)
	if !valField.IsValid() || !counterField.IsValid() {
		return InvalidMetricName
	}

	// Set the fields' values from the sample.
	// Note: the octets.local.tx/rx values were not collected correctly
	// by DISCOv2, so we set them to zero until we can fix that.
	if metric == "switch.octets.local.tx" ||
		metric == "switch.octets.local.rx" {
		valField.SetUint(0)
		counterField.SetUint(0)
		return nil
	}

	valField.SetUint(uint64(sample.Value))
	counterField.SetUint(uint64(sample.Counter))
	return nil
}

// NB: These functions are also required to complete the etl.Parser interface
// For SwitchParser, we just forward the calls to the Inserter.

func (p *SwitchParser) Flush() error {
	return p.Base.Flush()
}

func (p *SwitchParser) TableName() string {
	return p.table
}

func (p *SwitchParser) FullTableName() string {
	return p.table + p.suffix
}

// RowsInBuffer returns the count of rows currently in the buffer.
func (p *SwitchParser) RowsInBuffer() int {
	return p.GetStats().Pending
}

// Committed returns the count of rows successfully committed to BQ.
func (p *SwitchParser) Committed() int {
	return p.GetStats().Committed
}

// Accepted returns the count of all rows received through InsertRow(s).
func (p *SwitchParser) Accepted() int {
	return p.GetStats().Total()
}

// Failed returns the count of all rows that could not be committed.
func (p *SwitchParser) Failed() int {
	return p.GetStats().Failed
}
