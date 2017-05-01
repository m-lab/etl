package parser

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/web100"
)

type NDTParser struct {
	inserter  etl.Inserter
	tmpDir    string
	tableName string
}

func NewNDTParser(ins etl.Inserter, tableName, tmpDir string) *NDTParser {
	return &NDTParser{ins, tmpDir, tableName}
}

// ParseAndInsert extracts the last snaplog from the given raw snap log.
func (n *NDTParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, rawSnapLog []byte) error {
	// TODO(prod): do not write to a temporary file; operate on byte array directly.
	// Write rawSnapLog to /mnt/tmpfs.
	if !strings.HasSuffix(testName, "c2s_snaplog") && !strings.HasSuffix(testName, "s2c_snaplog") {
		// Ignoring non-snaplog file.
		return nil
	}
	if len(rawSnapLog) > 10*1024*1024 {
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": "oversize"}).Inc()
		log.Printf("Ignoring oversize snaplog: %d, %s\n",
			len(rawSnapLog), testName)
		return nil
	}

	tmpFile, err := ioutil.TempFile(n.tmpDir, "snaplog-")
	if err != nil {
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": "bad-tmp"}).Inc()
		log.Printf("Failed to create tmpfile for: %s\n", testName)
		return err
	}
	// Record the file size.
	metrics.FileSizeHistogram.Observe(float64(len(rawSnapLog)))

	metrics.WorkerState.WithLabelValues("ndt").Inc()
	defer metrics.WorkerState.WithLabelValues("ndt").Dec()

	c := 0
	for count := 0; count < len(rawSnapLog); count += c {
		c, err = tmpFile.Write(rawSnapLog)
		if err != nil {
			metrics.TestCount.With(prometheus.Labels{
				"table": n.TableName(), "type": "bad-write"}).Inc()
			return err
		}
	}
	tmpFile.Sync()
	// TODO(dev): log possible remove errors.
	defer os.Remove(tmpFile.Name())

	// TODO(dev): only do this once.
	// Parse the tcp-kis.txt web100 variable definition file.
	metrics.WorkerState.WithLabelValues("asset").Inc()
	defer metrics.WorkerState.WithLabelValues("asset").Dec()

	data, err := web100.Asset("tcp-kis.txt")
	if err != nil {
		// Asset missing from build.
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": "no-asset"}).Inc()
		return err
	}
	b := bytes.NewBuffer(data)
	metrics.WorkerState.WithLabelValues("parse-def").Inc() // ####################
	defer metrics.WorkerState.WithLabelValues("parse-def").Dec()
	legacyNames, err := web100.ParseWeb100Definitions(b)
	if err != nil {
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": "legacy-names"}).Inc()
		return err
	}

	// Open the file we created above.
	w, err := web100.Open(tmpFile.Name(), legacyNames)
	if err != nil {
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": "bad-open"}).Inc()
		return err
	}
	defer w.Close()

	// Find the last web100 snapshot.
	metrics.WorkerState.WithLabelValues("seek").Inc()
	defer metrics.WorkerState.WithLabelValues("seek").Dec()
	for {
		err = w.Next()
		if err != nil {
			// TODO - this will lose tests.  Do something better!
			break
		}
	}
	// We expect EOF.
	if err != io.EOF {
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": "not-eof"}).Inc()
		log.Printf("Failed to reach EOF: %s\n", tmpFile.Name())
		return err
	}

	// Extract the values from the last snapshot.
	metrics.WorkerState.WithLabelValues("parse").Inc()
	defer metrics.WorkerState.WithLabelValues("parse").Dec()
	results, err := w.Values()
	if err != nil {
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": "values-err"}).Inc()
		return err
	}
	err = n.inserter.InsertRow(&bq.MapSaver{results})

	if err != nil {
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": "insert-err"}).Inc()
	} else {
		if strings.HasSuffix(testName, "c2s_snaplog") {
			metrics.TestCount.With(prometheus.Labels{
				"table": n.TableName(), "type": "c2s"}).Inc()
		} else {
			metrics.TestCount.With(prometheus.Labels{
				"table": n.TableName(), "type": "s2c"}).Inc()
		}
	}
	return err
}

func (n *NDTParser) TableName() string {
	return n.tableName
}
