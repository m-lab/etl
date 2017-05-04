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
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/etl/web100"
)

var (
	TmpDir = "/mnt/tmpfs"
)

type NDTParser struct {
	inserter etl.Inserter
	// TODO(prod): eliminate need for tmpfs.
	tmpDir string
}

func NewNDTParser(ins etl.Inserter) *NDTParser {
	return &NDTParser{ins, TmpDir}
}

// ParseAndInsert extracts the last snaplog from the given raw snap log.
func (n *NDTParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, rawSnapLog []byte) error {
	// TODO(prod): do not write to a temporary file; operate on byte array directly.
	// Write rawSnapLog to /mnt/tmpfs.
	if !strings.HasSuffix(testName, "c2s_snaplog") && !strings.HasSuffix(testName, "s2c_snaplog") {
		// Ignoring non-snaplog file.
		return nil
	}

	// NOTE: this file size threshold and the number of simultaneous workers
	// defined in etl_worker.go must guarantee that all files written to
	// /mnt/tmpfs will fit.
	if len(rawSnapLog) > 10*1024*1024 {
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": "oversize"}).Inc()
		log.Printf("Ignoring oversize snaplog: %d, %s\n",
			len(rawSnapLog), testName)
		metrics.FileSizeHistogram.WithLabelValues(
			"huge").Observe(float64(len(rawSnapLog)))
		return nil
	} else {
		// Record the file size.
		metrics.FileSizeHistogram.WithLabelValues(
			"normal").Observe(float64(len(rawSnapLog)))
	}

	tmpFile, err := ioutil.TempFile(n.tmpDir, "snaplog-")
	if err != nil {
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": "no-tmp"}).Inc()
		log.Printf("Failed to create tmpfile for: %s, when processing: %s\n",
			testName, meta["filename"])
		return nil
	}

	metrics.WorkerState.WithLabelValues("ndt").Inc()
	defer metrics.WorkerState.WithLabelValues("ndt").Dec()

	c := 0
	for count := 0; count < len(rawSnapLog); count += c {
		c, err = tmpFile.Write(rawSnapLog)
		if err != nil {
			metrics.TestCount.With(prometheus.Labels{
				"table": n.TableName(), "type": "write-err"}).Inc()
			log.Printf("Tmpfs write error: %s, when processing: %s\n%s\n",
				testName, meta["filename"], err)
			return nil
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
		log.Printf("Asset missing error: %s, when processing: %s\n%s\n",
			testName, meta["filename"], err)
		return nil
	}
	b := bytes.NewBuffer(data)

	// These unfortunately nest.
	metrics.WorkerState.WithLabelValues("parse-def").Inc()
	defer metrics.WorkerState.WithLabelValues("parse-def").Dec()
	legacyNames, err := web100.ParseWeb100Definitions(b)
	if err != nil {
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": "legacy-names"}).Inc()
		log.Printf("ParseWeb100Def error: %s, when processing: %s\n%s\n",
			testName, meta["filename"], err)
		return nil
	}

	// Open the file we created above.
	w, err := web100.Open(tmpFile.Name(), legacyNames)
	if err != nil {
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": "no-tmp-legacy"}).Inc()
		log.Printf("legacyNames error: %s, when processing: %s\n%s\n",
			testName, meta["filename"], err)
		return nil
	}
	defer w.Close()

	// Find the last web100 snapshot.
	metrics.WorkerState.WithLabelValues("seek").Inc()
	defer metrics.WorkerState.WithLabelValues("seek").Dec()
	// Limit to parsing only up to 2100 snapshots.
	// NOTE: This is different from legacy pipeline!!
	for count := 0; count < 2100; count++ {
		err = w.Next()
		if err != nil {
			if err == io.EOF {
				// We expect EOF.
				break
			} else {
				// TODO - this will lose tests.  Do something better!
				metrics.TestCount.With(prometheus.Labels{
					"table": n.TableName(), "type": "not-eof"}).Inc()
				log.Printf("Failed to reach EOF: %d, %s, (%s), when processing: %s\n%s\n",
					count, tmpFile.Name(), testName, meta["filename"], err)
				return nil
			}
		}
		// HACK - just to see how expensive the Values() call is...
		// parse every 10th snapshot.
		if count%10 == 0 {
			// Note: read and discard the values by not saving the Web100ValueMap.
			err := w.SnapshotValues(schema.Web100ValueMap{})
			if err != nil {
				metrics.TestCount.With(prometheus.Labels{
					"table": n.TableName(), "type": "values-err"}).Inc()
			}
		}
	}

	// Extract the values from the last snapshot.
	metrics.WorkerState.WithLabelValues("parse").Inc()
	defer metrics.WorkerState.WithLabelValues("parse").Dec()

	snapValues := schema.Web100ValueMap{}
	err = w.SnapshotValues(snapValues)
	if err != nil {
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": "values-err"}).Inc()
		log.Printf("Error calling web100 Values(): %s, (%s), when processing: %s\n%s\n",
			tmpFile.Name(), testName, meta["filename"], err)
		return nil
	}

	connSpec := schema.Web100ValueMap{}
	w.ConnectionSpec(connSpec)

	results := schema.NewWeb100MinimalRecord(
		w.LogVersion(), w.LogTime(),
		(map[string]bigquery.Value)(connSpec),
		(map[string]bigquery.Value)(snapValues))

	err = n.inserter.InsertRow(&bq.MapSaver{fixValues(results)})
	if err != nil {
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": "insert-err"}).Inc()
		// TODO: This is an insert error, that might be recoverable if we try again.
		return err
	} else {
		// TODO - test type should be a separate label, so we can see which files have which errors.
		if strings.HasSuffix(testName, "c2s_snaplog") {
			metrics.TestCount.With(prometheus.Labels{
				"table": n.TableName(), "type": "c2s"}).Inc()
		} else {
			metrics.TestCount.With(prometheus.Labels{
				"table": n.TableName(), "type": "s2c"}).Inc()
		}
		return nil
	}
}

func (n *NDTParser) TableName() string {
	return n.inserter.TableName()
}

// fixValues updates web100 log values that need post-processing fix-ups.
// TODO(dev): does this only apply to NDT or is NPAD also affected?
func fixValues(r map[string]bigquery.Value) map[string]bigquery.Value {
	// TODO(dev): fix these values.
	// Fix StartTimeStamp:
	//  - web100_log_entry.snap.StartTimeStamp: (1000000 * StartTimeStamp + StartTimeUsec)
	// Fix IPv6 addresses in connection_spec:
	//  - web100_log_entry.connection_spec.local_ip
	//  - web100_log_entry.connection_spec.remote_ip
	// Fix local_af:
	//  - web100_log_entry.connection_spec.local_af: IPv4 = 0, IPv6 = 1.
	return r
}
