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
// TODO(dev) This is getting big and ugly and needs to be refactored.
// TODO(prod): do not write to a temporary file; operate on byte array directly.
// Write rawSnapLog to /mnt/tmpfs.
func (n *NDTParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, rawSnapLog []byte) error {
	var testType string
	if strings.HasSuffix(testName, "c2s_snaplog") {
		testType = "c2s"
	} else if strings.HasSuffix(testName, "s2c_snaplog") {
		testType = "s2c"
	} else {
		if strings.HasSuffix(testName, "meta") {
			testType = "meta"
		} else {
			testType = "other"
		}
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": testType}).Inc()
		return nil
	}

	// NOTE: this file size threshold and the number of simultaneous workers
	// defined in etl_worker.go must guarantee that all files written to
	// /mnt/tmpfs will fit.
	if len(rawSnapLog) > 10*1024*1024 {
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": ">10MB"}).Inc()
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

	if len(rawSnapLog) < 32*1024 {
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": "<32KB"}).Inc()
		log.Printf("Note: small rawSnapLog: %d, %s\n",
			len(rawSnapLog), testName)
	}
	if len(rawSnapLog) == 4096 {
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": "4KB"}).Inc()
	}

	metrics.WorkerState.WithLabelValues("ndt").Inc()
	defer metrics.WorkerState.WithLabelValues("ndt").Dec()

	// TODO(dev): only do this once.
	// Parse the tcp-kis.txt web100 variable definition file.
	metrics.WorkerState.WithLabelValues("asset").Inc()
	defer metrics.WorkerState.WithLabelValues("asset").Dec()

	data, err := web100.Asset("tcp-kis.txt")
	if err != nil {
		// Asset missing from build.
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": "web100.Asset"}).Inc()
		log.Printf("web100.Asset error: %s processing %s from %s\n",
			err, testName, meta["filename"])
		return nil
	}
	b := bytes.NewBuffer(data)

	// These unfortunately nest.
	metrics.WorkerState.WithLabelValues("parse-def").Inc()
	defer metrics.WorkerState.WithLabelValues("parse-def").Dec()
	legacyNames, err := web100.ParseWeb100Definitions(b)
	if err != nil {
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": "web100.ParseDef"}).Inc()
		log.Printf("web100.ParseDef error: %s processing %s from %s\n",
			err, testName, meta["filename"])
		return nil
	}

	// TODO(prod): do not write to a temporary file; operate on byte array directly.
	// Write rawSnapLog to /mnt/tmpfs.
	tmpFile, err := ioutil.TempFile(n.tmpDir, "snaplog-")
	if err != nil {
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": "no-tmp"}).Inc()
		log.Printf("Failed to create tmpfile for: %s, when processing: %s\n",
			testName, meta["filename"])
		return nil
	}

	c := 0
	for count := 0; count < len(rawSnapLog); count += c {
		c, err = tmpFile.Write(rawSnapLog)
		if err != nil {
			metrics.TestCount.With(prometheus.Labels{
				"table": n.TableName(), "type": "tmpFile.Write"}).Inc()
			log.Printf("tmpFile.Write error: %s processing: %s from %s\n",
				err, testName, meta["filename"])
			return nil
		}
	}

	tmpFile.Sync()
	// TODO(dev): log possible remove errors.
	defer os.Remove(tmpFile.Name())

	// Open the file we created above.
	w, err := web100.Open(tmpFile.Name(), legacyNames)
	if err != nil {
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": "web100.Open"}).Inc()
		// These are mostly "could not parse /proc/web100/header",
		// with some "file read/write error C"
		log.Printf("web100.Open error: %s processing %s from %s\n",
			err, testName, meta["filename"])
		return nil
	}
	defer w.Close()

	// Find the last web100 snapshot.
	metrics.WorkerState.WithLabelValues("seek").Inc()
	defer metrics.WorkerState.WithLabelValues("seek").Dec()
	// Limit to parsing only up to 2100 snapshots.
	// NOTE: This is different from legacy pipeline!!
	badRecords := 0
	for count := 0; count < 2100; count++ {
		err = w.Next()
		if err != nil {
			if err == io.EOF {
				// We expect EOF.
				break
			} else {
				// FYI - something like 1/5000 logs typically have these errors.
				// But they may be associated with specific bad machines.
				//
				// TODO - when this is "missing snaplog header", it is usually but
				// not always unrecoverable.  This is a tiny fraction - 200 / 20M.
				//
				// When we see "missing end of header" or
				// "truncated", then they are usually singletons, so either we
				// recover or perhaps we get EOF immediately.
				metrics.TestCount.With(prometheus.Labels{
					"table": n.TableName(), "type": "w.Next"}).Inc()
				log.Printf("w.Next error: %s processing snap %d from %s from %s\n",
					err, count, testName, meta["filename"])
				// This could either be a bad record, or EOF.
				badRecords++
				if badRecords > 10 {
					metrics.TestCount.With(prometheus.Labels{
						"table": n.TableName(), "type": "10 bad"}).Inc()
					// TODO - don't see this in the logs on 5/4.  Not sure why.
					log.Printf("w.Next 10 bad processing snapshots from %s from %s\n",
						testName, meta["filename"])
					return nil
				}
				continue
			}
		}
		// HACK - just to see how expensive the Values() call is...
		// parse every 10th snapshot.
		if count%10 == 0 {
			// Note: read and discard the values by not saving the Web100ValueMap.
			err := w.SnapshotValues(schema.Web100ValueMap{})
			if err != nil {
				metrics.TestCount.With(prometheus.Labels{
					"table": n.TableName(), "type": "w.Values"}).Inc()
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
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": testType}).Inc()
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
