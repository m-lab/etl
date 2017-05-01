package parser

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/web100"
)

type NDTParser struct {
	inserter  etl.Inserter
	tableName string
	// TODO(prod): eliminate need for tmpfs.
	tmpDir string
}

func NewNDTParser(ins etl.Inserter, tableName string) *NDTParser {
	return &NDTParser{ins, tableName, "/mnt/tmpDir"}
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

	// Record the file size.
	metrics.FileSizeHistogram.Observe(float64(len(rawSnapLog)))

	tmpFile, err := ioutil.TempFile(n.tmpDir, "snaplog-")
	if err != nil {
		metrics.TestCount.With(prometheus.Labels{
			"table": n.TableName(), "type": "no-tmp"}).Inc()
		log.Printf("Failed to create tmpfile for: %s\n", testName)
		return err
	}

	// Record the file size.
	metrics.FileSizeHistogram.Observe(float64(len(rawSnapLog)))
	c := 0
	for count := 0; count < len(rawSnapLog); count += c {
		c, err = tmpFile.Write(rawSnapLog)
		if err != nil {
			return err
		}
	}
	tmpFile.Sync()
	// TODO(dev): log possible remove errors.
	defer os.Remove(tmpFile.Name())

	// TODO(dev): only do this once.
	// Parse the tcp-kis.txt web100 variable definition file.
	data, err := web100.Asset("tcp-kis.txt")
	if err != nil {
		// Asset missing from build.
		return err
	}
	b := bytes.NewBuffer(data)
	legacyNames, err := web100.ParseWeb100Definitions(b)
	if err != nil {
		return err
	}

	// Open the file we created above.
	w, err := web100.Open(tmpFile.Name(), legacyNames)
	if err != nil {
		return err
	}
	defer w.Close()

	// Find the last web100 snapshot.
	for {
		err = w.Next()
		if err != nil {
			break
		}
	}
	// We expect EOF.
	if err != io.EOF {
		log.Printf("Failed to reach EOF: %s\n", tmpFile.Name())
		return err
	}

	// Extract the values from the last snapshot.
	results, err := w.Values()
	if err != nil {
		return err
	}
	return n.inserter.InsertRow(&bq.MapSaver{results})
}

// TODO(dev) TableName should come from initialization params.
func (n *NDTParser) TableName() string {
	return n.inserter.TableName()
}
