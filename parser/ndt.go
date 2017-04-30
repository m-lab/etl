package parser

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/web100"

	"github.com/m-lab/etl/etl"
)

const (
	// TODO(prod): eliminate need for tmpfs.
	tmpDir = "/mnt/tmpfs"
)

type NDTParser struct {
	inserter  etl.Inserter
	tmpDir    string
	tableName string
}

func NewNDTParser(ins etl.Inserter, tableName string) *NDTParser {
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
	tmpFile, err := ioutil.TempFile(n.tmpDir, "snaplog-")
	if err != nil {
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
	return n.tableName
}
