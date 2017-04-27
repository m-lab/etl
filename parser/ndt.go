package parser

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/web100"

	"github.com/m-lab/etl/etl"
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
		log.Printf("Ignoring non-snaplog file: %s\n", testName)
		return nil
	}
	tmpFile := fmt.Sprintf("%s/%s", n.tmpDir, path.Base(testName))
	log.Printf("writing file: %s\n", tmpFile)
	err := ioutil.WriteFile(tmpFile, rawSnapLog, 0644)
	if err != nil {
		return err
	}
	// TODO(dev): log possible remove errors.
	defer os.Remove(tmpFile)

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

	log.Printf("opening web100 snap file: %s\n", tmpFile)
	// Open the file we created above.
	w, err := web100.Open(tmpFile, legacyNames)
	if err != nil {
		return err
	}
	defer w.Close()

	log.Printf("finding last snaplog\n")
	// Find the last web100 snapshot.
	for {
		err = w.Next()
		if err != nil {
			break
		}
	}
	// We expect EOF.
	if err != io.EOF {
		log.Printf("Failed to reach EOF: %s\n", tmpFile)
		return err
	}

	// Extract the values from the last snapshot.
	results, err := w.Values()
	if err != nil {
		return err
	}
	log.Printf("Inserting values from: %s\n", tmpFile)
	return n.inserter.InsertRow(&bq.MapSaver{results})
}

// TODO(dev) TableName should come from initialization params.
func (n *NDTParser) TableName() string {
	return n.tableName
}
