package parser

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/web100"

	"github.com/m-lab/etl/etl"
)

type NDTParser struct {
	etl.Parser
	tmpDir    string
	tableName string
}

// TODO correctly implement Parser interface.
func (n *NDTParser) Parse(meta map[string]bigquery.Value, testName string, rawSnapLog []byte) (interface{}, error) {
	// TODO(prod): do not write to a temporary file; operate on byte array directly.
	// Write rawSnapLog to /mnt/tmpfs.
	tmpFile := fmt.Sprintf("%s/%s", n.tmpDir, testName)
	err := ioutil.WriteFile(tmpFile, rawSnapLog, 0644)
	if err != nil {
		return nil, err
	}
	// TODO(dev): log possible remove errors.
	defer os.Remove(tmpFile)

	// TODO(dev): only do this once.
	// Parse the tcp-kis.txt web100 variable definition file.
	data, err := web100.Asset("tcp-kis.txt")
	if err != nil {
		// Asset missing from build.
		return nil, err
	}
	b := bytes.NewBuffer(data)
	legacyNames, err := web100.ParseWeb100Definitions(b)
	if err != nil {
		return nil, err
	}

	// Open the file we created above.
	w, err := web100.Open(tmpFile, legacyNames)
	if err != nil {
		return nil, err
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
		return nil, err
	}

	// Extract the values from the last snapshot.
	results, err := w.Values()
	if err != nil {
		return nil, err
	}
	return results, nil
}

// TODO(dev) TableName should come from initialization params.
func (n *NDTParser) TableName() string {
	return n.tableName
}
