// Package task provides the tracking of state for a single task pushed by the
// external task queue.
//
// The Task type ...
// TODO(dev) Improve comments and header before merging to dev.
package task

import (
	"archive/tar"
	"compress/gzip"
	"github.com/m-lab/etl/parser"
	"io"
	"io/ioutil"
	"strings"
)

type Task struct {
	table string        // The table to insert rows into, INCLUDING the partition!
	rdr   *tar.Reader   // Tar reader from which to read tests.
	prsr  parser.Parser // Parser to parse the tests.
}

// NewTask constructs a task, injecting the tar reader and the parser.
func NewTask(rdr *tar.Reader, prsr parser.Parser, table string) *Task {
	t := new(Task)
	t.table = table
	t.rdr = rdr
	t.prsr = prsr
	return t
}

// Next reads the next test object from the tar file.
// Returns io.EOF when there are no more tests.
func (tt *Task) Next() (string, []byte, error) {
	h, err := tt.rdr.Next()
	if err != nil {
		return "", nil, err
	}
	if h.Typeflag != tar.TypeReg {
		return h.Name, nil, nil
	}
	var data []byte
	if strings.HasSuffix(strings.ToLower(h.Name), ".gz") {
		// TODO add unit test
		zipReader, err := gzip.NewReader(tt.rdr)
		if err != nil {
			return h.Name, nil, err
		}
		defer zipReader.Close()
		data, err = ioutil.ReadAll(zipReader)
	} else {
		data, err = ioutil.ReadAll(tt.rdr)
	}
	if err != nil {
		return h.Name, nil, err
	}
	return h.Name, data, nil
}

// ProcessAllTests loops through all the tests in a tar file, calls the
// injected parser to parse them, and inserts them into bigquery (not yet implemented).
func (tt *Task) ProcessAllTests() {
	// Read each file from the tar
	for fn, data, err := tt.Next(); err != io.EOF; fn, data, err = tt.Next() {
		if err != nil {
			if err == io.EOF {
				return
			}
			// TODO(dev) add error handling
			continue
		}
		if data == nil {
			// If verbose, log the filename that is skipped.
			continue
		}

		// TODO update table name
		_, err := tt.prsr.HandleTest(fn, tt.table, data)
		// TODO handle insertion into BQ.
		if err != nil {
			continue
		}
	}
	return
}
