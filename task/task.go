// Package task provides the tracking of state for a single task pushed by the
// external task queue.
//
// The Task type ...
// TODO(dev) Improve comments and header before merging to dev.
package task

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/storage"
)

type Task struct {
	storage.TarReader        // Tar reader from which to read tests.
	parser.Parser            // Parser to parse the tests.
	bq.Inserter              // provides InsertRows(...)
	table             string // The table to insert rows into, INCLUDING the partition!
}

// NewTask constructs a task, injecting the tar reader and the parser.
func NewTask(rdr storage.TarReader, prsr parser.Parser, inserter bq.Inserter, table string) *Task {
	t := Task{rdr, prsr, inserter, table}
	return &t
}

// Next reads the next test object from the tar file.
// Returns io.EOF when there are no more tests.
// TODO - probably should move this to storage.go.
func (tt *Task) NextTest() (string, []byte, error) {
	h, err := tt.Next()
	if err != nil {
		return "", nil, err
	}
	if h.Typeflag != tar.TypeReg {
		return h.Name, nil, nil
	}
	var data []byte
	if strings.HasSuffix(strings.ToLower(h.Name), "gz") {
		// TODO add unit test
		zipReader, err := gzip.NewReader(tt)
		if err != nil {
			return h.Name, nil, err
		}
		defer zipReader.Close()
		data, err = ioutil.ReadAll(zipReader)
	} else {
		data, err = ioutil.ReadAll(tt)
	}
	if err != nil {
		return h.Name, nil, err
	}
	return h.Name, data, nil
}

// ProcessAllTests loops through all the tests in a tar file, calls the
// injected parser to parse them, and inserts them into bigquery (not yet implemented).
func (tt *Task) ProcessAllTests() {
	// TODO(dev) better error handling
	defer tt.Flush(5*time.Second)
	tests := 0
	inserts := 0
	// Read each file from the tar
	for fn, data, err := tt.NextTest(); err != io.EOF; fn, data, err = tt.NextTest() {
		tests += 1
		if err != nil {
			if err == io.EOF {
				return
			}
			// TODO(dev) Handle this error properly!
			log.Printf("%v", err)
			continue
		}
		if data == nil {
			// If verbose, log the filename that is skipped.
			continue
		}

		row, err := tt.Parser.Parse(fn, tt.table, data)
		if err != nil {
			log.Printf("%v", err)
			// TODO(dev) Handle this error properly!
			continue
		}
		// TODO(dev) Aggregate rows into single insert request, here
		// or in Inserter.
		inserts += 1
		err = tt.InsertRows(row, 5*time.Second)
		if err != nil {
			log.Printf("%v", err)
			// Handle this error properly!
		}
	}
	// TODO - make this debug or remove
	log.Printf("%d tests, %d inserts", tests, inserts)
	err := tt.Flush(5*time.Second)
	if err != nil {
		log.Printf("%v", err)
	}
	return
}
