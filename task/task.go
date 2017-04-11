// Package task provides the tracking of state for a single task pushed by the
// external task queue.
//
// The components are:
//  Parsers - process individual tests
//  BigQueryWriter - inserts new test records into BigQuery
//

package task

import (
	"archive/tar"
	"github.com/m-lab/etl/parser"
	"io"
	"io/ioutil"
	"strings"
)

// A Task encapsulates a source of test files (and filenames), a deadline, and a
// responder to indicate when/if the source was successfully processed.
// TODO - add deadline related code.
type Tasker interface {
	// err == io.EOF when there are no further files to process.
	Next() (fn string, data []byte, err error)
	// If error != nil, the Response may have failed.
	Respond(success bool) error
}

type Task struct {
	Tasker
	// Not ideal for this to be so specific, but good enough for now.
	rdr  *tar.Reader
	prsr parser.Parser // Interface doesn't need pointer?
}

// NewTask constructs a task, injecting the tar reader and the parser.
func NewTask(rdr *tar.Reader, prsr parser.Parser) *Task {
	t := new(Task)
	t.rdr = rdr
	t.prsr = prsr
	return t
}

func (tt *Task) Next() (string, []byte, error) {
	h, err := tt.rdr.Next()
	if err != nil {
		return "", nil, err
	}
	if h.Typeflag != tar.TypeReg {
		return h.Name, nil, err
	}
	if strings.HasSuffix(strings.ToLower(h.Name), ".gz") {
		// TODO handle gzip files
		return h.Name, nil, nil
	} else {
		data, err := ioutil.ReadAll(tt.rdr)
		if err != nil {
			return h.Name, nil, err
		}
		return h.Name, data, nil
	}
}

// Caller may synchronize on returned WaitGroup
func (tt *Task) ProcessAllTests() {
	// Read each file from the tar
	for fn, data, err := tt.Next(); err != io.EOF; fn, data, err = tt.Next() {
		if err != nil {
			if err == io.EOF {
				return
			}
		}
		if data == nil {
			// If verbose, log the filename that is skipped.
			continue
		}

		// TODO update table name
		_, err := tt.prsr.HandleTest(fn, "default", data)
		// TODO handle insertion into BQ.
		if err != nil {
			continue
		}
	}
	return
}
