package parser

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"regexp"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/etl/web100"
)

var (
	TmpDir = "/mnt/tmpfs"
)

//=========================================================================
// Test name parsing related stuff.
//=========================================================================

const dateTime = `^(?P<date>\d{8})T(?P<time>[012]\d:[0-5]\d:\d{2}\.\d{9})Z_`
const address = `(?P<address>.*)`
const suffix = `\.(?P<suffix>[a-z2].*)$`

var (
	// Pattern for any valid test file name
	testFilePattern = regexp.MustCompile(dateTime + address + suffix)

	startPattern = regexp.MustCompile(dateTime)
	endPattern   = regexp.MustCompile(suffix)
)

// testInfo contains all the fields from a valid NDT test file name.
type testInfo struct {
	Date    string // The date field from the test file name
	Time    string // The time field
	Address string // The remote address field
	Suffix  string // The filename suffix
}

func ParseNDTFileName(path string) (*testInfo, error) {
	fields := testFilePattern.FindStringSubmatch(path)

	if fields == nil {
		if !startPattern.MatchString(path) {
			return nil, errors.New("Path should begin with yyyymmddThh:mm:ss...Z:" + path)
		}
		if !endPattern.MatchString(path) {
			return nil, errors.New("Path should end in \\.[a-z2].*: " + path)
		}
		return nil, errors.New("Invalid test path: " + path)
	}
	return &testInfo{fields[1], fields[2], fields[3], fields[4]}, nil
}

//=========================================================================
// NDTParser stuff.
//=========================================================================

type fileInfoAndData struct {
	fn   string
	info testInfo
	data []byte
}

// This is the parse info from the .meta file.
type metaFileData struct {
}

type NDTParser struct {
	inserter etl.Inserter
	// TODO(prod): eliminate need for tmpfs.
	tmpDir string

	timestamp string // The unique timestamp common across all files in current batch.
	c2s       *fileInfoAndData
	s2c       *fileInfoAndData

	meta *metaFileData
}

func NewNDTParser(ins etl.Inserter) *NDTParser {
	return &NDTParser{inserter: ins, tmpDir: TmpDir}
}

// ParseAndInsert extracts the last snaplog from the given raw snap log.
// Writes rawSnapLog to /mnt/tmpfs.
// TODO(dev) This is getting big and ugly and needs to be refactored.
// TODO(prod): do not write to a temporary file; operate on byte array directly.
func (n *NDTParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, content []byte) error {
	// Scraper adds files to tar file in lexical order.  This groups together all
	// files in a single test, but the order of the files varies because of port number.
	// If c2s or s2c files precede the .meta file, we must cache them, and process
	// them only when the .meta file has been processed.
	// If we detect a new prefix before getting all three, we should log appropriate
	// information about that, and possibly place error rows in the BQ table.
	// TODO(prod) Ensure that archive files are also date sorted.
	info, err := ParseNDTFileName(testName)
	if err != nil {
		// TODO - should log and count this.
		return err
	}

	if info.Time != n.timestamp {
		// All files are processed ASAP.  However, if there is ONLY
		// a data file, or ONLY a meta file, we should log and count that.
		// TODO Log/count if we never see one of the three files.
		n.timestamp = info.Time
		n.s2c = nil
		n.c2s = nil
		n.meta = nil
	}

	var testType string
	switch info.Suffix {
	case "c2s_snaplog":
		if n.c2s != nil {
			// TODO - report collisions
		}
		testType = "c2s"
		n.c2s = &fileInfoAndData{testName, *info, content}
		if n.meta != nil {
			return n.processTest(meta, n.c2s.fn, testType, n.c2s.data)
		}
	case "s2c_snaplog":
		if n.s2c != nil {
			// TODO - report collisions
		}
		testType = "s2c"
		n.s2c = &fileInfoAndData{testName, *info, content}
		if n.meta != nil {
			return n.processTest(meta, n.s2c.fn, testType, n.s2c.data)
		}
	case "meta":
		if n.meta != nil {
			// TODO - report collisions
		}
		n.processMeta(&fileInfoAndData{testName, *info, content})
		testType = "meta"
		var err error
		if n.c2s != nil {
			err = n.processTest(meta, n.c2s.fn, testType, n.c2s.data)
		}
		if n.s2c != nil {
			s2cErr := n.processTest(meta, n.s2c.fn, testType, n.s2c.data)
			if s2cErr != nil {
				// TODO - also handle case of errors on both files
				return s2cErr
			}
		}
		return err
	case "c2s_ndttrace":
	case "s2c_ndttrace":
	case "cputime":
	default:
		metrics.TestCount.WithLabelValues(
			n.TableName(), "unknown", info.Suffix).Inc()
		return errors.New("Unknown test suffix: " + info.Suffix)
	}

	return nil
}

// Process the meta test data.
func (n *NDTParser) processMeta(infoAndData *fileInfoAndData) error {
	// TODO(dev) - actually parse the meta data and use it!
	n.meta = &metaFileData{}
	return nil
}

// processMeta should already have been called and produced valid data in n.meta
func (n *NDTParser) processTest(meta map[string]bigquery.Value, testName string, testType string, rawSnapLog []byte) error {

	// NOTE: this file size threshold and the number of simultaneous workers
	// defined in etl_worker.go must guarantee that all files written to
	// /mnt/tmpfs will fit.
	if len(rawSnapLog) > 10*1024*1024 {
		metrics.TestCount.WithLabelValues(
			n.TableName(), testType, ">10MB").Inc()
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
		// TODO - Use separate counter, since this is not unique across
		// the test.
		metrics.TestCount.WithLabelValues(
			n.TableName(), testType, "<32KB").Inc()
		log.Printf("Note: small rawSnapLog: %d, %s\n",
			len(rawSnapLog), testName)
	}
	if len(rawSnapLog) == 4096 {
		// TODO - Use separate counter, since this is not unique across
		// the test.
		metrics.TestCount.WithLabelValues(
			n.TableName(), testType, "4KB").Inc()
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
		metrics.TestCount.WithLabelValues(
			n.TableName(), testType, "web100.Asset").Inc()
		log.Printf("web100.Asset error: %s processing %s from %s\n",
			err, testName, meta["filename"].(string))
		return nil
	}
	b := bytes.NewBuffer(data)

	// These unfortunately nest.
	metrics.WorkerState.WithLabelValues("parse-def").Inc()
	defer metrics.WorkerState.WithLabelValues("parse-def").Dec()
	legacyNames, err := web100.ParseWeb100Definitions(b)
	if err != nil {
		metrics.TestCount.WithLabelValues(
			n.TableName(), testType, "web100.ParseDef").Inc()
		log.Printf("web100.ParseDef error: %s processing %s from %s\n",
			err, testName, meta["filename"])
		return nil
	}

	// TODO(prod): do not write to a temporary file; operate on byte array directly.
	// Write rawSnapLog to /mnt/tmpfs.
	tmpFile, err := ioutil.TempFile(n.tmpDir, "snaplog-")
	if err != nil {
		metrics.TestCount.WithLabelValues(
			n.TableName(), testType, "TmpFile").Inc()
		log.Printf("Failed to create tmpfile for: %s, when processing: %s\n",
			testName, meta["filename"])
		return nil
	}

	c := 0
	for count := 0; count < len(rawSnapLog); count += c {
		c, err = tmpFile.Write(rawSnapLog)
		if err != nil {
			metrics.TestCount.WithLabelValues(
				n.TableName(), testType, "tmpFile.Write").Inc()
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
		metrics.TestCount.WithLabelValues(
			n.TableName(), testType, "web100.Open").Inc()
		// These are mostly "could not parse /proc/web100/header",
		// with some "file read/write error C"
		log.Printf("web100.Open error: %s processing %s from %s\n",
			err, testName, meta["filename"])
		return nil
	}
	defer w.Close()

	// Seek to either last snapshot, or snapshot 2100 if there are more than that.
	if !seek(w, n.TableName(), meta["filename"].(string), testName, testType) {
		// TODO - is there a previous snapshot we can use???
		return nil
	}

	return n.getAndInsertValues(w, meta["filename"].(string), testName, testType)
}

// Find the "last" web100 snapshot.
// Returns true if valid snapshot found.
func seek(w *web100.Web100, tableName string, tarFileName string, testName string, testType string) bool {
	metrics.WorkerState.WithLabelValues("seek").Inc()
	defer metrics.WorkerState.WithLabelValues("seek").Dec()
	// Limit to parsing only up to 2100 snapshots.
	// NOTE: This is different from legacy pipeline!!
	badRecords := 0
	for count := 0; count < 2100; count++ {
		err := w.Next()
		if err != nil {
			if err == io.EOF {
				// We expect EOF.
				break
			} else {
				// FYI - something like 1/5000 logs typically have these errors.
				// But they may be associated with specific bad machines.
				//
				// When we see "missing end of header" or
				// "truncated", then they are usually singletons, so either we
				// recover or perhaps we get EOF immediately.
				// TODO - Use separate counter, since this is not unique across
				// the test.
				metrics.TestCount.WithLabelValues(
					tableName, testType, "w.Next").Inc()
				log.Printf("w.Next error: %s processing snap %d from %s from %s\n",
					err, count, testName, tarFileName)
				// This could either be a bad record, or EOF.
				badRecords++
				if badRecords > 10 {
					// TODO - Use separate counter, since this is not unique across
					// the test.
					metrics.TestCount.WithLabelValues(
						tableName, testType, "badRecords").Inc()
					// TODO - don't see this in the logs on 5/4.  Not sure why.
					log.Printf("w.Next 10 bad processing snapshots from %s from %s\n",
						testName, tarFileName)
					// TODO - is there a previous snapshot we can use???
					return false
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
				// TODO - Use separate counter, since this is not unique across
				// the test.
				metrics.TestCount.WithLabelValues(
					tableName, testType, "w.Values").Inc()
			}
		}
	}
	return true
}

func (n *NDTParser) getAndInsertValues(w *web100.Web100, tarFileName string, testName string, testType string) error {
	// Extract the values from the last snapshot.
	metrics.WorkerState.WithLabelValues("parse").Inc()
	defer metrics.WorkerState.WithLabelValues("parse").Dec()

	snapValues := schema.Web100ValueMap{}
	err := w.SnapshotValues(snapValues)
	if err != nil {
		metrics.TestCount.WithLabelValues(
			n.TableName(), testType, "values-err").Inc()
		log.Printf("Error calling web100 Values() in test %s, when processing: %s\n%s\n",
			testName, tarFileName, err)
		return nil
	}

	// TODO(prod) Write a row with this data, even if the snapshot parsing fails?
	connSpec := schema.Web100ValueMap{}
	w.ConnectionSpec(connSpec)

	results := schema.NewWeb100MinimalRecord(
		w.LogVersion(), w.LogTime(),
		(map[string]bigquery.Value)(connSpec),
		(map[string]bigquery.Value)(snapValues))

	err = n.inserter.InsertRow(&bq.MapSaver{fixValues(results)})
	if err != nil {
		metrics.TestCount.WithLabelValues(
			n.TableName(), testType, "insert-err").Inc()
		// TODO: This is an insert error, that might be recoverable if we try again.
		return err
	} else {
		metrics.TestCount.WithLabelValues(
			n.TableName(), testType, "ok").Inc()
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
