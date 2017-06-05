package parser

import (
	"errors"
	"log"
	"regexp"
	"time"

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
// NDT Test filename parsing related stuff.
//=========================================================================

// TODO - should this be optional?
const dateDir = `^(?P<dir>\d{4}/\d{2}/\d{2}/)?`

// TODO - use time.Parse to parse this part of the filename.
const dateField = `(?P<date>\d{8})`
const timeField = `(?P<time>[012]\d:[0-6]\d:\d{2}\.\d{1,10})`
const address = `(?P<address>.*)`
const suffix = `(?P<suffix>[a-z2].*)`

var (
	// Pattern for any valid test file name
	testFilePattern = regexp.MustCompile(
		"^" + dateDir + dateField + "T" + timeField + "Z_" + address + `\.` + suffix + "$")
	gzTestFilePattern = regexp.MustCompile(
		"^" + dateDir + dateField + "T" + timeField + "Z_" + address + `\.` + suffix + `\.gz$`)

	datePattern = regexp.MustCompile(dateField)
	timePattern = regexp.MustCompile("T" + timeField + "Z_")
	endPattern  = regexp.MustCompile(suffix + `$`)
)

// testInfo contains all the fields from a valid NDT test file name.
type testInfo struct {
	DateDir   string    // Optional leading date yyyy/mm/dd/
	Date      string    // The date field from the test file name
	Time      string    // The time field
	Address   string    // The remote address field
	Suffix    string    // The filename suffix
	Timestamp time.Time // The parsed timestamp, with microsecond resolution
}

func ParseNDTFileName(path string) (*testInfo, error) {
	fields := gzTestFilePattern.FindStringSubmatch(path)

	if fields == nil {
		// Try without trailing .gz
		fields = testFilePattern.FindStringSubmatch(path)
	}
	if fields == nil {
		if !datePattern.MatchString(path) {
			return nil, errors.New("Path should contain yyyymmddT: " + path)
		} else if !timePattern.MatchString(path) {
			return nil, errors.New("Path should contain Thh:mm:ss.ff...Z_: " + path)
		} else if !endPattern.MatchString(path) {
			return nil, errors.New("Path should end in \\.[a-z2].*: " + path)
		}
		return nil, errors.New("Invalid test path: " + path)
	}
	timestamp, err := time.Parse("20060102T15:04:05.999999999Z_", fields[2]+"T"+fields[3]+"Z_")
	if err != nil {
		log.Println(fields[2] + "T" + fields[3] + "   " + err.Error())
		return nil, errors.New("Invalid test path: " + path)
	}
	return &testInfo{fields[1], fields[2], fields[3], fields[4], fields[5], timestamp}, nil
}

//=========================================================================
// NDTParser stuff.
//=========================================================================

type fileInfoAndData struct {
	fn   string
	info testInfo
	data []byte
}

type NDTParser struct {
	inserter etl.Inserter
	// TODO(prod): eliminate need for tmpfs.
	tmpDir string

	timestamp string // The unique timestamp common across all files in current batch.
	time      time.Time

	// TODO(dev) Sometimes NDT writes multiple copies of c2s or s2c.  We need to save them
	// and use only the one identified in meta file.
	c2s *fileInfoAndData
	s2c *fileInfoAndData

	metaFile *MetaFileData
}

func NewNDTParser(ins etl.Inserter) *NDTParser {
	return &NDTParser{inserter: ins, tmpDir: TmpDir}
}

// ParseAndInsert extracts the last snaplog from the given raw snap log.
// Writes rawSnapLog to /mnt/tmpfs.
// TODO(prod): do not write to a temporary file; operate on byte array directly.
func (n *NDTParser) ParseAndInsert(taskInfo map[string]bigquery.Value, testName string, content []byte) error {
	// Scraper adds files to tar file in lexical order.  This groups together all
	// files in a single test, but the order of the files varies because of port number.
	// If c2s or s2c files precede the .meta file, we must cache them, and process
	// them only when the .meta file has been processed.
	// If we detect a new prefix before getting all three, we should log appropriate
	// information about that, and possibly place error rows in the BQ table.
	// TODO(prod) Ensure that archive files are also date sorted.
	info, err := ParseNDTFileName(testName)
	if err != nil {
		metrics.TestCount.WithLabelValues(
			n.TableName(), "unknown", "bad filename").Inc()
		// TODO - should log and count this.
		log.Println(err)
		return nil
	}

	taskFileName := taskInfo["filename"].(string)

	if info.Time != n.timestamp {
		// All files are processed ASAP.  However, if there is ONLY
		// a data file, or ONLY a meta file, we have to process the
		// test files anyway.
		n.handleAnomolies(taskFileName)

		n.timestamp = info.Time
		n.s2c = nil
		n.c2s = nil
		n.metaFile = nil
	}

	switch info.Suffix {
	case "c2s_snaplog":
		if n.c2s != nil {
			metrics.WarningCount.WithLabelValues(
				n.TableName(), "c2s", "timestamp collision").Inc()
			log.Printf("Collision: %s and %s\n", n.c2s.fn, testName)
		}
		n.c2s = &fileInfoAndData{testName, *info, content}
		n.processTest(taskFileName, n.c2s, "c2s")
	case "s2c_snaplog":
		if n.s2c != nil {
			metrics.WarningCount.WithLabelValues(
				n.TableName(), "s2c", "timestamp collision").Inc()
			log.Printf("Collision: %s and %s\n", n.s2c.fn, testName)
		}
		n.s2c = &fileInfoAndData{testName, *info, content}
		n.processTest(taskFileName, n.s2c, "s2c")
	case "meta":
		if n.metaFile != nil {
			metrics.WarningCount.WithLabelValues(
				n.TableName(), "meta", "timestamp collision").Inc()
		}
		n.metaFile = ProcessMetaFile(
			n.TableName(), n.inserter.TableSuffix(), testName, content)
		if n.c2s != nil {
			n.processTest(taskFileName, n.c2s, "c2s")
		}
		if n.s2c != nil {
			n.processTest(taskFileName, n.s2c, "s2c")
		}
	case "c2s_ndttrace":
	case "s2c_ndttrace":
	case "cputime":
	default:
		metrics.TestCount.WithLabelValues(
			n.TableName(), "unknown", "unknown suffix").Inc()
		return errors.New("Unknown test suffix: " + info.Suffix)
	}

	return nil
}

// In the case that we are missing one or more files, report and handle gracefully.
func (n *NDTParser) handleAnomolies(taskFileName string) {
	switch {
	case n.metaFile == nil:
		n.metaFile = &MetaFileData{} // Hack to allow processTest to run.
		if n.s2c != nil {
			metrics.WarningCount.WithLabelValues(
				n.TableName(), "s2c", "no meta").Inc()
			// TODO enable this once noise is reduced.
			// log.Printf("No meta: %s %s\n", taskFileName, n.s2c.fn)
			n.processTest(taskFileName, n.s2c, "s2c")
		}
		if n.c2s != nil {
			metrics.WarningCount.WithLabelValues(
				n.TableName(), "c2s", "no meta").Inc()
			// TODO enable this once noise is reduced.
			// log.Printf("No meta: %s %s\n", taskFileName, n.c2s.fn)
			n.processTest(taskFileName, n.c2s, "c2s")
		}
		if n.s2c == nil && n.c2s == nil {
			metrics.WarningCount.WithLabelValues(
				n.TableName(), "test", "no meta,c2s,s2c").Inc()
		}
	// Now meta is non-nil
	case n.s2c == nil && n.c2s == nil:
		// Meta file but no test file.
		metrics.WarningCount.WithLabelValues(
			n.TableName(), "meta", "no tests").Inc()
		log.Printf("No tests: %s %s\n", taskFileName, n.metaFile.TestName)
	// Now meta and at least one test are non-nil
	default:
		// We often only get meta + one, so no
		// need to log this.
	}
}

// processTest digests a single s2c or c2s test, and writes a row to the Inserter.
// ProcessMetaFile should already have been called and produced valid data in n.metaFile
// However, we often get s2c and c2s without corresponding meta files.  When this happens,
// we proceed with an empty metaFile.
func (n *NDTParser) processTest(taskFileName string, test *fileInfoAndData, testType string) {
	if n.metaFile == nil {
		// Defer processing until we get the meta file.
		return
	}

	// NOTE: this file size threshold and the number of simultaneous workers
	// defined in etl_worker.go must guarantee that all files written to
	// /mnt/tmpfs will fit.
	if len(test.data) > 10*1024*1024 {
		metrics.ErrorCount.WithLabelValues(
			n.TableName(), testType, ">10MB").Inc()
		log.Printf("Ignoring oversize snaplog: %d, %s\n",
			len(test.data), test.fn)
		metrics.FileSizeHistogram.WithLabelValues(
			"huge").Observe(float64(len(test.data)))
		return
	} else {
		// Record the file size.
		metrics.FileSizeHistogram.WithLabelValues(
			"normal").Observe(float64(len(test.data)))
	}

	if len(test.data) < 16*1024 {
		metrics.WarningCount.WithLabelValues(
			n.TableName(), testType, "<16KB").Inc()
		log.Printf("Note: small rawSnapLog: %d, %s\n",
			len(test.data), test.fn)
	}
	if len(test.data) == 4096 {
		metrics.WarningCount.WithLabelValues(
			n.TableName(), testType, "4KB").Inc()
	}

	metrics.WorkerState.WithLabelValues("ndt").Inc()
	defer metrics.WorkerState.WithLabelValues("ndt").Dec()

	n.getAndInsertValues(taskFileName, test, testType)
}

func (n *NDTParser) getAndInsertValues(taskFileName string, test *fileInfoAndData, testType string) {
	// Extract the values from the last snapshot.
	metrics.WorkerState.WithLabelValues("parse").Inc()
	defer metrics.WorkerState.WithLabelValues("parse").Dec()

	snaplog, err := web100.NewSnapLog(test.data)
	if err != nil {
		metrics.ErrorCount.WithLabelValues(
			n.TableName(), testType, "snaplog failure").Inc()
		return
	}

	err = snaplog.ValidateSnapshots()
	if err != nil {
		log.Printf("ValidateSnapshots failed for %s, when processing: %s\n%s\n",
			test.fn, taskFileName, err)
		metrics.WarningCount.WithLabelValues(
			n.TableName(), testType, "validate failed").Inc()
	}

	// HACK - just to see how expensive the Values() call is...
	// parse ALL the snapshots.
	for count := 0; count < snaplog.SnapCount() && count < 2100; count++ {
		snap, err := snaplog.Snapshot(count)
		if err != nil {
			metrics.TestCount.WithLabelValues(
				n.TableName(), testType, "snapshot failure").Inc()
			return
		}
		// Proper sizing avoids evacuate, saving about 20%, excluding BQ code.
		snap.SnapshotValues(schema.EmptySnap())
		if err != nil {
			metrics.ErrorCount.WithLabelValues(
				n.TableName(), testType, "snapValues failure").Inc()
			return
		}
	}

	last := snaplog.SnapCount() - 1
	if last > 2100 {
		last = 2100
	}
	snap, err := snaplog.Snapshot(last)
	if err != nil {
		// TODO - Use separate counter, since this is not unique across
		// the test.
		metrics.TestCount.WithLabelValues(
			n.TableName(), testType, "final snapshot failure").Inc()
		return
	}
	snapValues := schema.EmptySnap()
	snap.SnapshotValues(snapValues)
	if err != nil {
		// TODO - Use separate counter, since this is not unique across
		// the test.
		metrics.TestCount.WithLabelValues(
			n.TableName(), testType, "final snapValues failure").Inc()
		log.Printf("Error calling SnapshotValues() in test %s, when processing: %s\n%s\n",
			test.fn, taskFileName, err)
		return
	}

	// TODO(prod) Write a row with this data, even if the snapshot parsing fails?
	nestedConnSpec := make(schema.Web100ValueMap, 6)
	snaplog.ConnectionSpecValues(nestedConnSpec)

	results := schema.NewWeb100MinimalRecord(
		snaplog.Version, int64(snaplog.LogTime),
		(map[string]bigquery.Value)(nestedConnSpec),
		(map[string]bigquery.Value)(snapValues))

	results["test_id"] = test.fn
	results["task_filename"] = taskFileName
	// This is the timestamp parsed from the filename.
	lt, err := test.info.Timestamp.MarshalText()
	if err != nil {
		log.Println(err)
		metrics.ErrorCount.WithLabelValues(
			n.inserter.TableBase(), "log_time marshal error").Inc()
	} else {
		results["log_time"] = string(lt)
	}
	now, err := time.Now().MarshalText()
	if err != nil {
		log.Println(err)
		metrics.ErrorCount.WithLabelValues(
			n.inserter.TableBase(), "parse_time marshal error").Inc()
	} else {
		results["parse_time"] = string(now)
	}

	connSpec := schema.EmptyConnectionSpec()
	n.metaFile.PopulateConnSpec(connSpec)
	switch testType {
	case "c2s":
		connSpec.SetInt64("data_direction", CLIENT_TO_SERVER)
	case "s2c":
		connSpec.SetInt64("data_direction", SERVER_TO_CLIENT)
	default:
	}
	results["connection_spec"] = connSpec

	fixValues(results)
	err = n.inserter.InsertRow(&bq.MapSaver{results})
	if err != nil {
		metrics.TestCount.WithLabelValues(
			n.TableName(), testType, "insert-err").Inc()
		// TODO: This is an insert error, that might be recoverable if we try again.
		log.Println("insert-err: " + err.Error())
		return
	} else {
		metrics.TestCount.WithLabelValues(
			n.TableName(), testType, "ok").Inc()
		return
	}
}

func (n *NDTParser) TableName() string {
	return n.inserter.TableBase()
}

// fixValues updates web100 log values that need post-processing fix-ups.
// TODO(dev): does this only apply to NDT or is NPAD also affected?
func fixValues(r schema.Web100ValueMap) {
	logEntry := r.GetMap([]string{"web100_log_entry"})

	// Always substitute, unless for some reason the snapshot value is missing.
	logEntry.SubstituteString(false, []string{"connection_spec", "local_ip"},
		[]string{"snap", "LocalAddress"})
	logEntry.SubstituteString(false, []string{"connection_spec", "remote_ip"},
		[]string{"snap", "RemAddress"})
	logEntry.SubstituteInt64(false, []string{"connection_spec", "local_af"},
		[]string{"snap", "LocalAddressType"})

	// Only substitute these if they are null, (because the .meta file was missing).
	r.SubstituteString(true, []string{"connection_spec", "server_ip"},
		[]string{"web100_log_entry", "connection_spec", "local_ip"})
	r.SubstituteInt64(true, []string{"connection_spec", "server_af"},
		[]string{"web100_log_entry", "connection_spec", "local_af"})
	r.SubstituteString(true, []string{"connection_spec", "client_ip"},
		[]string{"web100_log_entry", "connection_spec", "remote_ip"})
	r.SubstituteInt64(true, []string{"connection_spec", "client_af"},
		[]string{"web100_log_entry", "connection_spec", "local_af"})

	snap := logEntry.GetMap([]string{"snap"})
	start, ok := snap.GetInt64([]string{"StartTimeStamp"})
	if ok {
		start = 1000000 * start
		usec, ok := snap.GetInt64([]string{"StartTimeUsec"})
		if ok {
			start += usec
		}
		snap.SetInt64("StartTimeStamp", start)
	}

	// Fix local_af ?
	//  - web100_log_entry.connection_spec.local_af: IPv4 = 0, IPv6 = 1.
}
