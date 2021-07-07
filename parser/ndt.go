package parser

/******************************************************************************
 NDTParser has some unique features (among current parsers)
 1. It groups c2s, s2c and meta tests so and processes them together.
 2. It uses map[string]bigquery.Value for rows.
 3. It uses the Web100 file format to discover the field names and
   populate the snapshots.

 This version uses the new Base parser to handle row buffering,
 annotation, and insertion.  The implementation of the Annotatable
 interface is a bit clunky because it has to traverse the map hierarchy.

******************************************************************************/

import (
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/m-lab/go/logx"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/annotation-service/api"
	v2as "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/etl/annotation"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/etl/web100"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// NDTEstimateBW flag indicates if we should run BW estimation code
	// and annotate rows.
	NDTEstimateBW, _ = strconv.ParseBool(os.Getenv("NDT_ESTIMATE_BW"))
)

const (
	// Some snaplogs are very large, and we don't want to parse the entire
	// snaplog, when there is no value.  However, although the nominal test
	// length is 10 seconds, many tests collect snaplogs up to about 13 seconds,
	// to avoid race conditions in the collection.  So, we will process up
	// to 2800 snapshots, which corresponds to 14 seconds, and should be long
	// after the data transfer has completed.
	//
	// TODO - in future, we should probably detect when the connection state changes
	// from established, as there is little reason to parse snapshots beyond that
	// point.
	minNumSnapshots = 1600 // If fewer than this, then set anomalies.num_snaps
	maxNumSnapshots = 2800 // If more than this, truncate, and set anomolies.num_snaps
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

// TestInfo contains all the fields from a valid NDT test file name.
type TestInfo struct {
	DateDir   string    // Optional leading date yyyy/mm/dd/
	Date      string    // The date field from the test file name
	Time      string    // The time field
	Address   string    // The remote address field
	Suffix    string    // The filename suffix
	Timestamp time.Time // The parsed timestamp, with microsecond resolution
}

// ParseNDTFileName parses the name of a tar or tgz file containing NDT test data.
func ParseNDTFileName(path string) (*TestInfo, error) {
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
	return &TestInfo{fields[1], fields[2], fields[3], fields[4], fields[5], timestamp}, nil
}

//=========================================================================
// NDTParser stuff.
//=========================================================================

type fileInfoAndData struct {
	fn   string
	info TestInfo
	data []byte
}

// NDTParser implements the Parser interface for NDT.
type NDTParser struct {
	Base

	// These will be non-empty iff a test group is pending.
	taskFileName string // The tar file containing these tests.
	timestamp    string // The unique timestamp common across all files in current batch.

	// These are non-null when the respective files have been read (within a timestamp group)
	c2s *fileInfoAndData
	s2c *fileInfoAndData

	metaFile *MetaFileData
}

// NewNDTParser returns a new NDT parser.
// Caller may include an annotator.  If not provided, the default annotator is used.
// TODO - clean up the vararg annotator hack once it is standard in all parsers.
func NewNDTParser(ins etl.Inserter, annotator ...v2as.Annotator) *NDTParser {
	bufSize := etl.NDT.BQBufferSize()
	var ann v2as.Annotator
	if len(annotator) > 0 && annotator[0] != nil {
		ann = annotator[0]
	} else {
		ann = v2as.GetAnnotator(annotation.BatchURL)
	}

	return &NDTParser{Base: *NewBase(ins, bufSize, ann)}
}

// These functions implement the etl.Parser interface.

// TaskError returns non-nil if more than 10% of row inserts failed.
func (n *NDTParser) TaskError() error {
	if n.Inserter.Committed() < 10*n.Inserter.Failed() {
		log.Printf("Warning: high row commit errors: %d / %d\n",
			n.Inserter.Accepted(), n.Inserter.Failed())
		return errors.New("too many insertion failures")
	}
	return nil
}

// Flush completes processing of final task group, if any, and flushes
// buffer to BigQuery.
func (n *NDTParser) Flush() error {
	// Process the last group (if it exists) before flushing the inserter.
	if n.timestamp != "" {
		n.processGroup()
	}

	return n.Base.AnnotateAndFlush(n.TableBase())
}

// TableName returns the base of the bq table inserter target.
func (n *NDTParser) TableName() string {
	return n.TableBase()
}

// IsParsable returns the canonical test type and whether to parse data.
func (n *NDTParser) IsParsable(testName string, data []byte) (string, bool) {
	info, err := ParseNDTFileName(testName)
	if err != nil {
		return "unknown", false
	}
	switch info.Suffix {
	// Parsable types:
	case "c2s_snaplog":
		fallthrough // b/c this is parsable
	case "s2c_snaplog":
		fallthrough // b/c this is parsable
	case "meta":
		return info.Suffix, true
	// Unparsable types:
	case "c2s_ndttrace":
		fallthrough // b/c this is unparsable
	case "s2c_ndttrace":
		fallthrough // b/c this is unparsable
	case "cputime":
		return info.Suffix, false
	}
	// All other cases.
	metrics.TestCount.WithLabelValues(
		n.TableName(), "unknown", "unknown suffix").Inc()
	return "unknown", false
}

// ParseAndInsert extracts the last snaplog from the given raw snap log.
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
		log.Println(err)
		return nil
	}

	if info.Time != n.timestamp {
		// Handle previous test group before processing new group.
		n.processGroup()

		// Verify that tests are arriving in timestamp order.
		// TODO(prod) Consider moving this up to task.go (or storage.go)
		if info.Time < n.timestamp {
			metrics.ErrorCount.WithLabelValues(
				n.TableName(), "unknown", "TIMESTAMPS OUT OF ORDER").Inc()
			log.Printf("Timestamps out of order in: %s: %s\n",
				n.taskFileName, err)
			panic("Timestamps out of order in tar file")
		}

		n.taskFileName = taskInfo["filename"].(string)
		n.timestamp = info.Time
	} else {
		// Within a group of tests, we expect consistent taskInfo.
		if n.taskFileName != taskInfo["filename"].(string) {
			metrics.TestCount.WithLabelValues(
				n.TableName(), "any", "inconsistent taskFileName").Inc()
		}
	}

	// Because of port number, the c2s, s2c, and meta files may come in
	// any order.  We defer processing until Flush or new test group.
	switch info.Suffix {
	case "c2s_snaplog":
		if n.c2s == nil {
			n.c2s = &fileInfoAndData{testName, *info, content}
		} else {
			// There are occasional collisions between tests that
			// have the same timestamp.
			if (n.c2s.fn + ".gz") == testName {
				// When rsync collects both the original file and
				// the gzipped file, prefer the zipped file, since
				// the unzipped file may be incomplete.
				n.c2s = &fileInfoAndData{testName, *info, content}
			} else if n.c2s.fn == (testName + ".gz") {
				// Unzipped file follows zipped file is unexpected,
				// but harmless. We just ignore the unzipped file.
			} else {
				// Unexpected name collision...
				metrics.WarningCount.WithLabelValues(
					n.TableName(), "c2s", "timestamp collision").Inc()
				log.Printf("Collision: %s and %s\n", n.c2s.fn, testName)
			}
		}
	case "s2c_snaplog":
		if n.s2c == nil {
			n.s2c = &fileInfoAndData{testName, *info, content}
		} else {
			// There are occasional collisions between tests that
			// have the same timestamp.
			if (n.s2c.fn + ".gz") == testName {
				// When rsync collects both the original file and
				// the gzipped file, prefer the zipped file, since
				// the unzipped file may be incomplete.
				n.s2c = &fileInfoAndData{testName, *info, content}
			} else if n.s2c.fn == (testName + ".gz") {
				// Unzipped file follows zipped file is unexpected,
				// but harmless. We just ignore the unzipped file.
			} else {
				// Unexpected name collision...
				metrics.WarningCount.WithLabelValues(
					n.TableName(), "s2c", "timestamp collision").Inc()
				log.Printf("Collision: %s and %s\n", n.s2c.fn, testName)
			}
		}
	case "meta":
		if n.metaFile != nil {
			metrics.WarningCount.WithLabelValues(
				n.TableName(), "meta", "timestamp collision").Inc()
		}
		n.metaFile = ProcessMetaFile(
			n.TableName(), n.TableSuffix(), testName, content)
	default:
		metrics.TestCount.WithLabelValues(
			n.TableName(), "unknown", "unparsable file").Inc()
		return errors.New("Unknown test suffix: " + info.Suffix)
	}

	return nil
}

func (n *NDTParser) reportAnomalies() {
	// Report all groups that are missing files.
	tag := ""
	code := 0
	if n.metaFile != nil {
		tag += "meta, "
		code += 4
	}
	if n.s2c != nil {
		tag += "s2c, "
		code += 2
	}
	if n.c2s != nil {
		tag += "c2s"
		code++
	}
	if code != 7 {
		if tag == "" {
			tag = "Found no files"
		} else {
			tag = "Found only " + tag
		}
		metrics.WarningCount.WithLabelValues(
			n.TableName(), "group", tag).Inc()
		// Logging missing meta file is too spammy.  Should restore this when
		// NDT is fixed.
	}
}

// processGroup processes tests in the current timestamp grouping.
func (n *NDTParser) processGroup() {
	n.reportAnomalies()
	// Now process the tests, with or without meta file.
	if n.s2c != nil {
		n.processTest(n.s2c, "s2c")
	}
	if n.c2s != nil {
		n.processTest(n.c2s, "c2s")
	}

	n.taskFileName = ""
	n.timestamp = ""
	n.s2c = nil
	n.c2s = nil
	n.metaFile = nil
}

// processTest digests a single s2c or c2s test, and writes a row to the Inserter.
// ProcessMetaFile should already have been called and produced valid data in n.metaFile
// However, we often get s2c and c2s without corresponding meta files.  When this happens,
// we proceed with an empty metaFile.
func (n *NDTParser) processTest(test *fileInfoAndData, testType string) {
	// TODO: handle this logic earlier in ParseAndInsert or in IsParsable.
	if len(test.data) > 10*1024*1024 {
		metrics.ErrorCount.WithLabelValues(
			n.TableName(), testType, ">10MB").Inc()
		log.Printf("Ignoring oversize snaplog: %d, %s\n",
			len(test.data), test.fn)
		return
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

	metrics.WorkerState.WithLabelValues(n.TableName(), "ndt").Inc()
	defer metrics.WorkerState.WithLabelValues(n.TableName(), "ndt").Dec()

	n.getAndInsertValues(test, testType)
}

func (n *NDTParser) getDeltas(snaplog *web100.SnapLog, testType string) ([]schema.Web100ValueMap, int) {
	deltas := []schema.Web100ValueMap{}
	deltaFieldCount := 0
	if etl.OmitDeltas {
		return deltas, deltaFieldCount
	}
	snapshotCount := 0
	last := &web100.Snapshot{}
	for count := 0; count < snaplog.SnapCount() && count < maxNumSnapshots; count++ {
		snap, err := snaplog.Snapshot(count)
		if err != nil {
			// TODO - refine label and maybe write a log?
			metrics.TestCount.WithLabelValues(
				n.TableName(), testType, "snapshot failure").Inc()
			return nil, 0
		}
		// Proper sizing avoids evacuate, saving about 20%, excluding BQ code.
		delta := schema.EmptySnap10()
		snap.SnapshotDeltas(last, delta)
		if err != nil {
			metrics.ErrorCount.WithLabelValues(
				n.TableName(), testType, "snapValues failure").Inc()
			return nil, 0
		}

		// Delete the constant fields.
		delete(delta, "TimeStamps")
		delete(delta, "StartTimeStamp")
		delete(delta, "StartTimeUsec")
		delete(delta, "LocalAddress")
		delete(delta, "LocalAddressType")
		delete(delta, "LocalPort")
		delete(delta, "RemAddress")
		delete(delta, "RemPort")
		delete(delta, "SACK")
		// Now ignore delta if the only field that changed is duration.
		if len(delta) == 1 {
			_, ok := delta["Duration"]
			if ok {
				continue
			}
		}
		delta["snapshot_num"] = count
		delta["delta_index"] = snapshotCount
		snapshotCount++
		metrics.DeltaNumFieldsHistogram.WithLabelValues(n.TableName()).
			Observe(float64(len(delta)))

		deltaFieldCount += len(delta)
		deltas = append(deltas, delta)
		last = &snap
	}

	if len(deltas) > 0 {
		// We tag some of the deltas with specific tags, to make them easy
		// to find.  is_last is the first, but more will be added as we work
		// out the most useful tags.
		deltas[len(deltas)-1]["is_last"] = true
	}

	return deltas, deltaFieldCount
}

func (n *NDTParser) getAndInsertValues(test *fileInfoAndData, testType string) {
	// Extract the values from the last snapshot.
	metrics.WorkerState.WithLabelValues(n.TableName(), "ndt-parse").Inc()
	defer metrics.WorkerState.WithLabelValues(n.TableName(), "ndt-parse").Dec()

	if !strings.HasSuffix(test.fn, ".gz") {
		metrics.WarningCount.WithLabelValues(
			n.TableName(), testType, "uncompressed file").Inc()
	}

	// Large allocation here.
	snaplog, err := web100.NewSnapLog(test.data)
	if err != nil {
		metrics.ErrorCount.WithLabelValues(
			n.TableName(), testType, "snaplog failure").Inc()
		log.Printf("Unable to parse snaplog for %s, when processing: %s\n%s\n",
			test.fn, n.taskFileName, err)
		return
	}

	valid := true
	err = snaplog.ValidateSnapshots()
	if err != nil {
		log.Printf("ValidateSnapshots failed for %s, when processing: %s (%s)\n",
			test.fn, n.taskFileName, err)
		metrics.WarningCount.WithLabelValues(
			n.TableName(), testType, "validate failed").Inc()
		// If ValidateSnapshots returns error, it generally means that there
		// is a problem with the last snapshot, typically a truncated file.
		// In most cases, there are still many valid snapshots.
		valid = false
	}

	var deltas []schema.Web100ValueMap
	deltaFieldCount := 0
	deltas, deltaFieldCount = n.getDeltas(snaplog, testType)
	if deltas == nil {
		// There was some kind of major failure parsing snapshots.
		return
	}
	final := snaplog.SnapCount() - 1
	if final > maxNumSnapshots {
		final = maxNumSnapshots
	}
	snap, err := snaplog.Snapshot(final)
	if err != nil {
		metrics.ErrorCount.WithLabelValues(
			n.TableName(), testType, "final snapshot failure").Inc()
		metrics.TestCount.WithLabelValues(
			n.TableName(), testType, "final snapshot failure").Inc()
		return
	}
	snapValues := schema.EmptySnap()
	snap.SnapshotValues(snapValues)
	if err != nil {
		metrics.ErrorCount.WithLabelValues(
			n.TableName(), testType, "final snapValues failure").Inc()
		metrics.TestCount.WithLabelValues(
			n.TableName(), testType, "final snapValues failure").Inc()
		log.Printf("Error calling SnapshotValues() in test %s, when processing: %s\n%s\n",
			test.fn, n.taskFileName, err)
		return
	}

	// TODO(prod) Write a row with this data, even if the snapshot parsing fails?
	nestedConnSpec := make(schema.Web100ValueMap, 6)
	snaplog.ConnectionSpecValues(nestedConnSpec)

	results := schema.NewWeb100MinimalRecord(
		snaplog.Version, int64(snaplog.LogTime),
		nestedConnSpec, snapValues, deltas)

	// Create a synthetic UUID for joining with annotations.
	results["id"] = ndtWeb100SyntheticUUID(test.fn)
	results["test_id"] = test.fn
	results["task_filename"] = n.taskFileName
	if snaplog.SnapCount() > maxNumSnapshots || snaplog.SnapCount() < minNumSnapshots {
		results["anomalies"].(schema.Web100ValueMap)["num_snaps"] = snaplog.SnapCount()
	}
	if !valid {
		results["anomalies"].(schema.Web100ValueMap)["snaplog_error"] = true
	}

	if NDTEstimateBW {
		// This is not terribly useful as is.  Intended as a place holder for code
		// we are working on in parallel.
		congEvents := make(schema.Web100ValueMap, 10)
		snapNums, snapErr := snaplog.ChangeIndices("SmoothedRTT")
		if snapErr != nil {
			log.Println(snapErr)
		} else {
			congEvents["indices"] = snapNums
			congEvents["smoothedRTT"] = snaplog.SliceIntField("SmoothedRTT", snapNums)
			congEvents["thruOctetsAcked"] = snaplog.SliceIntField("HCThruOctetsAcked", snapNums)
			results["slices"] = congEvents
		}
	}

	// This is the timestamp parsed from the filename.
	lt, err := test.info.Timestamp.MarshalText()
	if err != nil {
		log.Println(err)
		metrics.ErrorCount.WithLabelValues(
			n.TableBase(), "log_time marshal error").Inc()
	} else {
		results["log_time"] = string(lt)
	}
	now, err := time.Now().MarshalText()
	if err != nil {
		log.Println(err)
		metrics.ErrorCount.WithLabelValues(
			n.TableBase(), "parse_time marshal error").Inc()
	} else {
		results["parse_time"] = string(now)
	}
	// Record the parser version used to calculate this row.
	results["parser_version"] = Version()

	connSpec := schema.EmptyConnectionSpec()
	if n.metaFile != nil {
		// TODO - metaFile is currently used only to populate the connection spec.
		// Should we be using it for anything else?
		n.metaFile.PopulateConnSpec(connSpec)
	} else {
		// TODO Add a log once noise is reduced.
		metrics.WarningCount.WithLabelValues(
			n.TableName(), testType, "no meta").Inc()
		results["anomalies"].(schema.Web100ValueMap)["no_meta"] = true
		// TODO(dev) - use other information to partially populate
		// the connection spec.
	}

	switch testType {
	case "c2s":
		connSpec.SetInt64("data_direction", CLIENT_TO_SERVER)
	case "s2c":
		connSpec.SetInt64("data_direction", SERVER_TO_CLIENT)
	default:
	}

	results["connection_spec"] = connSpec

	n.fixValues(results)

	// TODO fix InsertRow so that we can distinguish errors from prior rows.
	metrics.EntryFieldCountHistogram.WithLabelValues(n.TableName()).
		Observe(float64(deltaFieldCount))
	if deltaFieldCount > 43000 {
		log.Printf("Lots of fields (%d) processing %s from %s\n",
			deltaFieldCount, test.fn, n.taskFileName)
	}

	// TODO - estimate the size of the json (or fields) to allow more rows per request,
	// but avoid going over the 10MB limit.
	// Add row to buffer, possibly flushing buffer if it is full.
	ndtTest := NDTTest{results}
	err = n.Base.AddRow(ndtTest)
	if err == etl.ErrBufferFull {
		// Ignore annotation errors.  They are counted and logged elsewhere.
		n.Base.AnnotateAndPutAsync(n.TableBase())
		err = n.Base.AddRow(ndtTest)
	}
	if err != nil {
		metrics.ErrorCount.WithLabelValues(
			n.TableName(), testType, "insert-err").Inc()
		// TODO: This is an insert error, that might be recoverable if we try again.
		log.Println("insert-err: " + err.Error())
		return
	}
	metrics.TestCount.WithLabelValues(
		n.TableName(), testType, "ok").Inc()
}

const (
	// These are all caps to reflect the linux constant names.
	WC_ADDRTYPE_IPV4 = 1
	WC_ADDRTYPE_IPV6 = 2
	LOCAL_AF_IPV4    = 0
	LOCAL_AF_IPV6    = 1
)

// fixValues updates web100 log values that need post-processing fix-ups.
// TODO(dev): does this only apply to NDT or is NPAD also affected?
// TODO(dev) - consider improving test coverage.
func (n *NDTParser) fixValues(r schema.Web100ValueMap) {
	connSpec := r.GetMap([]string{"connection_spec"})
	logEntry := r.GetMap([]string{"web100_log_entry"})
	snap := logEntry.GetMap([]string{"snap"})
	nestedConnSpec := logEntry.GetMap([]string{"connection_spec"})

	// If FQDN not available, NDT puts "No FQDN name" into the client_hostname string.
	// In legacy tables, this results in the entry being left empty, so we duplicate
	// that here.
	if connSpec["client_hostname"] == "No FQDN name" {
		delete(connSpec, "client_hostname")
	}

	data, err := etl.ValidateTestPath(n.taskFileName)
	if err != nil {
		// The current filename is ambiguous, but the timestamp should help.
		log.Printf("WARNING: taskFileName is unexpectedly invalid: %s %s: %q",
			n.taskFileName, n.timestamp, err)
	} else {
		// TODO - this is a rather hacky place to put this.
		connSpec.Get("server").SetString("iata_code", strings.ToUpper(data.Site[0:3]))

		// If there is no meta file then the server hostname will not be set.
		// We must check for presence and an empty value.
		hn, ok := connSpec["server_hostname"]
		if !ok || hn == "" {
			connSpec.SetString("server_hostname", fmt.Sprintf(
				"%s.%s.%s", data.Host, data.Site, etl.MlabDomain))
		}
	}

	// snapshot addresses are always authoritative.  Other sources don't handle
	// ipv6 correctly.  So, always substitute, unless for some reason the snapshot
	// value is missing.
	logEntry.SubstituteString(true, []string{"connection_spec", "local_ip"},
		[]string{"snap", "LocalAddress"})
	logEntry.SubstituteString(true, []string{"connection_spec", "remote_ip"},
		[]string{"snap", "RemAddress"})

	// Handle local_af.
	// Translate LocalAddressType values of WC_ADDRTYPE_IPV4 (1) or WC_ADDRTYPE_IPV6 (2)
	// to legacy tables local_af values (LOCAL_AF_IPV*.)
	localAddrType, ok := snap["LocalAddressType"]
	if ok {
		switch localAddrType {
		case WC_ADDRTYPE_IPV4:
			nestedConnSpec.SetInt64("local_af", LOCAL_AF_IPV4)
		case WC_ADDRTYPE_IPV6:
			nestedConnSpec.SetInt64("local_af", LOCAL_AF_IPV6)
		default:
			// Leave it empty.
		}
	}

	// Top level connection spec values are high quality, but if the meta
	// file is missing, they may be empty.  In that case, we replace them
	// with values from the log entry, (which usually come from the snapshot.)
	// TODO - make these the ONLY representation of client/server tuple.
	r.SubstituteString(false, []string{"connection_spec", "server_ip"},
		[]string{"web100_log_entry", "connection_spec", "local_ip"})
	r.SubstituteInt64(false, []string{"connection_spec", "server_af"},
		[]string{"web100_log_entry", "connection_spec", "local_af"})
	r.SubstituteString(false, []string{"connection_spec", "client_ip"},
		[]string{"web100_log_entry", "connection_spec", "remote_ip"})
	r.SubstituteInt64(false, []string{"connection_spec", "client_af"},
		[]string{"web100_log_entry", "connection_spec", "local_af"})

	start, ok := snap.GetInt64([]string{"StartTimeStamp"})
	if ok {
		start = 1000000 * start
		usec, ok := snap.GetInt64([]string{"StartTimeUsec"})
		if ok {
			start += usec
		}
		snap.SetInt64("StartTimeStamp", start)
	}

}

// Implement parser.Annotatable
// These are somewhat ugly, since we have to pull things out of the nested maps and interpret them.

// NDTTest is a wrapper for Web100ValueMap that implements Annotatable.
type NDTTest struct {
	schema.Web100ValueMap
}

// Only valid on top level
func (ndt NDTTest) getConnSpec() schema.Web100ValueMap {
	return ndt.GetMap([]string{"connection_spec"})
}

// GetLogTime returns the timestamp that should be used for annotation.
func (ndt NDTTest) GetLogTime() time.Time {
	var t time.Time
	tt, ok := ndt.GetString([]string{"log_time"})
	if !ok {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "NDTTest missing log_time."}).Inc()
		return time.Now()
	}
	err := t.UnmarshalText([]byte(tt))
	if err != nil {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "NDTTest error parsing log_time."}).Inc()
		return time.Now()
	}
	return t
}

// GetClientIPs returns the client (remote) IP for annotation.  See parser.Annotatable
// This is a bit ugly because of the use of bigquery Value maps.
func (ndt NDTTest) GetClientIPs() []string {
	connSpec := ndt.getConnSpec()
	ip, _ := connSpec.GetString([]string{"client_ip"})
	if ip == "" {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "NDTTest missing client IP."}).Inc()
		return []string{}
	}
	return []string{ip}
}

// GetServerIP returns the server (local) IP for annotation.  See parser.Annotatable
// This is a bit ugly because of the use of bigquery Value maps.
func (ndt NDTTest) GetServerIP() string {
	connSpec := ndt.getConnSpec()
	ip, ok := connSpec.GetString([]string{"server_ip"})
	if !ok {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "NDTTest missing server IP."}).Inc()
		return ""
	}
	return ip
}

// CopyStructToMap takes a POINTER to an arbitrary SIMPLE struct and copies
// it's fields into a value map. It will also make fields entirely
// lower case, for convienece when working with exported structs. Also,
// NEVER pass in something that is not a pointer to a struct, as this
// will cause a panic.
func CopyStructToMap(sourceStruct interface{}, destinationMap map[string]bigquery.Value) {
	structToCopy := reflect.ValueOf(sourceStruct).Elem()
	typeOfStruct := structToCopy.Type()
	for i := 0; i < typeOfStruct.NumField(); i++ {
		f := structToCopy.Field(i)
		v := f.Interface()
		switch t := v.(type) {
		case string:
			// TODO - are these still needed?  Does the omitempty cover it?
			if t == "" {
				continue
			}
		case int64:
			if t == 0 {
				continue
			}
		}
		jsonTag, ok := typeOfStruct.Field(i).Tag.Lookup("json")
		name := strings.ToLower(typeOfStruct.Field(i).Name)
		if ok {
			tags := strings.Split(jsonTag, ",")
			if len(tags) > 0 && tags[0] != "" {
				name = tags[0]
			}
		}
		destinationMap[strings.ToLower(name)] = v
	}
}

// copyStructToMapDirectly copies struct fields to the given bigquery.Value map.
// Unlike CopyStructToMap, this function preserves the original field case.
func copyStructToMapDirectly(sourceStruct interface{}, destinationMap map[string]bigquery.Value) {
	structToCopy := reflect.ValueOf(sourceStruct).Elem()
	typeOfStruct := structToCopy.Type()
	for i := 0; i < typeOfStruct.NumField(); i++ {
		f := structToCopy.Field(i)
		v := f.Interface()
		switch t := v.(type) {
		case string:
			if t == "" {
				continue
			}
		case int64:
			if t == 0 {
				continue
			}
		case uint32:
			if t == 0 {
				continue
			}
			// NOTE: bigquery.Value does not support unsigned int types. The
			// annotation-service API returns uint32 for the ASNumber field.
			// When copying this value, convert it to an int64 so that it is
			// BigQuery compatible.
			v = int64(t)
		}
		destinationMap[typeOfStruct.Field(i).Name] = v
	}
}

var logMissing = logx.NewLogEvery(nil, 60*time.Second) // This is per instance.

// AnnotateClients adds the client annotations. See parser.Annotatable
// This is a bit ugly because of the use of bigquery Value maps.
func (ndt NDTTest) AnnotateClients(annMap map[string]*api.Annotations) error {
	spec := ndt.getConnSpec()
	ip, _ := spec.GetString([]string{"client_ip"})
	if ip == "" {
		return ErrAnnotationError
	}

	if data, ok := annMap[ip]; ok && data.Geo != nil {
		CopyStructToMap(data.Geo, spec.Get("client_geolocation"))
		if data.Network != nil {
			asn, err := data.Network.BestASN()
			if err != nil {
				metrics.AnnotationErrorCount.With(prometheus.
					Labels{"source": "NDTTest BestASN error on client IP."}).Inc()
			} else {
				spec.Get("client").Get("network")["asn"] = asn
				// CopyStructToMap(data.Network, spec.Get("client"))
				c := v2as.ConvertAnnotationsToClientAnnotations(data)
				copyStructToMapDirectly(c.Geo, spec.Get("ClientX").Get("Geo"))
				copyStructToMapDirectly(c.Network, spec.Get("ClientX").Get("Network"))
			}
		}
	} else {
		// A large fraction of our unannotated entries in 2018 are from this single IP,
		// so we break it out with its own label.
		if ip == "45.56.98.222" {
			metrics.AnnotationErrorCount.With(prometheus.
				Labels{"source": "NDTTest missing annotations for client IP " + ip}).Inc()
			return ErrAnnotationError
		}

		// The rate of missing annotations is modest aside from 45.56.98,222, so we log
		// all of them.  May have to reconsider if this is too spammy.  Might want to
		// limit to once per second with logx.LogEvery.
		logMissing.Println("Missing annotation for", ip)

		// A large fraction of our unannotated entries are from this ipv6 prefix.
		// Don't know why, but break it out with its own label to facilitate debugging.
		if strings.HasPrefix(ip, "2002:") {
			metrics.AnnotationErrorCount.With(prometheus.
				Labels{"source": "NDTTest missing annotations for client IP in 2002:..."}).Inc()
			return ErrAnnotationError
		}
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "NDTTest missing annotations for client IP"}).Inc()
		return ErrAnnotationError
	}
	return nil
}

// AnnotateServer adds the server annotations. See parser.Annotatable
// This is a bit ugly because of the use of bigquery Value maps.
func (ndt NDTTest) AnnotateServer(local *api.Annotations) error {
	if local == nil {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "NDTTest missing annotations for server IP."}).Inc()
		return ErrAnnotationError
	}
	spec := ndt.getConnSpec()
	if local.Geo != nil {
		CopyStructToMap(local.Geo, spec.Get("server_geolocation"))
	}
	if local.Network != nil {
		asn, err := local.Network.BestASN()
		if err != nil {
			metrics.AnnotationErrorCount.With(prometheus.
				Labels{"source": "NDTTest BestASN error on server IP."}).Inc()
			return err
		}
		spec.Get("server").Get("network")["asn"] = asn
		s := v2as.ConvertAnnotationsToServerAnnotations(local)
		copyStructToMapDirectly(s.Geo, spec.Get("ServerX").Get("Geo"))
		copyStructToMapDirectly(s.Network, spec.Get("ServerX").Get("Network"))
	}

	return nil
}
