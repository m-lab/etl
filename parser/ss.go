package parser

import (
	"bytes"
	"errors"
	"log"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/etl/web100"
)

// Parser for parsing sidestream tests.

// SSParser provides a parser implementation for SideStream data.
type SSParser struct {
	Base
}

// NewSSParser creates a new sidestream parser.
func NewSSParser(ins etl.Inserter) *SSParser {
	bufSize := etl.SS.BQBufferSize()
	return &SSParser{*NewBase(ins, bufSize)}
}

// TODO get rid of this hack.
func NewDefaultSSParser(ins etl.Inserter) *SSParser {
	bufSize := etl.SS.BQBufferSize()
	return &SSParser{*NewBase(ins, bufSize)}
}

// ExtractLogtimeFromFilename extracts the log time.
// legacy filename is like  "20170203T00:00:00Z_ALL0.web100"
// The current filename is like "20170315T01:00:00Z_173.205.3.39_0.web100"
// Return time stamp if the filename is in right format
func ExtractLogtimeFromFilename(fileName string) (time.Time, error) {
	testName := filepath.Base(fileName)
	if len(testName) < 19 || !strings.Contains(testName, ".web100") {
		log.Println(testName)
		return time.Time{}, errors.New("Invalid sidestream filename")
	}

	t, err := time.Parse("20060102T15:04:05.999999999Z_", testName[0:17]+".000000000Z_")
	if err != nil {
		return time.Time{}, err
	}

	return t, nil
}

// ParseKHeader parses the first line of SS file, in format "K: cid PollTime LocalAddress LocalPort ... other_web100_variables_separated_by_space"
func ParseKHeader(header string) ([]string, error) {
	var varNames []string
	web100Vars := strings.Split(header, " ")
	if web100Vars[0] != "K:" {
		return varNames, errors.New("Corrupted header")
	}

	data, err := web100.Asset("tcp-kis.txt")
	if err != nil {
		// TODO - convert this panic to something else.
		panic("tcp-kis.txt not found")
	}
	b := bytes.NewBuffer(data)

	mapping, err := web100.ParseWeb100Definitions(b)
	if err != nil {
		// TODO - convert this panic to something else.
		panic("tcp-kis.txt not found")
	}

	for index, name := range web100Vars {
		if index == 0 {
			continue
		}

		if mapping[name] != "" {
			varNames = append(varNames, mapping[name])
		} else {
			varNames = append(varNames, name)
		}
	}
	return varNames, nil
}

// TableName of the table that this Parser inserts into.
func (ss *SSParser) TableName() string {
	return ss.TableBase()
}

// PackDataIntoSchema packs data into sidestream BigQeury schema and buffers it.
func PackDataIntoSchema(ssValue map[string]string, logTime time.Time, testName string) (schema.SS, error) {
	localPort, err := strconv.Atoi(ssValue["LocalPort"])
	if err != nil {
		return schema.SS{}, err
	}
	remotePort, err := strconv.Atoi(ssValue["RemPort"])
	if err != nil {
		return schema.SS{}, err
	}

	ssValue["LocalAddress"] = NormalizeIP(ssValue["LocalAddress"])
	ssValue["RemAddress"] = NormalizeIP(ssValue["RemAddress"])
	connSpec := &schema.Web100ConnectionSpecification{
		Local_ip:    ssValue["LocalAddress"],
		Local_af:    web100.ParseIPFamily(ssValue["LocalAddress"]),
		Local_port:  int64(localPort),
		Remote_ip:   ssValue["RemAddress"],
		Remote_port: int64(remotePort),
	}

	// NOTE: Annotation was previously done here, using AddGeoDataSS...(), but it now done
	// in ss.Annotate, prior to inserter.PutAsync
	snap, err := PopulateSnap(ssValue)
	if err != nil {
		return schema.SS{}, err
	}
	web100Log := &schema.Web100LogEntry{
		LogTime:         logTime.Unix(), // TODO: Should use timestamp, not integer
		Version:         "unknown",
		Group_name:      "read", // TODO: Use Camelcase, with json annotations?
		Connection_spec: *connSpec,
		Snap:            snap,
	}

	// Create a synthetic UUID for joining with annotations.
	id := ssSyntheticUUID(
		testName,
		web100Log.Snap.StartTimeStamp,
		web100Log.Connection_spec.Local_ip,
		web100Log.Connection_spec.Local_port,
		web100Log.Connection_spec.Remote_ip,
		web100Log.Connection_spec.Remote_port)

	ssTest := &schema.SS{
		ID:               id,
		TestID:           testName,
		LogTime:          logTime.Unix(),
		Type:             int64(1),
		Project:          int64(2),
		Web100_log_entry: *web100Log,
	}
	return *ssTest, nil
}

// ParseOneLine parses a single line of sidestream data.
func ParseOneLine(snapshot string, varNames []string) (map[string]string, error) {
	value := strings.Split(snapshot, " ")
	ssValue := make(map[string]string)
	if value[0] != "C:" || len(value) != len(varNames)+1 {
		log.Printf("corrupted content:")
		log.Printf(snapshot)
		return ssValue, errors.New("corrupted content")
	}

	for index, val := range value[1:] {
		// Match value with var_name
		ssValue[varNames[index]] = val
	}
	return ssValue, nil
}

// PopulateSnap fills in the snapshot data.
func PopulateSnap(ssValue map[string]string) (schema.Web100Snap, error) {
	var snap = &schema.Web100Snap{}
	var startTimeUsec int64

	// First, extract StartTimeUsec value before all others so we can combine
	// it with StartTimeStamp below.
	if valueStr, ok := ssValue["StartTimeUsec"]; ok {
		value, err := strconv.ParseInt(valueStr, 10, 64)
		if err == nil {
			startTimeUsec = value
		}
	}

	// Process every other snap key.
	for key := range ssValue {
		// Skip cid and PollTime. They are SideStream-specific fields, not web100 variables.
		if key == "cid" || key == "PollTime" {
			continue
		}
		// Skip StartTimeUsec because this is not part of the Web100Snap struct.
		if key == "StartTimeUsec" {
			continue
		}
		x := reflect.ValueOf(snap).Elem().FieldByName(key)

		switch x.Type().String() {
		case "int64":
			value, err := strconv.ParseInt(ssValue[key], 10, 64)
			if err != nil {
				return *snap, err
			}
			x.SetInt(value)
		case "string":
			x.Set(reflect.ValueOf(ssValue[key]))
		case "bool":
			if ssValue[key] == "0" {
				x.Set(reflect.ValueOf(false))
			} else if ssValue[key] == "1" {
				x.Set(reflect.ValueOf(true))
			} else {
				return *snap, errors.New("Cannot parse field " + key + " into a valie bool value.")
			}
		}
	}
	// Combine the StartTimeStamp and StartTimeUsec values.
	snap.StartTimeStamp = snap.StartTimeStamp*1000000 + startTimeUsec

	// TODO: check whether snap has valid LocalAddress, RemAddress. Return error if not.
	return *snap, nil
}

// IsParsable returns the canonical test type and whether to parse data.
func (ss *SSParser) IsParsable(testName string, data []byte) (string, bool) {
	if strings.HasSuffix(testName, ".web100") {
		return "web100", true
	}
	if strings.HasSuffix(testName, ".tra") {
		// Ignore the trace file for sidestream test.
		return "trace", false
	}
	return "unknown", false
}

// ParseAndInsert extracts each sidestream record from the rawContent and inserts each into a separate row.
func (ss *SSParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, rawContent []byte) error {
	// TODO: for common metric states with constant labels, define global constants.
	metrics.WorkerState.WithLabelValues(ss.TableName(), "ss").Inc()
	defer metrics.WorkerState.WithLabelValues(ss.TableName(), "ss").Dec()

	logTime, err := ExtractLogtimeFromFilename(testName)
	if err != nil {
		return err
	}
	testContent := strings.Split(string(rawContent[:]), "\n")
	if len(testContent) < 2 {
		return errors.New("empty test file")
	}
	varNames, err := ParseKHeader(testContent[0])
	if err != nil {
		metrics.ErrorCount.WithLabelValues(
			ss.TableName(), "ss", "corrupted header").Inc()
		return err
	}
	for _, oneLine := range testContent[1:] {
		oneLine = strings.TrimSuffix(oneLine, "\n")

		if len(oneLine) == 0 {
			continue
		}
		ssValue, err := ParseOneLine(oneLine, varNames)
		if err != nil {
			metrics.TestTotal.WithLabelValues(
				ss.TableName(), "ss", "corrupted content").Inc()
			continue
		}
		err = web100.ValidateIP(ssValue["LocalAddress"])
		if err != nil {
			metrics.TestTotal.WithLabelValues(
				ss.TableName(), "ss", "Invalid server IP").Inc()
			log.Printf("Invalid server IP address: %s with error: %s\n", ssValue["LocalAddress"], err)
			continue
		}
		err = web100.ValidateIP(ssValue["RemAddress"])
		if err != nil {
			metrics.TestTotal.WithLabelValues(
				ss.TableName(), "ss", "Invalid client IP").Inc()
			log.Printf("Invalid client IP address: %s with error: %s", ssValue["RemAddress"], err)
			continue
		}
		ssTest, err := PackDataIntoSchema(ssValue, logTime, testName)
		if err != nil {
			metrics.TestTotal.WithLabelValues(
				ss.TableName(), "ss", "corrupted data").Inc()
			log.Printf("cannot pack data into sidestream schema: %v\n", err)
			continue
		}

		ssTest.ParseTime = time.Now() // for map, use string(time.Now().MarshalText())
		ssTest.ParserVersion = Version()
		if meta["filename"] != nil {
			ssTest.TaskFileName = meta["filename"].(string)
		}

		// ArchiveURL must already be valid, so error is safe to ignore.
		dp, _ := etl.ValidateTestPath(ssTest.TaskFileName)
		ssTest.Web100_log_entry.Connection_spec.ServerX.Site = dp.Site
		ssTest.Web100_log_entry.Connection_spec.ServerX.Machine = dp.Host

		// Add row to buffer, possibly flushing buffer if it is full.
		err = ss.AddRow(&ssTest)
		if err == etl.ErrBufferFull {
			// Flush asynchronously, to improve throughput.
			ss.PutAsync(ss.TakeRows())
			err = ss.AddRow(&ssTest)
		}
		if err != nil {
			metrics.ErrorCount.WithLabelValues(
				ss.TableName(), "ss", "insert-err").Inc()
			metrics.TestTotal.WithLabelValues(
				ss.TableName(), "ss", "insert-err").Inc()
			log.Printf("insert-err: %v\n", err)
			continue
		}
		metrics.TestTotal.WithLabelValues(ss.TableName(), "ss", "ok").Inc()
	}
	return nil
}
