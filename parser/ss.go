// Parse Sidestream tests.
package parser

import (
	"bytes"
	"errors"
	"fmt"
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

type SSParser struct {
	inserter etl.Inserter
	etl.RowStats
}

func NewSSParser(ins etl.Inserter) *SSParser {
	return &SSParser{ins, ins}
}

// The legacy filename is like  "20170203T00:00:00Z_ALL0.web100"
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

// the first line of SS test is in format "K: cid PollTime LocalAddress LocalPort ... other_web100_variables_separated_by_space"
func ParseKHeader(header string) ([]string, error) {
	var var_names []string
	web100_vars := strings.Split(header, " ")
	if web100_vars[0] != "K:" {
		return var_names, errors.New("Corrupted header")
	}

	data, err := web100.Asset("tcp-kis.txt")
	if err != nil {
		panic("tcp-kis.txt not found")
	}
	b := bytes.NewBuffer(data)

	mapping, err := web100.ParseWeb100Definitions(b)

	for index, name := range web100_vars {
		if index == 0 {
			continue
		}

		if mapping[name] != "" {
			var_names = append(var_names, mapping[name])
		} else {
			var_names = append(var_names, name)
		}
	}
	return var_names, nil
}

func (ss *SSParser) TaskError() error {
	return nil
}

func (ss *SSParser) TableName() string {
	return ss.inserter.TableBase()
}

func (ss *SSParser) FullTableName() string {
	return ss.inserter.FullTableName()
}

func (ss *SSParser) Flush() error {
	return ss.inserter.Flush()
}

// Prepare data into sidestream BigQeury schema and insert it.
func PackDataIntoSchema(ss_value map[string]string, log_time time.Time, testName string) (schema.SS, error) {
	local_port, err := strconv.Atoi(ss_value["LocalPort"])
	if err != nil {
		return schema.SS{}, err
	}
	remote_port, err := strconv.Atoi(ss_value["RemPort"])
	if err != nil {
		return schema.SS{}, err
	}

	conn_spec := &schema.Web100ConnectionSpecification{
		Local_ip:    ss_value["LocalAddress"],
		Local_af:    ParseIPFamily(ss_value["LocalAddress"]),
		Local_port:  int64(local_port),
		Remote_ip:   ss_value["RemAddress"],
		Remote_port: int64(remote_port),
	}

	AddGeoDataSSConnSpec(conn_spec, log_time)
	snap, err := PopulateSnap(ss_value)
	if err != nil {
		return schema.SS{}, err
	}
	web100_log := &schema.Web100LogEntry{
		Log_time:        log_time.Unix(),
		Version:         "unknown",
		Group_name:      "read",
		Connection_spec: *conn_spec,
		Snap:            snap,
	}

	ss_test := &schema.SS{
		Test_id:          testName,
		Log_time:         log_time.Unix(),
		Type:             int64(1),
		Project:          int64(2),
		Web100_log_entry: *web100_log,
	}
	return *ss_test, nil
}

func ParseOneLine(snapshot string, var_names []string) (map[string]string, error) {
	value := strings.Split(snapshot, " ")
	ss_value := make(map[string]string)
	if value[0] != "C:" || len(value) != len(var_names)+1 {
		log.Printf("corrupted content:")
		log.Printf(snapshot)
		return ss_value, errors.New("corrupted content")
	}

	for index, val := range value[1:] {
		// Match value with var_name
		ss_value[var_names[index]] = val
	}
	return ss_value, nil
}

func PopulateSnap(ss_value map[string]string) (schema.Web100Snap, error) {
	var snap = &schema.Web100Snap{}
	for key := range ss_value {
		// Skip cid and PollTime. They are SideStream-specific fields, not web100 variables.
		if key == "cid" || key == "PollTime" {
			continue
		}
		// We do special handling for this variable
		if key == "StartTimeUsec" {
			// TODO: func CalculateStartTimeStamp() to get correct StartTimeStamp value.
			continue
		}
		x := reflect.ValueOf(snap).Elem().FieldByName(key)

		switch x.Type().String() {
		case "int64":
			value, err := strconv.Atoi(ss_value[key])
			if err != nil {
				return *snap, err
			}
			x.SetInt(int64(value))
		case "string":
			x.Set(reflect.ValueOf(ss_value[key]))
		case "bool":
			if ss_value[key] == "0" {
				x.Set(reflect.ValueOf(false))
			} else if ss_value[key] == "1" {
				x.Set(reflect.ValueOf(true))
			} else {
				return *snap, errors.New("Cannot parse field " + key + " into a valie bool value.")
			}
		}
	}
	// TODO: check whether snap has valid LocalAddress, RemAddress. Return error if not.
	return *snap, nil
}

// IsParsable returns the canonical test type and whether to parse data.
func (ss *SSParser) IsParsable(testName string, data []byte) (string, bool) {
	if strings.HasSuffix(testName, "web100") {
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

	log_time, err := ExtractLogtimeFromFilename(testName)
	if err != nil {
		return err
	}
	var var_names []string
	testContent := strings.Split(string(rawContent[:]), "\n")
	if len(testContent) < 2 {
		return errors.New("empty test file.")
	}
	var_names, err = ParseKHeader(testContent[0])
	if err != nil {
		metrics.ErrorCount.WithLabelValues(
			ss.TableName(), "ss", "corrupted header").Inc()
		return err
	}
	for _, oneLine := range testContent[1:] {
		oneLine := strings.TrimSuffix(oneLine, "\n")

		if len(oneLine) == 0 {
			continue
		}
		ss_value, err := ParseOneLine(oneLine, var_names)
		if err != nil {
			metrics.TestCount.WithLabelValues(
				ss.TableName(), "ss", "corrupted content").Inc()
			return err
		}
		err = ValidateIP(ss_value["LocalAddress"])
		if err != nil {
			metrics.TestCount.WithLabelValues(
				ss.TableName(), "ss", "Invalid server IP").Inc()
			return fmt.Errorf("Invalid server IP address: %s with error: %s", ss_value["LocalAddress"], err)
		}
		err = ValidateIP(ss_value["RemAddress"])
		if err != nil {
			metrics.TestCount.WithLabelValues(
				ss.TableName(), "ss", "Invalid client IP").Inc()
			return fmt.Errorf("Invalid client IP address: %s with error: %s", ss_value["RemAddress"], err)
		}
		ss_test, err := PackDataIntoSchema(ss_value, log_time, testName)
		if err != nil {
			metrics.ErrorCount.WithLabelValues(
				ss.TableName(), "ss", "corrupted data").Inc()
			metrics.TestCount.WithLabelValues(
				ss.TableName(), "ss", "corrupted data").Inc()
			log.Printf("cannot pack data into sidestream schema: %v\n", err)
			return err
		}
		err = ss.inserter.InsertRow(ss_test)
		if err != nil {
			metrics.ErrorCount.WithLabelValues(
				ss.TableName(), "ss", "insert-err").Inc()
			metrics.TestCount.WithLabelValues(
				ss.TableName(), "ss", "insert-err").Inc()
			log.Printf("insert-err: %v\n", err)
			continue
		}
		metrics.TestCount.WithLabelValues(ss.TableName(), "ss", "ok").Inc()

	}
	return nil
}
