// Parse Sidestream tests.
package parser

import (
	"bytes"
	"cloud.google.com/go/bigquery"
	"errors"
	"log"
	"net"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

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
func ExtractLogtimeFromFilename(fileName string) (int64, error) {
	testName := filepath.Base(fileName)
	if len(testName) < 19 || !strings.Contains(testName, ".web100") {
		log.Println(testName)
		return 0, errors.New("Invalid sidestream filename")
	}

	t, err := time.Parse("20060102T15:04:05.999999999Z_", testName[0:17]+".000000000Z_")
	if err != nil {
		return 0, err
	}

	return t.Unix(), nil
}

func ParseIPFamily(ipStr string) int {
	ip := net.ParseIP(ipStr)
	if ip.To4() != nil {
		return syscall.AF_INET
	} else if ip.To16() != nil {
		return syscall.AF_INET6
	}
	return -1
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

func (ss *SSParser) TableName() string {
	return ss.inserter.TableBase()
}

func (ss *SSParser) FullTableName() string {
	return ss.inserter.FullTableName()
}

func (ss *SSParser) Flush() error {
	return ss.inserter.Flush()
}

// TODO: add metrics
// Prepare data into sidestream BigQeury schema and insert it.
func PackDataIntoSchema(ss_value map[string]string, log_time int64, testName string) (schema.SS, error) {
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
		Local_af:    int32(ParseIPFamily(ss_value["LocalAddress"])),
		Local_port:  int32(local_port),
		Remote_ip:   ss_value["RemAddress"],
		Remote_port: int32(remote_port),
	}
	snap, err := PopulateSnap(ss_value)
	if err != nil {
		return schema.SS{}, err
	}
	web100_log := &schema.Web100LogEntry{
		Log_time:        log_time,
		Version:         "unknown",
		Group_name:      "read",
		Connection_spec: *conn_spec,
		Snap:            snap,
	}

	ss_test := &schema.SS{
		Test_id:          testName,
		Log_time:         log_time,
		Type:             int32(1),
		Project:          int32(2),
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
		ss_value[var_names[index-1]] = val
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
		//fmt.Println(key)
		x := reflect.ValueOf(snap).Elem().FieldByName(key)
		t := x.Type().String()
		//log.Printf("Name: %s    Type: %s\n", key, t)

		switch t {
		case "int32":
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

func (ss *SSParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, rawContent []byte) error {
	log_time, err := ExtractLogtimeFromFilename(testName)
	if err != nil {
		return err
	}
	var var_names []string
	for index, oneLine := range strings.Split(string(rawContent[:]), "\n") {
		oneLine := strings.TrimSuffix(oneLine, "\n")
		// TODO: add metrics.
		if index == 0 {
			var_names, err = ParseKHeader(oneLine)
			if err != nil {
				return err
			}
		} else {
			if len(oneLine) == 0 {
				continue
			}
			ss_value, err := ParseOneLine(oneLine, var_names)
			if err != nil {
				return err
			}
			ss_test, err := PackDataIntoSchema(ss_value, log_time, testName)
			if err != nil {
				log.Printf("cannot pack data into sidestream schema: %v\n", err)
				return err
			}
			err = ss.inserter.InsertRow(ss_test)

			if err != nil {
				metrics.ErrorCount.WithLabelValues(
					ss.TableName(), "ss", "insert-err").Inc()
				log.Printf("insert-err: %v\n", err)
				continue
			}
		}
	}
	return nil
}
