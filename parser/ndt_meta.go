package parser

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/schema"
)

// This is the parsed info from the .meta file.
type metaFileData struct {
	testName    string
	dateTime    time.Time
	summaryData []int32
	tls         bool
	websockets  bool

	fields map[string]string // All of the string fields.
}

const (
	CLIENT_TO_SERVER = 0
	SERVER_TO_CLIENT = 1
)

var fieldPairs = map[string]string{
	"server IP address":       "server_ip",
	"server hostname":         "server_hostname",
	"server kernel version":   "server_kernel_version",
	"client IP address":       "client_ip",
	"client hostname":         "client_hostname",
	"client OS name":          "client_os",
	"client_browser name":     "client_browser",
	"client_application name": "client_application",
}

func (mfd *metaFileData) PopulateConnSpec(connSpec schema.Web100ValueMap) {
	for k, v := range fieldPairs {
		s, ok := mfd.fields[k]
		if ok && s != "" {
			connSpec.SetString(v, s)
		}
	}
	s, ok := mfd.fields["server_ip"]
	// TODO - extract function for this stanza
	if ok && s != "" {
		connSpec.SetString("server_ip", s)
		ip := net.ParseIP(s)
		if ip == nil {
			// TODO - log/metric
		} else {
			if ip.To4() != nil {
				connSpec.SetString("server_ip", ip.String())
				connSpec.SetInt64("server_af", syscall.AF_INET)
			} else if ip.To16() != nil {
				connSpec.SetString("server_ip", ip.String())
				connSpec.SetInt64("server_af", syscall.AF_INET6)
			}
		}
	}
	s, ok = mfd.fields["client_ip"]
	if ok && s != "" {
		connSpec.SetString("client_ip", s)
		ip := net.ParseIP(s)
		if ip == nil {
			// TODO - log/metric
		} else {
			if ip.To4() != nil {
				connSpec.SetString("client_ip", ip.String())
				connSpec.SetInt64("client_af", syscall.AF_INET)
			} else if ip.To16() != nil {
				connSpec.SetString("client_ip", ip.String())
				connSpec.SetInt64("client_af", syscall.AF_INET6)
			}
		}
	}
}

// createMetaFileData uses the key:value pairs to populate the interpreted fields.
// TODO(dev) - more unit tests?
// TODO(dev) - move to separate file - meta.go
func createMetaFileData(testName string, fields map[string]string) (*metaFileData, error) {
	var data metaFileData
	data.testName = testName
	data.fields = make(map[string]string, 20)
	for k, v := range fields {
		var err error
		v = strings.TrimSpace(v)
		switch k {
		case "Date/Time":
			data.dateTime, err = time.Parse(
				"20060102T15:04:05.999999999Z", v)
		case "tls":
			data.tls, err = strconv.ParseBool(v)
		case "websockets":
			data.websockets, err = strconv.ParseBool(v)
		case "Summary data":
			err = json.Unmarshal(
				[]byte(`{"summaryData":[`+v+`]}`),
				&data)
		default:
			data.fields[k] = v
		}
		if err != nil {
			return nil, err
		}
	}
	return &data, nil
}

// parseMetaFile converts the raw content into key value map.
// Meta file has .meta suffix, and contains mostly key value pairs separated by ':', some with no value, e.g.
// Date/Time: 20170512T17:55:18.538553000Z
// c2s_snaplog file: 20170512T17:55:18.538553000Z_94.197.121.150.threembb.co.uk:54430.c2s_snaplog.gz
// ...
// server IP address:
// server hostname: mlab1.lhr01.measurement-lab.org
// ...
// Summary data: 0,0,2952,0,0,0,...
// * Additional data:
// tls: true
// websockets: true
//
// Notable exception is the * Additional data: line, which also parses as a key value pair, but isn't.
// TODO pass fileInfoAndData
func parseMetaFile(rawContent []byte) (map[string]string, error) {
	result := make(map[string]string, 20)

	buf := bytes.NewBuffer(rawContent)
	var err error
	var line string
	for {
		line, err = buf.ReadString('\n')
		if err != nil {
			break
		}
		kv := strings.SplitN(line, ":", 2)
		if len(kv) != 2 {
			// Does this trigger for " * Additional data:"?
			// TODO Error message or counter?
			continue
		}
		// TODO(dev) - filter out binary data that sometimes shows up in corrupted files.
		result[kv[0]] = kv[1]
	}
	if err != io.EOF {
		// TODO Error message or counter?
		return nil, err
	}
	return result, nil
}

// Process the meta test data.
// TODO(dev) - add unit tests
// TODO(prod) - For tests that include a meta file, should respect the test filenames.
// See ndt_meta_log_parser_lib.cc
func (n *NDTParser) processMeta(testName string, content []byte) {
	// Create a map from the metafile raw content
	metamap, err := parseMetaFile(content)
	if err != nil {
		metrics.TestCount.WithLabelValues(
			n.TableName(), n.inserter.TableSuffix(), "meta", "error").Inc()
		log.Println("meta processing error: " + err.Error())
		return
	}
	n.metaFile, err = createMetaFileData(testName, metamap)
	if err != nil {
		metrics.TestCount.WithLabelValues(
			n.TableName(), n.inserter.TableSuffix(), "meta", "error").Inc()
		log.Println("meta processing error: " + err.Error())
		return
	}

	metrics.TestCount.WithLabelValues(
		n.TableName(), n.inserter.TableSuffix(), "meta", "ok").Inc()
	return
}
