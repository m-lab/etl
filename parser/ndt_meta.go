package parser

// ndt_meta.go contains code for processing the ndt .meta files.

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/etl/web100"
)

// MetaFileData is the parsed info from the .meta file.
type MetaFileData struct {
	TestName    string
	DateTime    time.Time
	SummaryData []int32 // Note: this is ignored in the legacy pipeline.
	Tls         bool
	Websockets  bool

	Fields map[string]string // All of the string fields.
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

	// Some client fields are "Additional" meta data optionally provided by the client.
	// The NDT client names these fields differently than the server.
	// Other clients may provide different key names.
	"client.kernel.version": "client_kernel_version",
	"client.version":        "client_version",

	// NDT SSL added two additional meta fields to signify whether the test was
	// a websocket and/or tls test.
	"tls":        "tls",
	"websockets": "websockets",
}

func handleIP(connSpec schema.Web100ValueMap, prefix string, ipString string) {
	connSpec.SetString(prefix+"_ip", ipString)
	if web100.ValidateIP(ipString) != nil {
		log.Printf("Failed parsing connSpec IP: %s\n", ipString)
		metrics.WarningCount.WithLabelValues(
			"ndt", "unknown", "failed parsing connSpec IP").Inc()
	} else {
		connSpec.SetString(prefix+"_ip", ipString)
		family := web100.ParseIPFamily(ipString)
		if family != -1 {
			connSpec.SetInt64(prefix+"_af", family)
		}
	}
}

func (mfd *MetaFileData) PopulateConnSpec(connSpec schema.Web100ValueMap) {
	// This populates most of the fields...
	for k, v := range fieldPairs {
		s, ok := mfd.Fields[k]
		if ok {
			if s != "" {
				connSpec.SetString(v, s)
			}
		}
	}

	// Only set the value for tls & websocket if the field is present.
	if s, ok := mfd.Fields["tls"]; ok {
		if s != "" {
			connSpec.SetBool("tls", mfd.Tls)
		}
	}
	if s, ok := mfd.Fields["websockets"]; ok {
		if s != "" {
			connSpec.SetBool("websockets", mfd.Websockets)
		}
	}

	// Note: this field is usually empty in legacy NDT .meta files.
	s, ok := connSpec["server_ip"]
	// TODO - extract function for this stanza
	if ok {
		if s != "" {
			handleIP(connSpec, "server", s.(string))
		}
	} else {
		metrics.WarningCount.WithLabelValues(
			"ndt", "unknown", "missing server_ip").Inc()
	}

	s, ok = connSpec["client_ip"]
	if ok {
		if s != "" {
			handleIP(connSpec, "client", s.(string))
		}
	} else {
		log.Println("client_ip missing from .meta")
		metrics.WarningCount.WithLabelValues(
			"ndt", "unknown", "missing client_ip").Inc()
	}
}

// createMetaFileData uses the key:value pairs to populate the interpreted fields.
// TODO(dev) - more unit tests?
// TODO(dev) - move to separate file - meta.go
func createMetaFileData(testName string, fields map[string]string) (*MetaFileData, error) {
	var data MetaFileData
	data.TestName = testName
	data.Fields = make(map[string]string, 20)
	for k, v := range fields {
		var err error
		v = strings.TrimSpace(v)
		switch k {
		case "Date/Time":
			data.DateTime, err = time.Parse(
				"20060102T15:04:05.999999999Z", v)
		case "tls":
			data.Tls, err = strconv.ParseBool(v)
			data.Fields[k] = v
		case "websockets":
			data.Websockets, err = strconv.ParseBool(v)
			data.Fields[k] = v
		case "Summary data":
			err = json.Unmarshal(
				[]byte(`{"SummaryData":[`+v+`]}`),
				&data)
		default:
			data.Fields[k] = v
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

// ProcessMetaFile parses the .meta file.
// TODO(dev) - add unit tests
// TODO(prod) - For tests that include a meta file, should respect the test filenames.
// See ndt_meta_log_parser_lib.cc
func ProcessMetaFile(tableName string, suffix string, testName string, content []byte) *MetaFileData {
	// Create a map from the metafile raw content
	metamap, err := parseMetaFile(content)
	if err != nil {
		metrics.TestCount.WithLabelValues(
			tableName, "meta", "error").Inc()
		log.Println("meta processing error: " + err.Error())
		return nil
	}
	metaFile, err := createMetaFileData(testName, metamap)
	if err != nil {
		metrics.TestCount.WithLabelValues(
			tableName, "meta", "error").Inc()
		log.Println("meta processing error: " + err.Error())
		return nil
	}

	metrics.TestCount.WithLabelValues(
		tableName, "meta", "ok").Inc()
	return metaFile
}
