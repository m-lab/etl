package parser

import (
	//"bytes"
	//"compress/gzip"
	"cloud.google.com/go/bigquery"
	"encoding/json"
	"errors"
	//"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/etl"
	//"github.com/m-lab/etl/metrics"
	//"github.com/m-lab/etl/schema"
	//"github.com/m-lab/etl/web100"
	//"io/ioutil"
	"log"
	"strconv"
	"strings"
)

// Result types
// ------------
//
// Here we define the types representing the different results.
//
// TODO(bassosimone): explain why we're cautious.

/*
{"latency": "0.075597235373", "privacy_informed": "1", "download_speed": "546624.492692", "real_address": "93.85.18.232", "neubot_version": "0.004016009", "timestamp": 1498607668, "connect_time": "0.0616570685961", "remote_address": "213.248.112.83", "privacy_can_share": "1", "platform": "win32", "upload_speed": "71078.6046005", "test_version": "1", "internal_address": "192.168.100.4", "privacy_can_collect": "1", "uuid": "1571720d-91c8-4b96-8661-f3db6ab9321d"}
*/

type speedtestResult struct {
	ConnectTime float64
	DownloadSpeed float64
	InternalAddress string
	Latency float64
	NeubotVersion float64
	Platform string
	PrivacyCanCollect bool
	PrivacyCanShare bool
	PrivacyInformed bool
	RealAddress string
	RemoteAddress string
	TestVersion int64
	Timestamp int64
	UUID string
	UploadSpeed float64
}

func robustAccessToString(v interface{}) (string, error) {
	str, okay := v.(string)
	if !okay {
		return "", errors.New("Value is not string")
	}
	if str == "" {
		return "", errors.New("Value is the empty string")
	}
	return str, nil
}

func robustAccessToFloat64(v interface{}) (float64, error) {
	value, okay := v.(float64)
	if okay {
		return value, nil
	}
	str, okay := v.(string)
	if !okay {
		return 0.0, errors.New("Value is neither float64 not string")
	}
	return strconv.ParseFloat(str, 64)
}

func robustAccessToInt64(v interface{}) (int64, error) {
	value, okay := v.(int64)
	if okay {
		return value, nil
	}
	str, okay := v.(string)
	if !okay {
		return 0, errors.New("Value is neither int64 not string")
	}
	return strconv.ParseInt(str, 10, 64)
}

func robustAccessToBool(v interface{}) (bool, error) {
	value, okay := v.(bool);
	if okay {
		return value, nil
	}
	ival, err := robustAccessToInt64(v)
	if err != nil {
		return false, err;
	}
	if ival != 0 {
		return true, nil
	}
	return ival != 0, nil
}

// Neubot parser
// -------------
//
// Here we define the Neubot parser structure and we define the functions
// required to fully implement the etl.Parser interface.

// NeubotParser is a parser for Neubot data.
type NeubotParser struct {
	inserter etl.Inserter
	etl.RowStats
}

// NewNeubotParser returns a new Neubot parser.
func NewNeubotParser(inserter etl.Inserter) *NeubotParser {
	return &NeubotParser{
		inserter: inserter,
		RowStats: inserter, // Will provide the RowStats interface.
	}
}

// Functions required to fully implement the etl.Parser interface:

// IsParsable reports a canonical file "kind" and whether such file appears to
// be parsable based on the name and content size.
func (n *NeubotParser) IsParsable(testName string, test []byte) (string, bool) {
	// TODO(bassosimone): validate more precisely the file name.
	if len(test) == 0 {
		// TODO(bassosimone): okay to return "invalid"?
		return "invalid", false
	}
	if strings.HasSuffix(testName, "_speedtest.gz") {
		return "speedtest", true
	}
	if strings.HasSuffix(testName, "_bittorrent.gz") {
		return "bittorrent", true
	}
	if strings.HasSuffix(testName, "_raw.gz") {
		return "raw", true
	}
	if strings.HasSuffix(testName, "_dash.gz") {
		return "dash", true
	}
	// TODO(bassosimone): here I should probably update stats.
	return "unknown", false
}

func (n *NeubotParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error {
	log.Println(testName)
	// Implementation note: judging from parser/ndt.go it is possible that
	// rsync collected both gunzipped and non gunzipped files. We don't care
	// because we always only consider files ending in `.gz`.
	if strings.HasSuffix(testName, "_speedtest.gz") {
		log.Printf("MSG: %s", test)
		var m map[string]interface{}
		err := json.Unmarshal(test, &m)
		if err != nil {
			return err;
		}
		result := speedtestResult{}
		result.ConnectTime, err = robustAccessToFloat64(m["connect_time"])
		if err != nil {
			return err
		}
		result.DownloadSpeed, err = robustAccessToFloat64(m["download_speed"])
		if err != nil {
			return err
		}
		result.InternalAddress, err = robustAccessToString(m["internal_address"])
		if err != nil {
			return err
		}
		// TODO(bassosimone): finish parsing the whole structure
		return errors.New("not yet implemented")
	}
	if strings.HasSuffix(testName, "_bittorrent.gz") {
		return errors.New("not yet implemented")
	}
	if strings.HasSuffix(testName, "_raw.gz") {
		return errors.New("not yet implemented")
	}
	if strings.HasSuffix(testName, "_dash.gz") {
		return errors.New("not yet implemented")
	}
	// TODO(bassosimone): should we panic() here?
	return errors.New("received a file that we should not parse")
}

// Flush flushes any pending rows.
func (n *NeubotParser) Flush() error {
	return errors.New("not yet implemented")
}

// Base name of the BQ table that we push to (i.e. no date).
func (n *NeubotParser) TableName() string {
	return n.inserter.TableBase()
}

// Full name of the BQ table that we push to (i.e. including date).
func (n *NeubotParser) FullTableName() string {
	return n.inserter.FullTableName()
}

// TaskError returns whether we consider this task failed.
func (n *NeubotParser) TaskError() error {
	// TODO(bassosimone): implement proper error checks.
	return nil
}
