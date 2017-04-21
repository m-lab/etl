// Parse PT filename like 20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris
package parser

import (
	"cloud.google.com/go/bigquery"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

type PTFileName struct {
	name string
}

// GetLocalIP parse the filename and return IP.
func (f *PTFileName) GetIPTuple() (string, string, string, string) {
	firstIPStart := strings.IndexByte(f.name, '-')
	first_segment := f.name[firstIPStart+1 : len(f.name)]
	firstPortStart := strings.IndexByte(first_segment, '-')
	second_segment := first_segment[firstPortStart+1 : len(first_segment)]
	secondIPStart := strings.IndexByte(second_segment, '-')
	third_segment := second_segment[secondIPStart+1 : len(second_segment)]
	secondPortStart := strings.IndexByte(third_segment, '-')
	secondPortEnd := strings.LastIndexByte(third_segment, '.')
	return first_segment[0:firstPortStart], second_segment[0:secondIPStart], third_segment[0:secondPortStart], third_segment[secondPortStart+1 : secondPortEnd]
}

func (f *PTFileName) GetDate() string {
	return f.name[0:8]
}

type FileNameParser interface {
	GetIPTuple()
	GetDate()
}

type PT struct {
	test_id  string
	project  int
	log_time int
	connection_spec
}

type connection_spec struct {
	server_ip      string
	server_af      int
	client_ip      string
	client_af      int
	data_direction int
}

// Save implements the ValueSaver interface.
func (i *PT) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"test_id":  i.test_id,
		"project":  i.project,
		"log_time": i.log_time,
	}, "", nil
}

type PTParser struct {
	Parser
	tmpDir string
}

func (pt *PTParser) Parse(meta map[string]bigquery.Value, fileName string, tableID string, rawContent []byte) (interface{}, error) {
	tmpFile := fmt.Sprintf("%s/%s", pt.tmpDir, fileName)
	err := ioutil.WriteFile(tmpFile, rawContent, 0644)
	if err != nil {
		return nil, err
	}
	// TODO(dev): log possible remove errors.
	defer os.Remove(tmpFile)

	one_row := &PT{
		test_id:  "20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris.gz",
		project:  3,
		log_time: 1234,
		connection_spec: connection_spec{
			server_ip:      "1:2.3.4",
			server_af:      2,
			client_ip:      "4.3.2.1",
			client_af:      2,
			data_direction: 0,
		},
	}
	data := []*PT{one_row}
	fmt.Printf("%v\n", data)
	return nil, nil
}
