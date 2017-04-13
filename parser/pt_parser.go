// PT parser pasres the Paris TraceRoute .web100 files, and output a ValueSaver
package parser

import (
	"bufio"
	"fmt"
	//"io"
	"cloud.google.com/go/bigquery"
	"os"
	"strings"
	//"golang.org/x/net/context"
	//"google.golang.org/api/iterator"
)

// PT implements the ValueSaver interface.
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

// The input is filebase name,
func PTParser(fn string) (bigquery.ValueSaver, error) {
	file, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// The filename contains 5-tuple like 20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris
	// We can get the logtime, local IP, local port, server IP, server port from fn directly
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		oneLine := strings.TrimSuffix(scanner.Text(), "\n")
		fmt.Println(oneLine)
	}
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
	return one_row, nil

}
