// Parse PT filename like 20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris
package parser

import (
	"bufio"
	"cloud.google.com/go/bigquery"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
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
	// Return date string in format "20170320T23:53:10Z"
	return f.name[0:18]
}

type FileNameParser interface {
	GetIPTuple()
	GetDate()
}

type PT struct {
	test_id  string
	project  int
	log_time int64
	connection_spec
}

type connection_spec struct {
	server_ip      string
	server_af      int
	client_ip      string
	client_af      int
	data_direction int // 0 for SERVER_TO_CLIENT
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
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Get the logtime
	fn := PTFileName{name: filepath.Base(fileName)}
	date := fn.GetDate()
	dest_IP, _, server_IP, _ := fn.GetIPTuple()

	//layout := "2012-11-01T22:08:41+00:00"
	// data is in format like "20170320T23:53:10Z"
	revised_date := date[0:4] + "-" + date[4:6] + "-" + date[6:18]
	fmt.Println(revised_date)
	t, err := time.Parse(time.RFC3339, revised_date)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(t.Unix())

	// The filename contains 5-tuple like 20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris
	// We can get the logtime, local IP, local port, server IP, server port from fileName directly
	scanner := bufio.NewScanner(file)
	is_first_line := true
	protocal := "icmp"
	for scanner.Scan() {
		oneLine := strings.TrimSuffix(scanner.Text(), "\n")
		fmt.Println(oneLine)
		// Skip initial lines starting with #.
		if len(oneLine) == 0 || oneLine[0] == '#' {
			continue
		}
		if is_first_line {
			is_first_line = false
			// Handle the first line, like
			// "traceroute [(64.86.132.76:33461) -> (98.162.212.214:53849)], protocol icmp, algo exhaustive, duration 19 s"
			parts := strings.Split(oneLine, ",")
			// check protocol
			// check algo
			for _, part := range parts {
				mm := strings.Split(strings.TrimSpace(part), " ")
				if len(mm) > 1 {
					fmt.Println(mm[0])
					if mm[0] == "algo" {
						if mm[1] != "exhaustive" {
							log.Fatal("Unexpected algorithm")
						}
					}
					if mm[0] == "protocol" {
						if mm[1] != "icmp" && mm[1] != "udp" && mm[1] != "tcp" {
							log.Fatal("Unknown protocol")
						} else {
							protocal = mm[1]
							fmt.Println(protocal)
						}
					}
				}
			}
		} else {
			// Handle each line of hops
			// Handle icmp
			// Handle tcp or udp

		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	one_row := &PT{
		test_id:  "20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris.gz",
		project:  3,
		log_time: t.Unix(),
		connection_spec: connection_spec{
			server_ip:      server_IP,
			server_af:      2,
			client_ip:      dest_IP,
			client_af:      2,
			data_direction: 0,
		},
	}
	data := []*PT{one_row}
	fmt.Printf("%v\n", data)
	return nil, nil
}
