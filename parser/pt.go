// Parse PT filename like 20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris
package parser

import (
	"bufio"
	"cloud.google.com/go/bigquery"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type PTFileName struct {
	name string
}

// GetLocalIP parse the filename and return IP.
// TODO(dev): use regex parser.
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

func (f *PTFileName) GetDate() (string, bool) {
	if len(f.name) > 18 {
		// Return date string in format "20170320T23:53:10Z"
		return f.name[0:18], true
	}
	return "", false
}

type FileNameParser interface {
	GetIPTuple()
	GetDate()
}

// MLabSnapshot in legacy code
type PT struct {
	test_id              string
	project              int // 3 for PARIS_TRACEROUTE
	log_time             int64
	connection_spec      MLabConnectionSpecification
	paris_traceroute_hop ParisTracerouteHop
}

type MLabConnectionSpecification struct {
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

type Node struct {
	hostname string
	ip       string
	rtts     []float64
	parent   *Node

	// For a given hop in a paris traceroute, there may be multiple IP
	// addresses. Each one belongs to a flow, which is an independent path from
	// the source to the destination IP. Some hops only have a single flow which
	// is given the -1 value. Any specific flows are numbered
	// sequentially starting from 0.
	flow int
}

type ParisTracerouteHop struct {
	protocal         string
	src_ip           string
	src_af           int32
	dest_ip          string
	dest_af          int32
	src_hostname     string
	des_hostname     string
	rtt              []float64
	src_geolocation  GeolocationIP
	dest_geolocation GeolocationIP
}

type GeolocationIP struct {
	continent_code string
	country_code   string
	country_code3  string
	country_name   string
	region         string
	metro_code     int64
	city           string
	area_code      int64
	postal_code    string
	latitude       float64
	longitude      float64
}

func ProcessAllNodes(all_nodes []Node, server_IP string) []ParisTracerouteHop {
	var results []ParisTracerouteHop
	if len(all_nodes) == 0 {
		return nil
	}

	// Iterate from the end of the list of nodes to minimize cost of removing nodes.
	for i := len(all_nodes) - 1; i >= 0; i-- {
		parent := all_nodes[i].parent
		if parent == nil {
			one_hop := &ParisTracerouteHop{
				dest_ip:      all_nodes[i].ip,
				des_hostname: all_nodes[i].hostname,
				rtt:          all_nodes[i].rtts,
				src_ip:       server_IP,
			}
			fmt.Println(*one_hop)
			results = append(results, *one_hop)
			break
		} else {
			one_hop := &ParisTracerouteHop{
				dest_ip:      all_nodes[i].ip,
				des_hostname: all_nodes[i].hostname,
				rtt:          all_nodes[i].rtts,
				src_ip:       parent.ip,
				src_hostname: parent.hostname,
			}
			fmt.Println(*one_hop)
			results = append(results, *one_hop)
		}
	}
	return results
}

func Unique(one_node Node, list []Node) bool {
	for _, existing_node := range list {
		if existing_node.hostname == one_node.hostname && existing_node.ip == one_node.ip && existing_node.flow == one_node.flow {
			return false
		}
	}
	return true
}

// Handle the first line, like
// "traceroute [(64.86.132.76:33461) -> (98.162.212.214:53849)], protocol icmp, algo exhaustive, duration 19 s"
func ParseFirstLine(oneLine string) (protocal string) {
	parts := strings.Split(oneLine, ",")
	// check protocol
	// check algo
	for _, part := range parts {
		mm := strings.Split(strings.TrimSpace(part), " ")
		if len(mm) > 1 {
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
				}
			}
		}
	}
	return protocal
}

func GetLogtime(filename PTFileName) int64 {
	date, _ := filename.GetDate()
	// data is in format like "20170320T23:53:10Z"
	revised_date := date[0:4] + "-" + date[4:6] + "-" + date[6:18]
	fmt.Println(revised_date)

	t, err := time.Parse(time.RFC3339, revised_date)
	if err != nil {
		fmt.Println(err)
		return 0
	}

	return t.Unix()
}

func (pt *PTParser) Parse(meta map[string]bigquery.Value, fileName string, tableID string, rawContent []byte) (interface{}, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Get the logtime
	fn := PTFileName{name: filepath.Base(fileName)}

	dest_IP, _, server_IP, _ := fn.GetIPTuple()
	fmt.Println(dest_IP)
	fmt.Println(server_IP)

	t := GetLogtime(fn)
	fmt.Println(t)
	// The filename contains 5-tuple like 20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris
	// We can get the logtime, local IP, local port, server IP, server port from fileName directly
	scanner := bufio.NewScanner(file)
	is_first_line := true
	protocal := "icmp"
	// This var keep all current leaves
	var current_leaves []Node
	// This var keep all possible nodes
	var all_nodes []Node
	for scanner.Scan() {
		oneLine := strings.TrimSuffix(scanner.Text(), "\n")
		fmt.Println(oneLine)
		// Skip initial lines starting with #.
		if len(oneLine) == 0 || oneLine[0] == '#' {
			continue
		}
		// This var keep all new leaves
		var new_leaves []Node
		if is_first_line {
			is_first_line = false
			protocal = ParseFirstLine(oneLine)
		} else {
			// Handle each line of test file after the first line.
			parts := strings.Fields(oneLine)
			// Skip line start with "MPLS"
			if len(parts) < 3 || parts[0] == "MPLS" {
				continue
			}

			// Drop the first 3 parts, like "1  P(6, 6)" because they are useless.
			// The following pars are grouped into 4 tuples, for each 4 tuples, it is like:
			// parts[3] is the hostname, like "if-ae-10-3.tcore2.DT8-Dallas.as6453.net".
			// parts[4] is IP address like "(66.110.57.41)" or "(72.14.218.190):0,2,3,4,6,8,10"
			// parts[5] are rtt in numbers like "0.298/0.318/0.340/0.016"
			// parts[6] should walys be "ms"
			// if there is parts[7], it should be hostname again like parts[3] ......
			for i := 3; i < len(parts); i += 4 {
				if parts[i+3] != "ms" {
					log.Fatal("Malformed line. Expected 'ms'")
				}
				var rtt []float64
				// Handle tcp or udp, parts[5] is a single number.
				if protocal == "tcp" || protocal == "udp" {
					fmt.Println(parts[i+2])
					one_rtt, err := strconv.ParseFloat(parts[i+2], 64)
					if err == nil {
						rtt = append(rtt, one_rtt)
					} else {
						log.Fatal("Failed to conver rtt to number with error %v", err)
					}
				}
				// Handle icmp, parts[5] has 4 numbers separated by "/"
				if protocal == "icmp" {
					nums := strings.Split(parts[i+2], "/")
					if len(nums) != 4 {
						log.Fatal("Failed to parse rtts for icmp test. 4 numbers expected")
					}
					for _, num := range nums {
						one_rtt, err := strconv.ParseFloat(num, 64)
						if err == nil {
							rtt = append(rtt, one_rtt)
						} else {
							log.Fatal("Failed to conver rtt to number with error %v", err)
						}
					}
				}
				// check whether it is single flow or mulitple flows
				// sample of miltiple flows: (72.14.218.190):0,2,3,4,6,8,10
				// sample of single flows: (172.25.252.166)
				parts_4 := parts[i+1]
				ips := strings.Split(parts_4, ":")

				// Check whether it is root node.
				if len(all_nodes) == 0 {
					one_node := &Node{
						hostname: parts[i],
						ip:       ips[0][1 : len(ips[0])-1],
						rtts:     rtt,
						parent:   nil,
						flow:     -1,
					}
					all_nodes = append(all_nodes, *one_node)
					new_leaves = append(new_leaves, *one_node)
					break
				}
				if len(ips) == 1 {
					// Single flow will be son of all current leaves
					for _, leaf := range current_leaves {
						one_node := &Node{
							hostname: parts[i],
							ip:       ips[0][1 : len(ips[0])-1],
							rtts:     rtt,
							parent:   &leaf,
							flow:     -1,
						}
						all_nodes = append(all_nodes, *one_node)
						if Unique(*one_node, new_leaves) {
							new_leaves = append(new_leaves, *one_node)
						}
					}
				} else if len(ips) == 2 {
					// Create a leave for each flow.
					flows := strings.Split(ips[1], ",")
					for _, flow := range flows {
						flow_int, _ := strconv.Atoi(flow)

						for _, leaf := range current_leaves {
							if leaf.flow == -1 || leaf.flow == flow_int {
								one_node := &Node{
									hostname: parts[i],
									ip:       ips[0][1 : len(ips[0])-1],
									rtts:     rtt,
									parent:   &leaf,
									flow:     flow_int,
								}
								all_nodes = append(all_nodes, *one_node)
								if Unique(*one_node, new_leaves) {
									new_leaves = append(new_leaves, *one_node)
								}
							}
						}
					}
				} // Done with multiple flows

			} // Done with a 4-tuple parsing
		} // Done with one line
		current_leaves = new_leaves
		fmt.Println("Here are new leaves")
		for _, leaf := range current_leaves {
			fmt.Println(leaf)
		}

	} // Done with a test file

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	// Generate Hops from all_nodes
	PT_hops := ProcessAllNodes(all_nodes, server_IP)
	fmt.Println(len(PT_hops))
	return nil, nil
}
