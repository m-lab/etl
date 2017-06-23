// Parse PT filename like 20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris
// The format of test file can be found at https://paris-traceroute.net/.
package parser

import (
	"cloud.google.com/go/bigquery"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/schema"
)

type PTFileName struct {
	Name string
}

// GetLocalIP parse the filename and return IP.
// TODO(dev): use regex parser.
func (f *PTFileName) GetIPTuple() (string, string, string, string) {
	firstIPStart := strings.IndexByte(f.Name, '-')
	first_segment := f.Name[firstIPStart+1 : len(f.Name)]
	firstPortStart := strings.IndexByte(first_segment, '-')
	second_segment := first_segment[firstPortStart+1 : len(first_segment)]
	secondIPStart := strings.IndexByte(second_segment, '-')
	third_segment := second_segment[secondIPStart+1 : len(second_segment)]
	secondPortStart := strings.IndexByte(third_segment, '-')
	secondPortEnd := strings.LastIndexByte(third_segment, '.')
	return first_segment[0:firstPortStart], second_segment[0:secondIPStart], third_segment[0:secondPortStart], third_segment[secondPortStart+1 : secondPortEnd]
}

func (f *PTFileName) GetDate() (string, bool) {
	if len(f.Name) > 18 {
		// Return date string in format "20170320T23:53:10Z"
		return f.Name[0:18], true
	}
	return "", false
}

type PTParser struct {
	inserter etl.Inserter
	etl.RowStats
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

const IPv4_AF int32 = 2
const IPv6_AF int32 = 10

func NewPTParser(ins etl.Inserter) *PTParser {
	return &PTParser{ins, ins}
}

// ProcessAllNodes take the array of the Nodes, and generate one ParisTracerouteHop entry from each node.
func ProcessAllNodes(all_nodes []Node, server_IP, protocol string) []schema.ParisTracerouteHop {
	var results []schema.ParisTracerouteHop
	if len(all_nodes) == 0 {
		return nil
	}

	// Iterate from the end of the list of nodes to minimize cost of removing nodes.
	for i := len(all_nodes) - 1; i >= 0; i-- {
		parent := all_nodes[i].parent
		if parent == nil {
			one_hop := &schema.ParisTracerouteHop{
				Protocol:      protocol,
				Dest_ip:       all_nodes[i].ip,
				Dest_hostname: all_nodes[i].hostname,
				Rtt:           all_nodes[i].rtts,
				Src_ip:        server_IP,
				Src_af:        IPv4_AF,
				Dest_af:       IPv4_AF,
			}
			results = append(results, *one_hop)
			break
		} else {
			one_hop := &schema.ParisTracerouteHop{
				Protocol:      protocol,
				Dest_ip:       all_nodes[i].ip,
				Dest_hostname: all_nodes[i].hostname,
				Rtt:           all_nodes[i].rtts,
				Src_ip:        parent.ip,
				Src_hostname:  parent.hostname,
				Src_af:        IPv4_AF,
				Dest_af:       IPv4_AF,
			}
			results = append(results, *one_hop)
		}
	}
	return results
}

// This function was designed for hops with multiple flows. When the source IP are duplicate flows, but the destination IP is
// single flow IP, those hops will result in just one node in the list.
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
func ParseFirstLine(oneLine string) (protocol string) {
	parts := strings.Split(oneLine, ",")
	// check protocol
	// check algo
	for _, part := range parts {
		mm := strings.Split(strings.TrimSpace(part), " ")
		if len(mm) > 1 {
			if mm[0] == "algo" {
				if mm[1] != "exhaustive" {
					log.Printf("Unexpected algorithm")
				}
			}
			if mm[0] == "protocol" {
				if mm[1] != "icmp" && mm[1] != "udp" && mm[1] != "tcp" {
					log.Printf("Unknown protocol")
					return ""
				} else {
					protocol = mm[1]
				}
			}
		}
	}
	return protocol
}

func GetLogtime(filename PTFileName) int64 {
	date, _ := filename.GetDate()
	// data is in format like "20170320T23:53:10Z"
	revised_date := date[0:4] + "-" + date[4:6] + "-" + date[6:18]

	t, err := time.Parse(time.RFC3339, revised_date)
	if err != nil {
		fmt.Println(err)
		return 0
	}

	return t.Unix()
}

func (pt *PTParser) TableName() string {
	return pt.inserter.TableBase()
}

func (pt *PTParser) FullTableName() string {
	return pt.inserter.FullTableName()
}

func (pt *PTParser) Flush() error {
	return pt.inserter.Flush()
}

func CreateTestId(fn string) string {
	base_name := filepath.Base(fn)
	// base_name is in format like 20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris
	// test_id is in format like 2017/05/01/mlab1.lga06/20170501T23:58:07Z-72.228.158.51-40835-128.177.119.209-8080.paris.gz
	// TODO: get site info and add to test_id.
	return base_name
}

func (pt *PTParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, rawContent []byte) error {
	hops, logTime, conn_spec, err := Parse(meta, testName, rawContent)
	if err != nil {
		metrics.ErrorCount.WithLabelValues(
			pt.TableName(), "pt-test", "insert-err").Inc()
		metrics.TestCount.WithLabelValues(
			pt.TableName(), "pt", "corrupted content").Inc()
		log.Println(err)
		return err
	}
	test_id := CreateTestId(testName)
	metrics.TestCount.WithLabelValues(pt.TableName(), "pt", "ok").Inc()
	for _, hop := range hops {
		pt_test := schema.PT{
			Test_id:              test_id,
			Log_time:             logTime,
			Connection_spec:      *conn_spec,
			Paris_traceroute_hop: hop,
			Type:                 int32(2),
			Project:              int32(3),
		}
		err := pt.inserter.InsertRow(pt_test)
		if err != nil {
			metrics.ErrorCount.WithLabelValues(
				pt.TableName(), "pt-hop", "insert-err").Inc()
			metrics.PTHopCount.WithLabelValues(
				pt.TableName(), "pt", "insert-err").Inc()
			log.Printf("insert-err: %v\n", err)
			return err
		} else {
			metrics.PTHopCount.WithLabelValues(
				pt.TableName(), "pt", "ok").Inc()
		}
	}
	return nil
}

// For each 4 tuples, it is like:
// parts[0] is the hostname, like "if-ae-10-3.tcore2.DT8-Dallas.as6453.net".
// parts[1] is IP address like "(66.110.57.41)" or "(72.14.218.190):0,2,3,4,6,8,10"
// parts[2] are rtt in numbers like "0.298/0.318/0.340/0.016"
// parts[3] should always be "ms"
func ProcessOneTuple(parts []string, protocol string, current_leaves []Node, all_nodes, new_leaves *[]Node) error {
	if len(parts) != 4 {
		return errors.New("corrupted input")
	}
	if parts[3] != "ms" {
		return errors.New("Malformed line. Expected 'ms'")
	}
	var rtt []float64
	//TODO: to use regexp here.
	switch {
	// Handle tcp or udp, parts[2] is a single number.
	case protocol == "tcp" || protocol == "udp":
		one_rtt, err := strconv.ParseFloat(parts[2], 64)
		if err == nil {
			rtt = append(rtt, one_rtt)
		} else {
			log.Println("Failed to conver rtt to number with error %v", err)
			return err
		}

	// Handle icmp, parts[2] has 4 numbers separated by "/"
	case protocol == "icmp":
		nums := strings.Split(parts[2], "/")
		if len(nums) != 4 {
			return errors.New("Failed to parse rtts for icmp test. 4 numbers expected")
		}
		for _, num := range nums {
			one_rtt, err := strconv.ParseFloat(num, 64)
			if err == nil {
				rtt = append(rtt, one_rtt)
			} else {
				fmt.Printf("Failed to conver rtt to number with error %v", err)
				return err
			}
		}
	}
	// check whether it is single flow or mulitple flows
	// sample of multiple flows: (72.14.218.190):0,2,3,4,6,8,10
	// sample of single flows: (172.25.252.166)
	ips := strings.Split(parts[1], ":")

	// Check whether it is root node.
	if len(*all_nodes) == 0 {
		one_node := &Node{
			hostname: parts[0],
			ip:       ips[0][1 : len(ips[0])-1],
			rtts:     rtt,
			parent:   nil,
			flow:     -1,
		}
		*all_nodes = append(*all_nodes, *one_node)
		*new_leaves = append(*new_leaves, *one_node)
		return nil
	}
	// There are duplicates in all_nodes, but not in new_leaves.
	// TODO(dev): consider consolidating these with a repeat count.
	switch len(ips) {
	case 1:
		// For single flow, the new node will be son of all current leaves
		for _, leaf := range current_leaves {
			one_node := &Node{
				hostname: parts[0],
				ip:       ips[0][1 : len(ips[0])-1],
				rtts:     rtt,
				parent:   &leaf,
				flow:     -1,
			}
			*all_nodes = append(*all_nodes, *one_node)
			if Unique(*one_node, *new_leaves) {
				*new_leaves = append(*new_leaves, *one_node)
			}
		}
	case 2:
		// Create a leave for each flow.
		flows := strings.Split(ips[1], ",")
		for _, flow := range flows {
			flow_int, err := strconv.Atoi(flow)
			if err != nil {
				return err
			}

			for _, leaf := range current_leaves {
				if leaf.flow == -1 || leaf.flow == flow_int {
					one_node := &Node{
						hostname: parts[0],
						ip:       ips[0][1 : len(ips[0])-1],
						rtts:     rtt,
						parent:   &leaf,
						flow:     flow_int,
					}
					*all_nodes = append(*all_nodes, *one_node)
					if Unique(*one_node, *new_leaves) {
						*new_leaves = append(*new_leaves, *one_node)
					}
				}
			}
		}
	default:
		return errors.New("Wrong format for IP address.")
	}
	return nil
}

// Parse the raw test file into hops ParisTracerouteHop.
// TODO(dev): dedup the hops that are identical.
func Parse(meta map[string]bigquery.Value, testName string, rawContent []byte) ([]schema.ParisTracerouteHop, int64, *schema.MLabConnectionSpecification, error) {
	// log.Printf("%s", testName)

	metrics.WorkerState.WithLabelValues("pt").Inc()
	defer metrics.WorkerState.WithLabelValues("pt").Dec()

	// Get the logtime
	fn := PTFileName{Name: filepath.Base(testName)}
	dest_IP, _, server_IP, _ := fn.GetIPTuple()
	t := GetLogtime(fn)

	conn_spec := &schema.MLabConnectionSpecification{
		Server_ip:      server_IP,
		Server_af:      IPv4_AF,
		Client_ip:      dest_IP,
		Client_af:      IPv4_AF,
		Data_direction: 0,
	}

	// The filename contains 5-tuple like 20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris
	// We can get the logtime, local IP, local port, server IP, server port from fileName directly
	is_first_line := true
	protocol := "icmp"
	// This var keep all current leaves
	var current_leaves []Node
	// This var keep all possible nodes
	var all_nodes []Node

	for _, oneLine := range strings.Split(string(rawContent[:]), "\n") {
		oneLine := strings.TrimSuffix(oneLine, "\n")
		// Skip initial lines starting with #.
		if len(oneLine) == 0 || oneLine[0] == '#' {
			continue
		}
		// This var keep all new leaves
		var new_leaves []Node
		if is_first_line {
			is_first_line = false
			protocol = ParseFirstLine(oneLine)
		} else {
			// Handle each line of test file after the first line.
			// TODO(dev): use regexp here
			parts := strings.Fields(oneLine)
			// Skip line start with "MPLS"
			if len(parts) < 3 || parts[0] == "MPLS" {
				continue
			}

			// Drop the first 3 parts, like "1  P(6, 6)" because they are useless.
			// The following parts are grouped into tuples, each with 4 parts:
			for i := 3; i < len(parts); i += 4 {
				tuple_str := []string{parts[i], parts[i+1], parts[i+2], parts[i+3]}
				ProcessOneTuple(tuple_str, protocol, current_leaves, &all_nodes, &new_leaves)
				// Skip over any error codes for now. These are after the "ms" and start with '!'.
				for ; i+4 < len(parts) && parts[i+4] != "" && parts[i+4][0] == '!'; i += 1 {
				}
			} // Done with a 4-tuple parsing
		} // Done with one line
		current_leaves = new_leaves
	} // Done with a test file

	// Generate Hops from all_nodes
	PT_hops := ProcessAllNodes(all_nodes, server_IP, protocol)

	return PT_hops, t, conn_spec, nil
}
