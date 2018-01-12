// Parse PT filename like 20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris
// The format of test file can be found at https://paris-traceroute.net/.
package parser

import (
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/annotation"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/schema"
	"github.com/prometheus/client_golang/prometheus"
)

type PTFileName struct {
	Name string
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
	hostname        string
	ip              string
	rtts            []float64
	parent_ip       string
	parent_hostname string

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
func ProcessAllNodes(all_nodes []Node, server_IP, protocol string, tableName string) []*schema.ParisTracerouteHop {
	var results []*schema.ParisTracerouteHop
	if len(all_nodes) == 0 {
		return nil
	}

	// Iterate from the end of the list of nodes to minimize cost of removing nodes.
	for i := len(all_nodes) - 1; i >= 0; i-- {
		metrics.PTHopCount.WithLabelValues(tableName, "pt", "ok")
		if all_nodes[i].parent_ip == "" {
			one_hop := &schema.ParisTracerouteHop{
				Protocol:      protocol,
				Dest_ip:       all_nodes[i].ip,
				Dest_hostname: all_nodes[i].hostname,
				Rtt:           all_nodes[i].rtts,
				Src_ip:        server_IP,
				Src_af:        IPv4_AF,
				Dest_af:       IPv4_AF,
			}
			results = append(results, one_hop)
			break
		} else {
			one_hop := &schema.ParisTracerouteHop{
				Protocol:      protocol,
				Dest_ip:       all_nodes[i].ip,
				Dest_hostname: all_nodes[i].hostname,
				Rtt:           all_nodes[i].rtts,
				Src_ip:        all_nodes[i].parent_ip,
				Src_hostname:  all_nodes[i].parent_hostname,
				Src_af:        IPv4_AF,
				Dest_af:       IPv4_AF,
			}
			results = append(results, one_hop)
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
func ParseFirstLine(oneLine string) (protocol string, dest_IP string, server_IP string, err error) {
	parts := strings.Split(oneLine, ",")
	// check protocol
	// check algo
	for index, part := range parts {
		if index == 0 {
			segments := strings.Split(part, " ")
			if len(segments) == 4 {
				portIndex := strings.IndexByte(segments[1], ':')
				server_IP = segments[1][2:portIndex]
				portIndex = strings.IndexByte(segments[3], ':')
				dest_IP = segments[3][1:portIndex]
				if server_IP == "" || dest_IP == "" {
					return "", "", "", errors.New("Invalid IP address in the first line.")
				}
			} else {
				return "", "", "", errors.New("corrupted first line.")
			}
		}
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
					return "", "", "", errors.New("Unknown protocol")
				} else {
					protocol = mm[1]
				}
			}
		}
	}
	return protocol, dest_IP, server_IP, nil
}

// Return timestamp parsed from file name.
func GetLogtime(filename PTFileName) (time.Time, error) {
	date, _ := filename.GetDate()
	// data is in format like "20170320T23:53:10Z"
	revised_date := date[0:4] + "-" + date[4:6] + "-" + date[6:18]

	t, err := time.Parse(time.RFC3339, revised_date)
	if err != nil {
		log.Println(err)
		return time.Time{}, err
	}

	return t, nil
}

func (pt *PTParser) TaskError() error {
	return nil
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

func CreateTestId(fn string, bn string) string {
	raw_fn := filepath.Base(fn)
	// fn is in format like 20170501T000000Z-mlab1-acc02-paris-traceroute-0000.tgz
	// bn is in format like 20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris
	// test_id is in format like 2017/05/01/mlab1.lga06/20170501T23:58:07Z-72.228.158.51-40835-128.177.119.209-8080.paris.gz
	test_id := bn
	if len(raw_fn) > 30 {
		test_id = raw_fn[0:4] + "/" + raw_fn[4:6] + "/" + raw_fn[6:8] + "/" + raw_fn[17:22] + "." + raw_fn[23:28] + "/" + bn + ".gz"
	}
	return test_id
}

func (pt *PTParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, rawContent []byte) error {
	metrics.WorkerState.WithLabelValues("pt").Inc()
	defer metrics.WorkerState.WithLabelValues("pt").Dec()
	test_id := filepath.Base(testName)
	fileName := ""
	if meta["filename"] != nil {
		test_id = CreateTestId(meta["filename"].(string), filepath.Base(testName))
		fileName = meta["filename"].(string)
	}

	hops, logTime, conn_spec, err := Parse(meta, testName, fileName, rawContent, pt.TableName())
	if err != nil {
		metrics.ErrorCount.WithLabelValues(
			pt.TableName(), "pt", "corrupted content").Inc()
		metrics.TestCount.WithLabelValues(
			pt.TableName(), "pt", "corrupted content").Inc()
		log.Println(err)
		return err
	}

	insertErr := false
	for _, hop := range hops {
		pt_test := schema.PT{
			Test_id:              test_id,
			Log_time:             logTime.Unix(),
			Connection_spec:      *conn_spec,
			Paris_traceroute_hop: *hop,
			Type:                 int32(2),
			Project:              int32(3),
		}
		err := pt.inserter.InsertRow(pt_test)
		if err != nil {
			metrics.ErrorCount.WithLabelValues(
				pt.TableName(), "pt", "insert-err").Inc()
			insertErr = true
			log.Printf("insert-err: %v\n", err)
		}
	}
	if insertErr {
		// Inc TestCount only once per test.
		metrics.TestCount.WithLabelValues(pt.TableName(), "pt", "insert-err").Inc()
	} else {
		metrics.TestCount.WithLabelValues(pt.TableName(), "pt", "ok").Inc()
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
			hostname:  parts[0],
			ip:        ips[0][1 : len(ips[0])-1],
			rtts:      rtt,
			parent_ip: "",
			flow:      -1,
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
				hostname:        parts[0],
				ip:              ips[0][1 : len(ips[0])-1],
				rtts:            rtt,
				parent_ip:       leaf.ip,
				parent_hostname: leaf.hostname,
				flow:            -1,
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
						hostname:        parts[0],
						ip:              ips[0][1 : len(ips[0])-1],
						rtts:            rtt,
						parent_ip:       leaf.ip,
						parent_hostname: leaf.hostname,
						flow:            flow_int,
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
func Parse(meta map[string]bigquery.Value, testName string, fileName string, rawContent []byte, tableName string) ([]*schema.ParisTracerouteHop, time.Time, *schema.MLabConnectionSpecification, error) {
	//log.Printf("%s", testName)

	metrics.WorkerState.WithLabelValues("parse").Inc()
	defer metrics.WorkerState.WithLabelValues("parse").Dec()

	// Get the logtime
	fn := PTFileName{Name: filepath.Base(testName)}
	// Check whether the file name format is old format ("20160221T23:43:25Z_ALL27695.paris")
	// or new 5-tuple format ("20170501T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris").
	dest_IP := ""
	server_IP := ""
	// We do not need to get dest_IP and server_IP from file name, since they are at the first line
	// of test content as well.
	t, err := GetLogtime(fn)
	if err != nil {
		return nil, time.Time{}, nil, err
	}

	// The filename contains 5-tuple like 20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris
	// We can get the logtime, local IP, local port, server IP, server port from fileName directly
	is_first_line := true
	protocol := "icmp"
	// This var keep all current leaves
	var current_leaves []Node
	// This var keep all possible nodes
	var all_nodes []Node
	// TODO(dev): Handle the first line explicitly before this for loop,
	// then run the for loop on the remainder of the slice.
	last_line := ""
	reach_dest := false
	for _, oneLine := range strings.Split(string(rawContent[:]), "\n") {
		oneLine := strings.TrimSuffix(oneLine, "\n")
		// Skip empty line or initial lines starting with #.
		if len(oneLine) == 0 || oneLine[0] == '#' {
			continue
		}
		last_line = oneLine
		// This var keep all new leaves
		var new_leaves []Node
		if is_first_line {
			is_first_line = false
			var err error
			protocol, dest_IP, server_IP, err = ParseFirstLine(oneLine)
			if err != nil {
				log.Println(oneLine)
				metrics.ErrorCount.WithLabelValues(tableName, "pt", "corrupted first line").Inc()
				metrics.TestCount.WithLabelValues(tableName, "pt", "corrupted first line").Inc()
				return nil, time.Time{}, nil, err
			}
		} else {
			if strings.Contains(oneLine, dest_IP) {
				reach_dest = true
			}
			// Handle each line of test file after the first line.
			// TODO(dev): use regexp here
			parts := strings.Fields(oneLine)
			// Skip line start with "MPLS"
			if len(parts) < 4 || parts[0] == "MPLS" {
				continue
			}

			// Drop the first 3 parts, like "1  P(6, 6)" because they are useless.
			// The following parts are grouped into tuples, each with 4 parts:
			for i := 3; i < len(parts); i += 4 {
				if (i + 3) >= len(parts) {
					// avoid panic crash due to corrupted content
					break
				}
				tuple_str := []string{parts[i], parts[i+1], parts[i+2], parts[i+3]}
				err := ProcessOneTuple(tuple_str, protocol, current_leaves, &all_nodes, &new_leaves)
				if err != nil {
					metrics.PTHopCount.WithLabelValues(tableName, "pt", "discarded").Add(float64(len(all_nodes)))
					return nil, time.Time{}, nil, err
				}
				// Skip over any error codes for now. These are after the "ms" and start with '!'.
				for ; i+4 < len(parts) && parts[i+4] != "" && parts[i+4][0] == '!'; i += 1 {
				}
			} // Done with a 4-tuple parsing
		} // Done with one line
		current_leaves = new_leaves
	} // Done with a test file

	// Check whether the last hop is the dest_ip
	metroName := etl.GetMetroName(fileName)
	metrics.PTTestCount.WithLabelValues(metroName).Inc()
	if !strings.Contains(last_line, dest_IP) {
		metrics.PTNotReachDestCount.WithLabelValues(metroName).Inc()
		if reach_dest {
			// This test reach dest in the middle, but then do weired things
			metrics.PTReachDestInMiddle.WithLabelValues(metroName).Inc()
			log.Printf("middle mess up test_id: " + fileName + " " + testName)
		}
	}
	// Calculate how close is the last hop with the real dest.
	// The last node of all_nodes contains the last hop IP.
	bits_diff, ip_type := etl.NumberBitsDifferent(dest_IP, all_nodes[len(all_nodes)-1].ip)
	if ip_type == 4 {
		metrics.PTNotReachBitsDiffV4.WithLabelValues(metroName).Observe(float64(bits_diff))
	}
	if ip_type == 6 {
		metrics.PTNotReachBitsDiffV6.WithLabelValues(metroName).Observe(float64(bits_diff))
	}

	// Generate Hops from all_nodes
	PT_hops := ProcessAllNodes(all_nodes, server_IP, protocol, tableName)
	conn_spec := &schema.MLabConnectionSpecification{
		Server_ip:      server_IP,
		Server_af:      IPv4_AF,
		Client_ip:      dest_IP,
		Client_af:      IPv4_AF,
		Data_direction: 0,
	}

	// Only annotate if flag enabled...
	if !annotation.IPAnnotationEnabled {
		metrics.AnnotationErrorCount.With(prometheus.Labels{
			"source": "IP Annotation Disabled."}).Inc()
	} else {
		AddGeoDataPTConnSpec(conn_spec, t)
		AddGeoDataPTHopBatch(PT_hops, t)
	}
	return PT_hops, t, conn_spec, nil
}
