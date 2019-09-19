package parser

// Parse PT filename like 20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris
// The format of legacy test file can be found at https://paris-traceroute.net/.

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/google/go-jsonnet"
	v2as "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/etl/annotation"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/schema"
)

// -------------------------------------------------
// The following are struct and funcs shared by legacy parsing and Json parsing.
// -------------------------------------------------
type PTFileName struct {
	Name string
}

func (f *PTFileName) GetDate() (string, bool) {
	i := strings.Index(f.Name, "Z")
	if i >= 15 {
		// covert date like "20170320T23:53:10Z" or "20170320T235310Z" into one format
		return strings.Replace(f.Name[0:i+1], ":", "", -1), true
	}
	return "", false
}

// Return timestamp parsed from file name.
func GetLogtime(filename PTFileName) (time.Time, error) {
	date, success := filename.GetDate()
	if !success {
		return time.Time{}, errors.New("no date in filename")
	}

	return time.Parse("20060102T150405Z", date)
}

// -------------------------------------------------
// The following are struct and funcs used by Json parsing.
// -------------------------------------------------

type TS struct {
	Sec  int64 `json:"sec"`
	Usec int64 `json:"usec"`
}

type Reply struct {
	Rx         TS      `json:"rx"`
	Ttl        int     `json:"ttl"`
	Rtt        float64 `json:"rtt"`
	Icmp_type  int     `json:"icmp_type"`
	Icmp_code  int     `json:"icmp_code"`
	Icmp_q_tos int     `json:"icmp_q_tos"`
	Icmp_q_ttl int     `json:"icmp_q_ttl"`
}

type Probe struct {
	Tx      TS      `json:"tx"`
	Replyc  int     `json:"replyc"`
	Ttl     int64   `json:"ttl"`
	Attempt int     `json:"attempt"`
	Flowid  int64   `json:"flowid"`
	Replies []Reply `json:"replies"`
}

type ScamperLink struct {
	Addr   string  `json:"addr"`
	Probes []Probe `json:"probes"`
}

type ScamperNode struct {
	Addr  string          `json:"addr"`
	Name  string          `json:"name"`
	Q_ttl int             `json:"q_ttl"`
	Linkc int64           `json:"linkc"`
	Links [][]ScamperLink `json:"links"`
}

type TracelbLine struct {
	Type         string        `json:"type"`
	Version      string        `json:"version"`
	Userid       float64       `json:"userid"`
	Method       string        `json:"method"`
	Src          string        `json:"src"`
	Dst          string        `json:"dst"`
	Start        TS            `json:"start"`
	Probe_size   float64       `json:"probe_size"`
	Firsthop     float64       `json:"firsthop"`
	Attempts     float64       `json:"attempts"`
	Confidence   float64       `json:"confidence"`
	Tos          float64       `json:"tos"`
	Gaplint      float64       `json:"gaplint"`
	Wait_timeout float64       `json:"wait_timeout"`
	Wait_probe   float64       `json:"wait_probe"`
	Probec       float64       `json:"probec"`
	Probec_max   float64       `json:"probec_max"`
	Nodec        float64       `json:"nodec"`
	Linkc        float64       `json:"linkc"`
	Nodes        []ScamperNode `json:"nodes"`
}

type CyclestartLine struct {
	Type       string  `json:"type"`
	List_name  string  `json:"list_name"`
	id         float64 `json:"id"`
	Hostname   string  `json:"hostname"`
	Start_time float64 `json:"start_time"`
}

type CyclestopLine struct {
	Type      string  `json:"type"`
	List_name string  `json:"list_name"`
	id        float64 `json:"id"`
	Hostname  string  `json:"hostname"`
	Stop_time float64 `json:"stop_time"`
}

// ParseJSON the raw jsonl test file into schema.PTTest.
func ParseJSON(testName string, rawContent []byte, tableName string, taskFilename string) (schema.PTTest, error) {
	metrics.WorkerState.WithLabelValues(tableName, "pt-json-parse").Inc()
	defer metrics.WorkerState.WithLabelValues(tableName, "pt-json-parse").Dec()

	// Get the logtime
	logTime, err := GetLogtime(PTFileName{Name: filepath.Base(testName)})
	if err != nil {
		return schema.PTTest{}, err
	}

	// Split the JSON file and parse it line by line.
	var uuid string
	var hops []schema.ScamperHop
	var cycleStart CyclestartLine
	var tracelb TracelbLine
	var cycleStop CyclestopLine

	for index, oneLine := range strings.Split(string(rawContent[:]), "\n") {
		oneLine = strings.TrimSuffix(oneLine, "\n")
		if len(oneLine) == 0 {
			continue
		}

		var scamperResult map[string]interface{}
		err := json.Unmarshal([]byte(oneLine), &scamperResult)

		if err != nil {
			// use jsonnett to do extra reprocessing
			vm := jsonnet.MakeVM()
			output, err := vm.EvaluateSnippet("file", oneLine)
			err = json.Unmarshal([]byte(output), &scamperResult)
			if err != nil {
				// fail and return here.
				return schema.PTTest{}, err
			}
		}

		// The first line should always be UUID line.
		if index == 0 {
			// extract uuid from {"UUID": "ndt-74mqr_1565960097_000000000006DBCC"}
			_, ok := scamperResult["UUID"]
			if !ok {
				metrics.ErrorCount.WithLabelValues(
					tableName, "pt", "corrupted json content").Inc()
				metrics.TestCount.WithLabelValues(
					tableName, "pt", "corrupted json content").Inc()
				return schema.PTTest{}, errors.New("empty UUID")
			}
			uuid = scamperResult["UUID"].(string)
			continue
		}
		var entryType string
		_, ok := scamperResult["type"]
		if !ok {
			continue
		}
		entryType = scamperResult["type"].(string)
		if entryType == "cycle-start" {
			// extract start_time
			// {"type":"cycle-start", "list_name":"/tmp/scamperctrl:62485", "id":1, "hostname":"ndt-74mqr", "start_time":1567900908}
			err := json.Unmarshal([]byte(oneLine), &cycleStart)
			if err != nil {
				metrics.ErrorCount.WithLabelValues(
					tableName, "pt", "corrupted json content").Inc()
				metrics.TestCount.WithLabelValues(
					tableName, "pt", "corrupted json content").Inc()
				return schema.PTTest{}, err
			}
		} else if entryType == "cycle-stop" {
			err := json.Unmarshal([]byte(oneLine), &cycleStop)
			if err != nil {
				metrics.ErrorCount.WithLabelValues(
					tableName, "pt", "corrupted json content").Inc()
				metrics.TestCount.WithLabelValues(
					tableName, "pt", "corrupted json content").Inc()
				return schema.PTTest{}, err
			}
		} else if entryType == "tracelb" {
			// Parse the line in struct
			err := json.Unmarshal([]byte(oneLine), &tracelb)
			if err != nil {
				// use jsonnett to do extra reprocessing
				// TODO: this is a hack. We should see if this can be simplified.
				vm := jsonnet.MakeVM()
				output, err := vm.EvaluateSnippet("file", oneLine)
				err = json.Unmarshal([]byte(output), &tracelb)
				//log.Printf("%+v\n", tracelb)
				if err != nil {
					// fail and return here.
					metrics.ErrorCount.WithLabelValues(
						tableName, "pt", "corrupted json content").Inc()
					metrics.TestCount.WithLabelValues(
						tableName, "pt", "corrupted json content").Inc()
					return schema.PTTest{}, err
				}
			}

			for _, oneNode := range tracelb.Nodes {
				var links []schema.HopLink
				for _, oneLink := range oneNode.Links[0] {
					var probes []schema.HopProbe
					var ttl int64
					for _, oneProbe := range oneLink.Probes {
						var rtt []float64
						for _, oneReply := range oneProbe.Replies {
							rtt = append(rtt, oneReply.Rtt)
						}
						probes = append(probes, schema.HopProbe{Flowid: int64(oneProbe.Flowid), Rtt: rtt})
						ttl = int64(oneProbe.Ttl)
					}
					links = append(links, schema.HopLink{HopDstIP: oneLink.Addr, TTL: ttl, Probes: probes})
				}
				hops = append(hops, schema.ScamperHop{
					Source: schema.HopIP{IP: oneNode.Addr, Hostname: oneNode.Name},
					Linkc:  oneNode.Linkc,
					Links:  links,
				})
			}
		} else {
			// Invalid entry
			metrics.ErrorCount.WithLabelValues(
				tableName, "pt", "corrupted json content").Inc()
			metrics.TestCount.WithLabelValues(
				tableName, "pt", "corrupted json content").Inc()
			return schema.PTTest{}, errors.New("invalid type entry")
		}
	}

	parseInfo := schema.ParseInfo{
		TaskFileName:  taskFilename,
		ParseTime:     time.Now(),
		ParserVersion: Version(),
	}

	return schema.PTTest{
		UUID:           uuid,
		TestTime:       logTime,
		Parseinfo:      parseInfo,
		StartTime:      int64(cycleStart.Start_time),
		StopTime:       int64(cycleStop.Stop_time),
		ScamperVersion: tracelb.Version,
		Source:         schema.ServerInfo{IP: tracelb.Src},
		Destination:    schema.ClientInfo{IP: tracelb.Dst},
		ProbeSize:      int64(tracelb.Probe_size),
		ProbeC:         int64(tracelb.Probec),
		Hop:            hops,
	}, nil
}

// -------------------------------------------------
// The following are struct and funcs used by legacy parsing.
// -------------------------------------------------

// The data structure is used to store the parsed results temporarily before it is verified
// not polluted and can be inserted into BQ tables
type cachedPTData struct {
	TestID           string
	Hops             []schema.ScamperHop
	LogTime          time.Time
	Source           schema.ServerInfo
	Destination      schema.ClientInfo
	LastValidHopLine string
	MetroName        string
	UUID             string
}

type PTParser struct {
	Base
	// Care should be taken to ensure this does not accumulate many rows and
	// lead to OOM problems.
	previousTests []cachedPTData
	taskFileName  string // The tar file containing these tests.
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
const PTBufferSize int = 2

func NewPTParser(ins etl.Inserter, ann ...v2as.Annotator) *PTParser {
	bufSize := etl.PT.BQBufferSize()
	var annotator v2as.Annotator
	if len(ann) > 0 && ann[0] != nil {
		annotator = ann[0]
	} else {
		annotator = v2as.GetAnnotator(annotation.BatchURL)
	}
	return &PTParser{Base: *NewBase(ins, bufSize, annotator)}
}

// ProcessAllNodes take the array of the Nodes, and generate one ScamperHop entry from each node.
func ProcessAllNodes(allNodes []Node, server_IP, protocol string, tableName string) []schema.ScamperHop {
	var results []schema.ScamperHop
	if len(allNodes) == 0 {
		return nil
	}

	// Iterate from the end of the list of nodes to minimize cost of removing nodes.
	for i := len(allNodes) - 1; i >= 0; i-- {
		metrics.PTHopCount.WithLabelValues(tableName, "pt", "ok")
		oneProbe := schema.HopProbe{
			Rtt: allNodes[i].rtts,
		}
		probes := make([]schema.HopProbe, 0, 1)
		probes = append(probes, oneProbe)
		hopLink := schema.HopLink{
			HopDstIP: allNodes[i].ip,
			Probes:   probes,
		}
		links := make([]schema.HopLink, 0, 1)
		links = append(links, hopLink)
		if allNodes[i].parent_ip == "" {
			// create a hop that from server_IP to allNodes[i].ip
			source := schema.HopIP{
				IP: server_IP,
			}
			oneHop := schema.ScamperHop{
				Source: source,
				Links:  links,
			}
			results = append(results, oneHop)
			break
		} else {
			source := schema.HopIP{
				IP:       allNodes[i].parent_ip,
				Hostname: allNodes[i].parent_hostname,
			}
			oneHop := schema.ScamperHop{
				Source: source,
				Links:  links,
			}
			results = append(results, oneHop)
		}
	}
	return results
}

// This function was designed for hops with multiple flows. When the source IP are duplicate flows, but the destination IP is
// single flow IP, those hops will result in just one node in the list.
func Unique(oneNode Node, list []Node) bool {
	for _, existingNode := range list {
		if existingNode.hostname == oneNode.hostname && existingNode.ip == oneNode.ip && existingNode.flow == oneNode.flow {
			return false
		}
	}
	return true
}

// Handle the first line, like
// "traceroute [(64.86.132.76:33461) -> (98.162.212.214:53849)], protocol icmp, algo exhaustive, duration 19 s"
func ParseFirstLine(oneLine string) (protocol string, destIP string, serverIP string, err error) {
	parts := strings.Split(oneLine, ",")
	// check protocol
	// check algo
	for index, part := range parts {
		if index == 0 {
			segments := strings.Split(part, " ")
			if len(segments) == 4 {
				portIndex := strings.IndexByte(segments[1], ':')
				serverIP = segments[1][2:portIndex]
				portIndex = strings.IndexByte(segments[3], ':')
				destIP = segments[3][1:portIndex]
				if serverIP == "" || destIP == "" {
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
	return protocol, destIP, serverIP, nil
}

func (pt *PTParser) TaskError() error {
	return nil
}

func (pt *PTParser) TableName() string {
	return pt.TableBase()
}

func (pt *PTParser) InsertOneTest(oneTest cachedPTData) {
	parseInfo := schema.ParseInfo{
		TaskFileName:  pt.taskFileName,
		ParseTime:     time.Now(),
		ParserVersion: Version(),
	}

	ptTest := schema.PTTest{
		UUID:        oneTest.UUID,
		TestTime:    oneTest.LogTime,
		Parseinfo:   parseInfo,
		Source:      oneTest.Source,
		Destination: oneTest.Destination,
		Hop:         oneTest.Hops,
	}

	err := pt.AddRow(&ptTest)
	if err == etl.ErrBufferFull {
		// Flush asynchronously, to improve throughput.
		pt.Annotate(pt.TableName())
		pt.PutAsync(pt.TakeRows())
		pt.AddRow(&ptTest)
	}
}

// Insert last several tests in previousTests
func (pt *PTParser) ProcessLastTests() error {
	for _, oneTest := range pt.previousTests {
		pt.InsertOneTest(oneTest)
	}
	pt.previousTests = []cachedPTData{}
	return nil
}

func (pt *PTParser) Flush() error {
	pt.ProcessLastTests()
	return pt.Inserter.Flush()
}

func CreateTestId(fn string, bn string) string {
	rawFn := filepath.Base(fn)
	// fn is in format like 20170501T000000Z-mlab1-acc02-paris-traceroute-0000.tgz
	// bn is in format like 20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris
	// test_id is in format like 2017/05/01/mlab1.lga06/20170501T23:58:07Z-72.228.158.51-40835-128.177.119.209-8080.paris.gz
	testId := bn
	if len(rawFn) > 30 {
		testId = rawFn[0:4] + "/" + rawFn[4:6] + "/" + rawFn[6:8] + "/" + rawFn[17:22] + "." + rawFn[23:28] + "/" + bn + ".gz"
	}
	return testId
}

func (pt *PTParser) NumBufferedTests() int {
	return len(pt.previousTests)
}

// IsParsable returns the canonical test type and whether to parse data.
func (pt *PTParser) IsParsable(testName string, data []byte) (string, bool) {
	if strings.HasSuffix(testName, ".paris") {
		return "paris", true
	}
	return "unknown", false
}

// ParseAndInsert parses a paris-traceroute log file and inserts results into a single row.
func (pt *PTParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, rawContent []byte) error {
	metrics.WorkerState.WithLabelValues(pt.TableName(), "pt").Inc()
	defer metrics.WorkerState.WithLabelValues(pt.TableName(), "pt").Dec()
	testId := filepath.Base(testName)
	if meta["filename"] != nil {
		testId = CreateTestId(meta["filename"].(string), filepath.Base(testName))
		pt.taskFileName = meta["filename"].(string)
	} else {
		return errors.New("empty filename")
	}

	// Process the json output of Scamper binary.
	if strings.Contains(pt.taskFileName, "jsonl") {
		ptTest, err := ParseJSON(testName, rawContent, pt.TableName(), pt.taskFileName)
		if err == nil {
			err := pt.AddRow(&ptTest)
			if err == etl.ErrBufferFull {
				// Flush asynchronously, to improve throughput.
				pt.Annotate(pt.TableName())
				pt.PutAsync(pt.TakeRows())
				pt.AddRow(&ptTest)
			}
		}
		return nil
	}

	// Process the legacy Paris Traceroute txt output
	cachedTest, err := Parse(meta, testName, testId, rawContent, pt.TableName())
	if err != nil {
		metrics.ErrorCount.WithLabelValues(
			pt.TableName(), "pt", "corrupted content").Inc()
		metrics.TestCount.WithLabelValues(
			pt.TableName(), "pt", "corrupted content").Inc()
		log.Printf("%v %s", err, testName)
		return err
	}

	if len(cachedTest.Hops) == 0 {
		// Empty test, no further action.
		return nil
	}

	// Check all buffered PT tests whether Client_ip in connSpec appear in
	// the last hop of the buffered test.
	// If it does appear, then the buffered test was polluted, and it will
	// be discarded from buffer.
	// If it does not appear, then no pollution detected.
	destIP := cachedTest.Destination.IP
	for index, PTTest := range pt.previousTests {
		// array of hops was built in reverse order from list of nodes
		// (in func ProcessAllNodes()). So the final parsed hop is Hops[0].
		finalHop := PTTest.Hops[0]
		if PTTest.Destination.IP != destIP && len(finalHop.Links) > 0 &&
			(finalHop.Links[0].HopDstIP == destIP || strings.Contains(PTTest.LastValidHopLine, destIP)) {
			// Discard pt.previousTests[index]
			metrics.PTPollutedCount.WithLabelValues(pt.previousTests[index].MetroName).Inc()
			pt.previousTests = append(pt.previousTests[:index], pt.previousTests[index+1:]...)
			break
		}
	}

	// If a test ends at the expected DestIP, it is not at risk of being
	// polluted,so we don't have to wait to check against further tests.
	// We can just go ahead and insert it to BigQuery table directly. This
	// optimization makes the pollution check more effective by saving the
	// unnecessary check between those tests (reached expected DestIP) and
	// the new test.
	// Also we don't care about test LogTime order, since there are other
	// workers inserting other blocks of hops concurrently.
	if cachedTest.LastValidHopLine == "ExpectedDestIP" {
		pt.InsertOneTest(cachedTest)
		return nil
	}

	// If buffer is full, remove the oldest test and insert it into BigQuery table.
	if len(pt.previousTests) >= PTBufferSize {
		// Insert the oldest test pt.previousTests[0] into BigQuery
		pt.InsertOneTest(pt.previousTests[0])
		pt.previousTests = pt.previousTests[1:]
	}
	// Insert current test into pt.previousTests
	pt.previousTests = append(pt.previousTests, cachedTest)
	return nil
}

// For each 4 tuples, it is like:
// parts[0] is the hostname, like "if-ae-10-3.tcore2.DT8-Dallas.as6453.net".
// parts[1] is IP address like "(66.110.57.41)" or "(72.14.218.190):0,2,3,4,6,8,10"
// parts[2] are rtt in numbers like "0.298/0.318/0.340/0.016"
// parts[3] should always be "ms"
func ProcessOneTuple(parts []string, protocol string, currentLeaves []Node, allNodes, newLeaves *[]Node) error {
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
		oneRtt, err := strconv.ParseFloat(parts[2], 64)
		if err == nil {
			rtt = append(rtt, oneRtt)
		} else {
			fmt.Printf("Failed to conver rtt to number with error %v", err)
			return err
		}

	// Handle icmp, parts[2] has 4 numbers separated by "/"
	case protocol == "icmp":
		nums := strings.Split(parts[2], "/")
		if len(nums) != 4 {
			return errors.New("Failed to parse rtts for icmp test. 4 numbers expected")
		}
		for _, num := range nums {
			oneRtt, err := strconv.ParseFloat(num, 64)
			if err == nil {
				rtt = append(rtt, oneRtt)
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
	if len(*allNodes) == 0 {
		oneNode := &Node{
			hostname:  parts[0],
			ip:        ips[0][1 : len(ips[0])-1],
			rtts:      rtt,
			parent_ip: "",
			flow:      -1,
		}

		*allNodes = append(*allNodes, *oneNode)
		*newLeaves = append(*newLeaves, *oneNode)
		return nil
	}
	// There are duplicates in allNodes, but not in newLeaves.
	// TODO(dev): consider consolidating these with a repeat count.
	switch len(ips) {
	case 1:
		// For single flow, the new node will be son of all current leaves
		for _, leaf := range currentLeaves {
			oneNode := &Node{
				hostname:        parts[0],
				ip:              ips[0][1 : len(ips[0])-1],
				rtts:            rtt,
				parent_ip:       leaf.ip,
				parent_hostname: leaf.hostname,
				flow:            -1,
			}
			*allNodes = append(*allNodes, *oneNode)
			if Unique(*oneNode, *newLeaves) {
				*newLeaves = append(*newLeaves, *oneNode)
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

			for _, leaf := range currentLeaves {
				if leaf.flow == -1 || leaf.flow == flow_int {
					oneNode := &Node{
						hostname:        parts[0],
						ip:              ips[0][1 : len(ips[0])-1],
						rtts:            rtt,
						parent_ip:       leaf.ip,
						parent_hostname: leaf.hostname,
						flow:            flow_int,
					}
					*allNodes = append(*allNodes, *oneNode)
					if Unique(*oneNode, *newLeaves) {
						*newLeaves = append(*newLeaves, *oneNode)
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
func Parse(meta map[string]bigquery.Value, testName string, testId string, rawContent []byte, tableName string) (cachedPTData, error) {
	//log.Printf("%s", testName)
	metrics.WorkerState.WithLabelValues(tableName, "pt-parse").Inc()
	defer metrics.WorkerState.WithLabelValues(tableName, "pt-parse").Dec()

	// Get the logtime
	fn := PTFileName{Name: filepath.Base(testName)}
	// Check whether the file name format is old format ("20160221T23:43:25Z_ALL27695.paris")
	// or new 5-tuple format ("20170501T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris").
	destIP := ""
	serverIP := ""
	// We do not need to get destIP and serverIP from file name, since they are at the first line
	// of test content as well.
	logTime, err := GetLogtime(fn)
	if err != nil {
		return cachedPTData{}, err
	}

	// The filename contains 5-tuple like 20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris
	// By design, they are logtime, local IP, local port, the server IP and port which served the test
	// that triggered this PT test (not the server IP & port that served THIS PT test.)
	isFirstLine := true
	protocol := "icmp"
	// This var keep all current leaves
	var currentLeaves []Node
	// This var keep all possible nodes
	var allNodes []Node
	// TODO(dev): Handle the first line explicitly before this for loop,
	// then run the for loop on the remainder of the slice.
	lastValidHopLine := ""
	reachedDest := false
	for _, oneLine := range strings.Split(string(rawContent[:]), "\n") {
		oneLine = strings.TrimSuffix(oneLine, "\n")
		// Skip empty line or initial lines starting with #.
		if len(oneLine) == 0 || oneLine[0] == '#' {
			continue
		}
		// This var keep all new leaves
		var newLeaves []Node
		if isFirstLine {
			isFirstLine = false
			var err error
			protocol, destIP, serverIP, err = ParseFirstLine(oneLine)
			if err != nil {
				log.Printf("%s %s", oneLine, testName)
				metrics.ErrorCount.WithLabelValues(tableName, "pt", "corrupted first line").Inc()
				metrics.TestCount.WithLabelValues(tableName, "pt", "corrupted first line").Inc()
				return cachedPTData{}, err
			}
		} else {
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
				tupleStr := []string{parts[i], parts[i+1], parts[i+2], parts[i+3]}
				err := ProcessOneTuple(tupleStr, protocol, currentLeaves, &allNodes, &newLeaves)
				if err != nil {
					metrics.PTHopCount.WithLabelValues(tableName, "pt", "discarded").Add(float64(len(allNodes)))
					return cachedPTData{}, err
				}
				// Skip over any error codes for now. These are after the "ms" and start with '!'.
				for ; i+4 < len(parts) && parts[i+4] != "" && parts[i+4][0] == '!'; i += 1 {
				}
			} // Done with a 4-tuple parsing
			if strings.Contains(oneLine, destIP) {
				reachedDest = true
				// TODO: It is an option that we just stop parsing
			}
			// lastValidHopLine is the last line from raw test file that contains valid hop information.
			lastValidHopLine = oneLine
		} // Done with one line
		currentLeaves = newLeaves
	} // Done with a test file

	// Check whether the last hop is the destIP
	fileName := ""
	if meta["filename"] != nil {
		fileName = meta["filename"].(string)
	}
	iataCode := etl.GetIATACode(fileName)
	metrics.PTTestCount.WithLabelValues(iataCode).Inc()
	// lastHop is a close estimation for where the test reached at the end.
	// It is possible that the last line contains destIP and other IP at the same time
	// if the previous hop contains multiple paths.
	// So it is possible that allNodes[len(allNodes)-1].ip is not destIP but the test
	// reach destIP at the last hop.
	lastHop := destIP
	if len(allNodes) == 0 {
		// Empty test, stop here.  Not an error.
		return cachedPTData{}, nil
	}
	if allNodes[len(allNodes)-1].ip != destIP && !strings.Contains(lastValidHopLine, destIP) {
		// This is the case that we consider the test did not reach destIP at the last hop.
		lastHop = allNodes[len(allNodes)-1].ip
		metrics.PTNotReachDestCount.WithLabelValues(iataCode).Inc()
		if reachedDest {
			// This test reach dest in the middle, but then do weird things for unknown reason.
			metrics.PTMoreHopsAfterDest.WithLabelValues(iataCode).Inc()
			log.Printf("middle mess up test_id: " + fileName + " " + testName)
		}
	} else {
		lastValidHopLine = "ExpectedDestIP"
	}
	// Calculate how close is the last hop with the real dest.
	// The last node of allNodes contains the last hop IP.
	bitsDiff, ipType := etl.NumberBitsDifferent(destIP, lastHop)
	if ipType == 4 {
		metrics.PTBitsAwayFromDestV4.WithLabelValues(iataCode).Observe(float64(bitsDiff))
	}
	if ipType == 6 {
		metrics.PTBitsAwayFromDestV6.WithLabelValues(iataCode).Observe(float64(bitsDiff))
	}

	// Generate Hops from allNodes
	PTHops := ProcessAllNodes(allNodes, serverIP, protocol, tableName)

	source := schema.ServerInfo{
		IP: serverIP,
	}
	destination := schema.ClientInfo{
		IP: destIP,
	}

	// TODO: Add annotation to the IP of source, destination and hops.

	return cachedPTData{
		TestID:           testId,
		Hops:             PTHops,
		LogTime:          logTime,
		Source:           source,
		Destination:      destination,
		LastValidHopLine: lastValidHopLine,
		MetroName:        iataCode,
	}, nil
}
