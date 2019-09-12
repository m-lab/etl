package parser_test

import (
	"testing"

	"github.com/m-lab/etl/parser"
)

// TODO: IPv6 tests
func TestGetLogtime(t *testing.T) {
	fn1 := parser.PTFileName{Name: "20160112T00:45:44Z_ALL27409.paris"}
	t1, err1 := parser.GetLogtime(fn1)
	if err1 != nil || t1.String() != "2016-01-12 00:45:44 +0000 UTC" {
		t.Errorf("Error in parsing log time from legacy filename!\n")
	}

	fn2 := parser.PTFileName{Name: "20170320T23:53:10Z-172.17.94.34-33456-74.125.224.100-33457.paris"}
	t2, err2 := parser.GetLogtime(fn2)
	if err2 != nil || t2.String() != "2017-03-20 23:53:10 +0000 UTC" {
		t.Errorf("Error in parsing log time from 5-tuple filename!\n")
	}

	fn3 := parser.PTFileName{Name: "20190908T000148Z_ndt-74mqr_1565960097_000000000006DBCC.jsonl"}
	t3, err3 := parser.GetLogtime(fn3)
	if err3 != nil || t3.String() != "2019-09-08 00:01:48 +0000 UTC" {
		t.Errorf("Error in parsing log time from scamper Json filename!\n")
	}
}

func TestParseJson(t *testing.T) {
	testStr1 := `{"UUID": "ndt-plh7v_1566050090_000000000004D60F"}
	{"type":"cycle-start", "list_name":"/tmp/scamperctrl:51803", "id":1, "hostname":"ndt-plh7v", "start_time":1566691268}
	{"type":"cycle-start", "list_name":"/tmp/scamperctrl:51803", "id":1, "hostname":"ndt-plh7v", "start_time":1566691268}
	{"type":"tracelb", "version":"0.1", "userid":0, "method":"icmp-echo", "src":"2405:2000:301::101", "dst":"2406:3003:2007:3acc:38ad:70da:ebd7:85b0", "start":{"sec":1566691268, "usec":495665, "ftime":"2019-08-25 00:01:08"}, "probe_size":60, "firsthop":1, "attempts":3, "confidence":95, "tos":0, "gaplimit":3, "wait_timeout":5, "wait_probe":250, "probec":67, "probec_max":3000, "nodec":3, "linkc":3, "nodes":[{"addr":"2405:2000:ffa0:102::3", "q_ttl":1, "linkc":1, "links":[[{"addr":"*"}],[{"addr":"*"}],]}]}
	{"type":"cycle-stop", "list_name":"/tmp/scamperctrl:51803", "id":1, "hostname":"ndt-plh7v", "stop_time":1566691541}`
	parser.ParseJson("20160112T00:45:44Z_ALL27409.paris", []byte(testStr1), "", "")

	//parser.GetJsonField([]byte(testJson))
	t.Errorf("XXX!\n")
}

/*
func TestParseFirstLine(t *testing.T) {
	protocol, dest_ip, server_ip, err := parser.ParseFirstLine("traceroute [(64.86.132.76:33461) -> (98.162.212.214:53849)], protocol icmp, algo exhaustive, duration 19 s")
	if dest_ip != "98.162.212.214" || server_ip != "64.86.132.76" || protocol != "icmp" || err != nil {
		t.Errorf("Error in parsing the first line!\n")
		return
	}

	protocol, dest_ip, server_ip, err = parser.ParseFirstLine("Exception : [ERROR](Probe.cc, 109)Can't send the probe : Invalid argument")
	if err == nil {
		t.Errorf("Error in parsing the first line!\n")
		return
	}

}

func TestCreateTestId(t *testing.T) {
	test_id := parser.CreateTestId("20170501T000000Z-mlab1-acc02-paris-traceroute-0000.tgz", "20170501T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris")
	if test_id != "2017/05/01/mlab1.acc02/20170501T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris.gz" {
		fmt.Println(test_id)
		t.Errorf("Error in creating test id!\n")
		return
	}
}

func TestParseLegacyFormatData(t *testing.T) {
	rawData, err := ioutil.ReadFile("testdata/20160112T00:45:44Z_ALL27409.paris")
	if err != nil {
		fmt.Println("cannot load test data")
		return
	}
	cachedTest, err := parser.Parse(nil, "testdata/20160112T00:45:44Z_ALL27409.paris", "", rawData, "pt-daily")
	if err != nil {
		t.Fatalf(err.Error())
	}
	if len(cachedTest.Hops) != 9 {
		t.Fatalf("Do not process hops correctly.")
	}
	if cachedTest.LogTime.Unix() != 1452559544 {
		t.Fatalf("Do not process log time correctly.")
	}
	if cachedTest.LastValidHopLine != "ExpectedDestIP" {
		t.Fatalf("Did not reach expected destination.")
	}
}

func TestPTParser(t *testing.T) {
	rawData, err := ioutil.ReadFile("testdata/20170320T23:53:10Z-172.17.94.34-33456-74.125.224.100-33457.paris")
	cachedTest, err := parser.Parse(nil, "testdata/20170320T23:53:10Z-172.17.94.34-33456-74.125.224.100-33457.paris", "", rawData, "pt-daily")
	if err != nil {
		t.Fatalf(err.Error())
	}
	if cachedTest.LogTime.Unix() != 1490053990 {
		t.Fatalf("Do not process log time correctly.")
	}

	if cachedTest.Source.IP != "172.17.94.34" {
		t.Fatalf("Wrong results for Server IP.")
	}

	if cachedTest.Destination.IP != "74.125.224.100" {
		t.Fatalf("Wrong results for Client IP.")
	}

	// TODO(dev): reformat these individual values to be more readable.
	expected_hop := schema.ScamperHop{
		Source: schema.HopIP{
			IP:          "64.233.174.109",
			City:        "",
			CountryCode: "",
			Hostname:    "sr05-te1-8.nuq04.net.google.com",
		},
		Linkc: 0,
		Links: []schema.HopLink{
			schema.HopLink{
				HopDstIP: "74.125.224.100",
				TTL:      0,
				Probes: []schema.HopProbe{
					schema.HopProbe{
						Flowid: 0,
						Rtt:    []float64{0.895},
					},
				},
			},
		},
	}
	if len(cachedTest.Hops) != 38 {
		t.Fatalf("Wrong number of PT hops!")
	}

	if !reflect.DeepEqual(cachedTest.Hops[0], expected_hop) {
		fmt.Printf("Here is expected    : %v\n", expected_hop)
		fmt.Printf("Here is what is real: %v\n", cachedTest.Hops[0])
		t.Fatalf("Wrong results for PT hops!")
	}
}

func TestPTInserter(t *testing.T) {
	ins := newInMemoryInserter()
	pt := parser.NewPTParser(ins)
	rawData, err := ioutil.ReadFile("testdata/20170320T23:53:10Z-172.17.94.34-33456-74.125.224.100-33457.paris")
	if err != nil {
		t.Fatalf("cannot read testdata.")
	}
	meta := map[string]bigquery.Value{"filename": "gs://fake-bucket/fake-archive.tgz"}
	err = pt.ParseAndInsert(meta, "testdata/20170320T23:53:10Z-172.17.94.34-33456-74.125.224.100-33457.paris", rawData)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if pt.NumRowsForTest() != 1 {
		fmt.Println(pt.NumRowsForTest())
		t.Fatalf("Number of rows in PT table is wrong.")
	}
	pt.AnnotateAndPutAsync("traceroute")
	//pt.Inserter.Flush()
	if len(ins.data) != 1 {
		fmt.Println(len(ins.data))
		t.Fatalf("Number of rows in inserter is wrong.")
	}
	if ins.data[0].(*schema.PTTest).Parseinfo.TaskFileName != "gs://fake-bucket/fake-archive.tgz" {
		t.Fatalf("Task filename is wrong.")
	}
}

func TestPTPollutionCheck(t *testing.T) {
	ins := &inMemoryInserter{}
	pt := parser.NewPTParser(ins)

	tests := []struct {
		fileName             string
		expectedBufferedTest int
		expectedNumRows      int
	}{
		{
			fileName:             "testdata/PT/20171208T00:00:04Z-35.188.101.1-40784-173.205.3.38-9090.paris",
			expectedBufferedTest: 1,
			expectedNumRows:      0,
		},
		{
			fileName: "testdata/PT/20171208T00:00:04Z-37.220.21.130-5667-173.205.3.43-42487.paris",
			// The second test reached expected destIP, and was inserted into BigQuery table.
			// The buffer has only the first test.
			expectedBufferedTest: 1,
			expectedNumRows:      1,
		},
		{
			fileName: "testdata/PT/20171208T00:00:14Z-139.60.160.135-2023-173.205.3.44-1101.paris",
			// The first test was detected that it was polluted by the third test.
			// expectedBufferedTest is 0, which means pollution detected and test removed.
			expectedBufferedTest: 0,
			// The third test reached its destIP and was inserted into BigQuery.
			expectedNumRows: 2,
		},
		{
			fileName: "testdata/PT/20171208T00:00:14Z-76.227.226.149-37156-173.205.3.37-52156.paris",
			// The 4th test was buffered.
			expectedBufferedTest: 1,
			expectedNumRows:      2,
		},
		{
			fileName: "testdata/PT/20171208T22:03:54Z-104.198.139.160-60574-163.22.28.37-7999.paris",
			// The 5th test was buffered too.
			expectedBufferedTest: 2,
			expectedNumRows:      2,
		},
		{
			fileName: "testdata/PT/20171208T22:03:59Z-139.60.160.135-1519-163.22.28.44-1101.paris",
			// The 5th test was detected that was polluted by the 6th test.
			// It was removed from buffer (expectedBufferedTest drop from 2 to 1).
			// Buffer contains the 4th test now.
			expectedBufferedTest: 1,
			// The 6th test reached its destIP and was inserted into BigQuery.
			expectedNumRows: 3,
		},
	}

	// Process the tests
	for _, test := range tests {
		rawData, err := ioutil.ReadFile(test.fileName)
		if err != nil {
			t.Fatalf("cannot read testdata.")
		}
		err = pt.ParseAndInsert(nil, test.fileName, rawData)
		if err != nil {
			t.Fatalf(err.Error())
		}
		if pt.NumBufferedTests() != test.expectedBufferedTest {
			t.Fatalf("Data not buffered correctly")
		}
		if pt.NumRowsForTest() != test.expectedNumRows {
			t.Fatalf("Data of test %s not inserted into BigQuery correctly. Expect %d Actually %d", test.fileName, test.expectedNumRows, ins.RowsInBuffer())
		}
	}

	// Insert the 4th test in the buffer to BigQuery.
	pt.ProcessLastTests()
	if pt.NumRowsForTest() != 4 {
		t.Fatalf("Number of tests in buffer not correct, expect 4, actually %d.", ins.RowsInBuffer())
	}
}

func TestPTEmptyTest(t *testing.T) {
	rawData, err := ioutil.ReadFile("testdata/20180201T07:57:37Z-125.212.217.215-56622-208.177.76.115-9100.paris")
	if err != nil {
		fmt.Println("cannot load test data")
		return
	}
	_, parseErr := parser.Parse(nil, "testdata/20180201T07:57:37Z-125.212.217.215-56622-208.177.76.115-9100.paris", "", rawData, "pt-daily")
	if parseErr.Error() != "Empty Test." {
		t.Fatalf("Do not handle empty test correctly.")
	}
}

*/
