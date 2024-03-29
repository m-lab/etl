package parser_test

import (
	"io/ioutil"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/traceroute-caller/hopannotation"
)

func TestParsePT(t *testing.T) {
	fileName := "20190825T000138Z_ndt-plh7v_1566050090_000000000004D64D.json"
	bytes, err := ioutil.ReadFile(filepath.Join("testdata/PT", fileName))
	if err != nil {
		t.Fatalf("failed to read file (error: %v)", err)
	}
	_, err = parser.ParsePT(fileName, bytes, "", "")
	if err != nil {
		t.Fatalf("failed to parse file %v (error: %v)", fileName, err)
	}

	fileName = "20190825T000138Z_ndt-plh7v_1566050090_000000000004D64E.json"
	bytes, err = ioutil.ReadFile(filepath.Join("testdata/PT", fileName))
	if err != nil {
		t.Fatalf("failed to read file (error: %v)", err)
	}
	got, err := parser.ParsePT(fileName, bytes, "", "fake.tgz")
	if err != nil {
		t.Fatalf("failed to parse file %v (error: %v)", fileName, err)
	}
	wantUUID := "ndt-rfwqf_1588021292_0000000000000242"
	if got.UUID != wantUUID {
		t.Fatalf("failed to parse UUID, wanted %q, got %q", wantUUID, got.UUID)
	}
	if !got.TestTime.Equal(time.Date(2019, 8, 25, 0, 1, 38, 0, time.UTC)) {
		t.Fatalf("failed to parse test time, wanted %v, got %v", "2019-08-25 01:38:00 +0000 UTC", got.TestTime)
	}
	if got.Parseinfo.Filename != fileName {
		t.Fatalf("failed to parse file name, wanted %v, got %v", fileName, got.Parseinfo.Filename)
	}

	fileName = "20210318T190500Z_ndt-klnff_1613689019_00000000000E86B8.json"
	bytes, err = ioutil.ReadFile(filepath.Join("testdata/PT", fileName))
	if err != nil {
		t.Fatalf("failed to parse file %v (error: %v)", fileName, err)
	}
	got, err = parser.ParsePT(fileName, bytes, "", "")
	if err != nil {
		t.Fatalf("failed to parse file %v (error: %v)", fileName, err)
	}
	wantUUID = "ndt-klnff_1613689019_00000000000E86B8"
	if got.UUID != wantUUID {
		t.Fatalf("failed to parse UUID, wanted %q, got %q", wantUUID, got.UUID)
	}
	if !got.TestTime.Equal(time.Date(2021, 3, 18, 19, 5, 0, 0, time.UTC)) {
		t.Fatalf("failed to parse test time, wanted %v, got %v", "2021-03-18 19:05:00 +0000 UTC", got.TestTime)
	}
	if got.Parseinfo.Filename != fileName {
		t.Fatalf("failed to parse file name, wanted %v, got %v", fileName, got.Parseinfo.Filename)
	}
}

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

func TestParseJSONLSimple(t *testing.T) {
	fileName := "20190825T000138Z_ndt-plh7v_1566050090_000000000004D64D.jsonl"
	bytes, err := ioutil.ReadFile(filepath.Join("testdata/PT", fileName))
	if err != nil {
		t.Fatalf("failed to read file (error: %v)", err)
	}
	wantUUID := "ndt-plh7v_1566050090_000000000004D64D"
	got, err := parser.ParseJSONL(fileName, bytes, "", "")
	if err != nil {
		t.Fatalf("failed to parse file %v (error: %v)", fileName, err)
	}
	if got.UUID != wantUUID {
		t.Fatalf("failed to parse UUID, wanted %q, got %q", wantUUID, got.UUID)
	}
	wantSourceIP := "180.87.97.101"
	if got.Source.IP != wantSourceIP {
		t.Fatalf("failed to parse source IP, wanted %q, got %q", wantSourceIP, got.Source.IP)
	}
	wantDestinationIP := "1.47.236.62"
	if got.Destination.IP != wantDestinationIP {
		t.Fatalf("failed to parse destination IP, wanted %q, got %q", wantDestinationIP, got.Destination.IP)
	}
	wantProbeSize := int64(60)
	if got.ProbeSize != wantProbeSize {
		t.Fatalf("failed to parse probe size, wanted %d, got %d", wantProbeSize, got.ProbeSize)
	}
	wantProbeC := int64(0)
	if got.ProbeC != wantProbeC {
		t.Fatalf("failed to parse probec, wanted %d, got %d", wantProbeC, got.ProbeC)
	}
	if got.Parseinfo.Filename != fileName {
		t.Fatalf("failed to parse file name, wanted %q, got %q", fileName, got.Parseinfo.Filename)
	}
}

func TestParseJSONLNoLinks(t *testing.T) {
	// Last object on the "type":"tracelb" line has "linkc":1 but no "links" set.
	fileName := "20190825T000138Z_ndt-plh7v_1566050090_000000000004D64F.jsonl"
	bytes, err := ioutil.ReadFile(filepath.Join("testdata/PT", fileName))
	if err != nil {
		t.Fatalf("failed to read file (error: %v)", err)
	}
	_, err = parser.ParseJSONL(fileName, bytes, "", "")
	if err != nil {
		t.Fatalf("failed to prase file %v (error: %v)", fileName, err)
	}
}

func TestParseJSONLComplex(t *testing.T) {
	fileName := "20190825T000138Z_ndt-plh7v_1566050090_000000000004D64C.jsonl"
	bytes, err := ioutil.ReadFile(filepath.Join("testdata/PT", fileName))
	if err != nil {
		t.Fatalf("failed to read file (error: %v)", err)
	}
	got, err := parser.ParseJSONL(fileName, bytes, "", "")
	if err != nil {
		t.Fatalf("failed to parse file %v (error: %v)", fileName, err)
	}
	wantUUID := "ndt-plh7v_1566050090_000000000004D60F"
	if got.UUID != wantUUID {
		t.Fatalf("failed to parse UUID, wanted %q, got %q", wantUUID, got.UUID)
	}
	wantSourceIP := "2001:550:1b01:1:e41d:2d00:151:f6c0"
	if got.Source.IP != wantSourceIP {
		t.Fatalf("failed to parse source IP, wanted %q, got %q", wantSourceIP, got.Source.IP)
	}
	wantDestinationIP := "2600:1009:b013:1a59:c369:b528:98fd:ab43"
	if got.Destination.IP != wantDestinationIP {
		t.Fatalf("failed to parse destination IP, wanted %q, got %q", wantDestinationIP, got.Destination.IP)
	}
	wantProbeSize := int64(60)
	if got.ProbeSize != wantProbeSize {
		t.Fatalf("failed to parse probe size, wanted %d, got %d", wantProbeSize, got.ProbeSize)
	}
	wantProbeC := int64(85)
	if got.ProbeC != wantProbeC {
		t.Fatalf("failed to parse probec, wanted %d, got %d", wantProbeC, got.ProbeC)
	}

	wantHop := schema.ScamperHop{
		Source: schema.HopIP{IP: "2001:550:1b01:1::1", ASN: 0,
			HopAnnotation1: &hopannotation.HopAnnotation1{ID: "20190825_ndt-plh7v_2001:550:1b01:1::1",
				Timestamp: time.Date(2019, time.August, 25, 00, 01, 8, 0, time.UTC)}},
		Linkc: 1,
		Links: []schema.HopLink{
			schema.HopLink{
				HopDstIP: "2001:550:3::1ca",
				TTL:      2,
				Probes: []schema.HopProbe{
					schema.HopProbe{Flowid: 1, Rtt: []float64{36.803}},
					schema.HopProbe{Flowid: 2, Rtt: []float64{0.332}},
					schema.HopProbe{Flowid: 3, Rtt: []float64{0.329}},
					schema.HopProbe{Flowid: 4, Rtt: []float64{0.567}},
					schema.HopProbe{Flowid: 5, Rtt: []float64{0.329}},
					schema.HopProbe{Flowid: 6, Rtt: []float64{1.237}},
				},
			},
		},
	}
	if !reflect.DeepEqual(got.Hop[0], wantHop) {
		t.Fatalf("failed to parse hops,\nwanted: %+v\ngot: %+v", wantHop, got.Hop[0])
	}
}

func TestParseFirstLine(t *testing.T) {
	line := "traceroute [(64.86.132.76:33461) -> (98.162.212.214:53849)], protocol icmp, algo exhaustive, duration 19 s"
	protocol, dest_ip, server_ip, err := parser.ParseFirstLine(line)
	if dest_ip != "98.162.212.214" || server_ip != "64.86.132.76" || protocol != "icmp" || err != nil {
		t.Errorf("Error in parsing the first line!\n")
		return
	}

	line = "traceroute [(64.86.132.76:33461) -> (2001:0db8:85a3:0000:0000:8a2e:0370:7334:53849)], protocol icmp, algo exhaustive, duration 19 s"
	protocol, dest_ip, server_ip, err = parser.ParseFirstLine(line)
	if dest_ip != "2001:0db8:85a3:0000:0000:8a2e:0370:7334" || server_ip != "64.86.132.76" || protocol != "icmp" || err != nil {
		t.Errorf("Error in parsing the first line!\n")
		return
	}

	line = "Exception : [ERROR](Probe.cc, 109)Can't send the probe : Invalid argument"
	protocol, dest_ip, server_ip, err = parser.ParseFirstLine(line)
	if err == nil {
		t.Errorf("Should return error for err message on the first line!\n")
		return
	}

	line = "traceroute to 35.243.216.203 (35.243.216.203), 30 hops max, 30 bytes packets"
	protocol, dest_ip, server_ip, err = parser.ParseFirstLine(line)
	if err == nil {
		t.Errorf("Should return error for unknown first line format!\n")
		return
	}

}

func TestCreateTestId(t *testing.T) {
	fn := "20170501T000000Z-mlab1-acc02-paris-traceroute-0000.tgz"
	bn := "20170501T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris"
	wantId := "2017/05/01/mlab1.acc02/20170501T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris.gz"
	gotId := parser.CreateTestId(fn, bn)
	if gotId != wantId {
		t.Errorf("failed to create test id, wanted: %q, got %q", wantId, gotId)
		return
	}
}

func TestParseLegacyFormatData(t *testing.T) {
	rawData, err := ioutil.ReadFile("testdata/PT/20160112T00:45:44Z_ALL27409.paris")
	if err != nil {
		t.Fatalf("cannot load test data: %v", err)
		return
	}
	cachedTest, err := parser.Parse(nil, "testdata/PT/20160112T00:45:44Z_ALL27409.paris", "", rawData, "pt-daily", etl.DataPath{})
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

func TestParseJSONL(t *testing.T) {
	ins := newInMemoryInserter()
	pt := parser.NewPTParser(ins, "paris1", "")

	filename := "testdata/PT/20190927T070859Z_ndt-qtfh8_1565996043_0000000000003B64.jsonl"
	rawData, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatalf(err.Error())
		return
	}

	url := "gs://archive-measurement-lab/ndt/traceroute/2019/09/27/20190927T000540.410989Z-traceroute-mlab2-nuq07-ndt.tgz"
	meta := map[string]bigquery.Value{"filename": url}
	err = pt.ParseAndInsert(meta, filename, rawData)
	if err != nil {
		t.Fatalf(err.Error())
	}

	s := pt.GetStats()
	if s.Total() != 1 {
		t.Fatalf("ParseJSONL expected 1 row inserted, got %d", s.Total())
	}
	pt.Flush()

	if len(ins.data) != 1 {
		t.Fatalf("ParseJSONL expected 1 row in buffer, got %d", len(ins.data))
	}

	ptTest := ins.data[0].(*schema.PTTest)
	if ptTest.Parseinfo.TaskFileName != url {
		t.Fatalf("Wrong TaskFilenName; got %q, want %q", ptTest.Parseinfo.TaskFileName, url)
	}

	if ptTest.UUID != "ndt-qtfh8_1565996043_0000000000003B64" {
		t.Fatalf("Wrong UUID; got %q, want %q", ptTest.UUID, "ndt-qtfh8_1565996043_0000000000003B64")
	}
}

func TestParse(t *testing.T) {
	rawData, err := ioutil.ReadFile("testdata/PT/20170320T23:53:10Z-172.17.94.34-33456-74.125.224.100-33457.paris")
	dp, _ := etl.ValidateTestPath("gs://archive-measurement-lab/paris-traceroute/2017/03/20/20170320T000000Z-mlab1-lax05-paris-traceroute-0000.tgz")
	cachedTest, err := parser.Parse(nil, "testdata/PT/20170320T23:53:10Z-172.17.94.34-33456-74.125.224.100-33457.paris", "", rawData,
		"pt-daily", dp)
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
			HopAnnotation1: &hopannotation.HopAnnotation1{
				ID:        "20170320_mlab1-lax05_64.233.174.109",
				Timestamp: cachedTest.LogTime,
			},
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
		t.Logf("Here is expected    : %v\n", expected_hop)
		t.Logf("Here is what is real: %v\n", cachedTest.Hops[0])
		t.Fatalf("Wrong results for PT hops!")
	}
}

func TestParseAndInsert(t *testing.T) {
	ins := newInMemoryInserter()
	pt := parser.NewPTParser(ins, "paris1", "")
	rawData, err := ioutil.ReadFile("testdata/PT/20130524T00:04:44Z_ALL5729.paris")
	if err != nil {
		t.Fatalf("cannot read testdata.")
	}
	url := "gs://archive-measurement-lab/paris-traceroute/2013/05/24/20130524T000000Z-mlab3-akl01-paris-traceroute-0000.tgz"
	meta := map[string]bigquery.Value{"filename": url}
	err = pt.ParseAndInsert(meta, "testdata/PT/20130524T00:04:44Z_ALL5729.paris", rawData)
	if err != nil {
		t.Fatalf(err.Error())
	}

	s := pt.GetStats()
	if s.Buffered != 0 {
		t.Fatalf("ParseAndInsert with buffered data; want 0, got %d", s.Buffered)
	}
	pt.Flush()

	s = pt.GetStats()
	if s.Committed != 1 {
		t.Fatalf("ParseAndInsert committed wrong row count; want 1, got %d", s.Committed)
	}

	if ins.data[0].(*schema.PTTest).Parseinfo.TaskFileName != url {
		t.Fatalf("Task filename is wrong.")
	}
	// echo -n 2013-05-24T00:04:44Z-91.239.96.102-2.80.132.33 | openssl dgst -binary -md5 | base64  | tr '/+' '_-' | tr -d '='
	if ins.data[0].(*schema.PTTest).UUID != "R9_wGx1-cSmqtSAt5aQtNg" {
		t.Fatalf("UUID is wrong; got %q, want %q", ins.data[0].(*schema.PTTest).UUID, "R9_wGx1-cSmqtSAt5aQtNg")
	}
}

func TestProcessLastTests(t *testing.T) {
	ins := &inMemoryInserter{}
	pt := parser.NewPTParser(ins, "paris1", "")

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
		meta := map[string]bigquery.Value{"filename": test.fileName, "parse_time": time.Now()}
		err = pt.ParseAndInsert(meta, test.fileName, rawData)
		if err != nil {
			t.Fatalf(err.Error())
		}
		if pt.NumBufferedTests() != test.expectedBufferedTest {
			t.Fatalf("Data not buffered correctly")
		}
		s := pt.GetStats()
		if s.Total() != test.expectedNumRows {
			t.Fatalf("Data of test %s not inserted into BigQuery correctly. Expect %d Actually %d", test.fileName, test.expectedNumRows, ins.RowsInBuffer())
		}
	}

	// Insert the 4th test in the buffer to BigQuery.
	pt.ProcessLastTests()
	s := pt.GetStats()
	if s.Total() != 4 {
		t.Fatalf("Number of tests in buffer not correct, expect 0, actually %d.", ins.RowsInBuffer())
	}
}

func TestParseEmpty(t *testing.T) {
	rawData, err := ioutil.ReadFile("testdata/PT/20180201T07:57:37Z-125.212.217.215-56622-208.177.76.115-9100.paris")
	if err != nil {
		t.Fatal("cannot load test data")
		return
	}
	_, parseErr := parser.Parse(nil, "testdata/PT/20180201T07:57:37Z-125.212.217.215-56622-208.177.76.115-9100.paris", "", rawData, "pt-daily",
		etl.DataPath{})
	if parseErr == nil {
		t.Fatal(parseErr)
	}
}
