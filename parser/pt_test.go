package parser_test

import (
	"fmt"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
)

// TODO: IPv6 tests
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
	}
}

func TestPTParser(t *testing.T) {
	rawData, err := ioutil.ReadFile("testdata/20170320T23:53:10Z-172.17.94.34-33456-74.125.224.100-33457.paris")
	hops, logTime, conn_spec, err := parser.Parse(nil, "testdata/20170320T23:53:10Z-172.17.94.34-33456-74.125.224.100-33457.paris", rawData, "pt-daily")
	if err != nil {
		t.Fatalf(err.Error())
	}
	if logTime != 1490053990 {
		t.Fatalf("Do not process log time correctly.")
	}

	expected_cspec := schema.MLabConnectionSpecification{
		Server_ip:      "172.17.94.34",
		Server_af:      2,
		Client_ip:      "74.125.224.100",
		Client_af:      2,
		Data_direction: 0,
	}
	if !reflect.DeepEqual(*conn_spec, expected_cspec) {
		t.Fatalf("Wrong results for connection spec!")
	}

	// TODO(dev): reformat these individual values to be more readable.
	expected_hops := []schema.ParisTracerouteHop{
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "64.233.174.109", Src_af: 2, Dest_ip: "74.125.224.100", Dest_af: 2, Src_hostname: "sr05-te1-8.nuq04.net.google.com", Dest_hostname: "74.125.224.100", Rtt: []float64{0.895}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Dest_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Dest_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Dest_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Dest_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Dest_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Dest_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Dest_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Dest_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Dest_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Dest_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Dest_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "72.14.232.136", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Dest_hostname: "bb01-ae7.nuq04.net.google.com", Rtt: []float64{1.693}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "72.14.232.136", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Dest_hostname: "bb01-ae7.nuq04.net.google.com", Rtt: []float64{1.693}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "72.14.232.136", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Dest_hostname: "bb01-ae7.nuq04.net.google.com", Rtt: []float64{1.693}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "72.14.232.136", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Dest_hostname: "bb01-ae7.nuq04.net.google.com", Rtt: []float64{1.693}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "216.239.49.250", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Dest_hostname: "bb01-ae3.nuq04.net.google.com", Rtt: []float64{1.386}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "216.239.49.250", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Dest_hostname: "bb01-ae3.nuq04.net.google.com", Rtt: []float64{1.386}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "216.239.49.250", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Dest_hostname: "bb01-ae3.nuq04.net.google.com", Rtt: []float64{1.386}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "216.239.49.250", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Dest_hostname: "bb01-ae3.nuq04.net.google.com", Rtt: []float64{1.386}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "216.239.49.250", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Dest_hostname: "bb01-ae3.nuq04.net.google.com", Rtt: []float64{1.386}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "216.239.49.250", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Dest_hostname: "bb01-ae3.nuq04.net.google.com", Rtt: []float64{1.386}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "216.239.49.250", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Dest_hostname: "bb01-ae3.nuq04.net.google.com", Rtt: []float64{1.386}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.196.8", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Dest_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Rtt: []float64{0.556}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.196.8", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Dest_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Rtt: []float64{0.556}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.196.8", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Dest_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Rtt: []float64{0.556}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.196.8", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Dest_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Rtt: []float64{0.556}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.218.190", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Dest_hostname: "pr01-xe-7-1-0.pao03.net.google.com", Rtt: []float64{0.53}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.218.190", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Dest_hostname: "pr01-xe-7-1-0.pao03.net.google.com", Rtt: []float64{0.53}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.218.190", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Dest_hostname: "pr01-xe-7-1-0.pao03.net.google.com", Rtt: []float64{0.53}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.218.190", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Dest_hostname: "pr01-xe-7-1-0.pao03.net.google.com", Rtt: []float64{0.53}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.218.190", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Dest_hostname: "pr01-xe-7-1-0.pao03.net.google.com", Rtt: []float64{0.53}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.218.190", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Dest_hostname: "pr01-xe-7-1-0.pao03.net.google.com", Rtt: []float64{0.53}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.218.190", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Dest_hostname: "pr01-xe-7-1-0.pao03.net.google.com", Rtt: []float64{0.53}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.252.166", Src_af: 2, Dest_ip: "172.25.253.46", Dest_af: 2, Src_hostname: "us-mtv-ply1-bb1-tengigabitethernet2-3.n.corp.google.com", Dest_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Rtt: []float64{0.343}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.252.172", Src_af: 2, Dest_ip: "172.25.252.166", Dest_af: 2, Src_hostname: "us-mtv-cl4-core1-gigabitethernet1-1.n.corp.google.com", Dest_hostname: "us-mtv-ply1-bb1-tengigabitethernet2-3.n.corp.google.com", Rtt: []float64{0.501}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.17.95.252", Src_af: 2, Dest_ip: "172.25.252.172", Dest_af: 2, Src_hostname: "172.17.95.252", Dest_hostname: "us-mtv-cl4-core1-gigabitethernet1-1.n.corp.google.com", Rtt: []float64{0.407}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.17.94.34", Src_af: 2, Dest_ip: "172.17.95.252", Dest_af: 2, Dest_hostname: "172.17.95.252", Rtt: []float64{0.376}},
	}
	if len(hops) != len(expected_hops) {
		t.Fatalf("Wrong results for PT hops!")
	}
	for i := 0; i < len(hops); i++ {
		if !reflect.DeepEqual(hops[i], expected_hops[i]) {
			fmt.Printf("Here is expected    : %v\n", expected_hops[i])
			fmt.Printf("Here is what is real: %v\n", hops[i])
			t.Fatalf("Wrong results for PT hops!")
		}
	}
}

func TestPTInserter(t *testing.T) {
	ins := &inMemoryInserter{}
	n := parser.NewPTParser(ins)
	rawData, err := ioutil.ReadFile("testdata/20170320T23:53:10Z-172.17.94.34-33456-74.125.224.100-33457.paris")
	if err != nil {
		t.Fatalf("cannot read testdata.")
	}
	err = n.ParseAndInsert(nil, "testdata/20170320T23:53:10Z-172.17.94.34-33456-74.125.224.100-33457.paris", rawData)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if ins.RowsInBuffer() != 38 {
		t.Fatalf("Number of rows in PT table is wrong.")
	}

	expectedValues := &schema.PT{
		Test_id:  "20170320T23:53:10Z-172.17.94.34-33456-74.125.224.100-33457.paris",
		Project:  3,
		Log_time: 1490053990,
		Connection_spec: schema.MLabConnectionSpecification{
			Server_ip:      "172.17.94.34",
			Server_af:      2,
			Client_ip:      "74.125.224.100",
			Client_af:      2,
			Data_direction: 0,
		},
		Paris_traceroute_hop: schema.ParisTracerouteHop{
			Protocol:      "tcp",
			Src_ip:        "64.233.174.109",
			Src_af:        2,
			Dest_ip:       "74.125.224.100",
			Dest_af:       2,
			Src_hostname:  "sr05-te1-8.nuq04.net.google.com",
			Dest_hostname: "74.125.224.100",
			Rtt:           []float64{0.895},
		},
		Type: 2,
	}
	if !reflect.DeepEqual(ins.data[0], *expectedValues) {
		fmt.Printf("Here is expected    : %v\n", expectedValues)
		fmt.Printf("Here is what is real: %v\n", ins.data[0])
		t.Errorf("Not the expected values:")
	}
}
