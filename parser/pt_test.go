package parser

import (
	"fmt"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/m-lab/etl/schema"
)

// TODO: IPv6 tests
func TestGetIPTuple(t *testing.T) {
	fn1 := PTFileName{name: "20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris"}
	dest_ip, dest_port, server_ip, server_port := fn1.GetIPTuple()
	if dest_ip != "98.162.212.214" || dest_port != "53849" || server_ip != "64.86.132.75" || server_port != "42677" {
		t.Errorf("Wrong file name parsing!\n")
		return
	}

}

func TestPTParser(t *testing.T) {
	rawData, err := ioutil.ReadFile("testdata/20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris")
	hops, logTime, conn_spec, err := Parse(nil, "testdata/20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris", rawData)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if logTime != 1490053990 {
		t.Fatalf("Do not process log time correctly.")
	}

	expected_cspec := schema.MLabConnectionSpecification{Server_ip: "64.86.132.75", Server_af: 2, Client_ip: "98.162.212.214", Client_af: 2, Data_direction: 0}
	if !reflect.DeepEqual(*conn_spec, expected_cspec) {
		t.Fatalf("Wrong results for connection spec!")
	}

	expected_hops := []schema.ParisTracerouteHop{
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "64.233.174.109", Src_af: 2, Dest_ip: "74.125.224.100", Dest_af: 2, Src_hostname: "sr05-te1-8.nuq04.net.google.com", Des_hostname: "74.125.224.100", Rtt: []float64{0.895}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Des_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Des_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Des_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Des_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Des_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Des_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Des_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Des_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Des_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Des_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.232.136", Src_af: 2, Dest_ip: "64.233.174.109", Dest_af: 2, Src_hostname: "bb01-ae7.nuq04.net.google.com", Des_hostname: "sr05-te1-8.nuq04.net.google.com", Rtt: []float64{1.614}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "72.14.232.136", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Des_hostname: "bb01-ae7.nuq04.net.google.com", Rtt: []float64{1.693}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "72.14.232.136", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Des_hostname: "bb01-ae7.nuq04.net.google.com", Rtt: []float64{1.693}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "72.14.232.136", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Des_hostname: "bb01-ae7.nuq04.net.google.com", Rtt: []float64{1.693}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "72.14.232.136", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Des_hostname: "bb01-ae7.nuq04.net.google.com", Rtt: []float64{1.693}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "216.239.49.250", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Des_hostname: "bb01-ae3.nuq04.net.google.com", Rtt: []float64{1.386}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "216.239.49.250", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Des_hostname: "bb01-ae3.nuq04.net.google.com", Rtt: []float64{1.386}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "216.239.49.250", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Des_hostname: "bb01-ae3.nuq04.net.google.com", Rtt: []float64{1.386}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "216.239.49.250", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Des_hostname: "bb01-ae3.nuq04.net.google.com", Rtt: []float64{1.386}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "216.239.49.250", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Des_hostname: "bb01-ae3.nuq04.net.google.com", Rtt: []float64{1.386}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "216.239.49.250", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Des_hostname: "bb01-ae3.nuq04.net.google.com", Rtt: []float64{1.386}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "72.14.196.8", Src_af: 2, Dest_ip: "216.239.49.250", Dest_af: 2, Src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Des_hostname: "bb01-ae3.nuq04.net.google.com", Rtt: []float64{1.386}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.196.8", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Des_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Rtt: []float64{0.556}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.196.8", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Des_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Rtt: []float64{0.556}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.196.8", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Des_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Rtt: []float64{0.556}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.196.8", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Des_hostname: "pr02-xe-3-0-1.pao03.net.google.com", Rtt: []float64{0.556}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.218.190", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Des_hostname: "pr01-xe-7-1-0.pao03.net.google.com", Rtt: []float64{0.53}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.218.190", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Des_hostname: "pr01-xe-7-1-0.pao03.net.google.com", Rtt: []float64{0.53}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.218.190", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Des_hostname: "pr01-xe-7-1-0.pao03.net.google.com", Rtt: []float64{0.53}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.218.190", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Des_hostname: "pr01-xe-7-1-0.pao03.net.google.com", Rtt: []float64{0.53}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.218.190", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Des_hostname: "pr01-xe-7-1-0.pao03.net.google.com", Rtt: []float64{0.53}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.218.190", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Des_hostname: "pr01-xe-7-1-0.pao03.net.google.com", Rtt: []float64{0.53}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.253.46", Src_af: 2, Dest_ip: "72.14.218.190", Dest_af: 2, Src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Des_hostname: "pr01-xe-7-1-0.pao03.net.google.com", Rtt: []float64{0.53}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.252.166", Src_af: 2, Dest_ip: "172.25.253.46", Dest_af: 2, Src_hostname: "us-mtv-ply1-bb1-tengigabitethernet2-3.n.corp.google.com", Des_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", Rtt: []float64{0.343}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.25.252.172", Src_af: 2, Dest_ip: "172.25.252.166", Dest_af: 2, Src_hostname: "us-mtv-cl4-core1-gigabitethernet1-1.n.corp.google.com", Des_hostname: "us-mtv-ply1-bb1-tengigabitethernet2-3.n.corp.google.com", Rtt: []float64{0.501}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "172.17.95.252", Src_af: 2, Dest_ip: "172.25.252.172", Dest_af: 2, Src_hostname: "172.17.95.252", Des_hostname: "us-mtv-cl4-core1-gigabitethernet1-1.n.corp.google.com", Rtt: []float64{0.407}},
		schema.ParisTracerouteHop{Protocol: "tcp", Src_ip: "64.86.132.75", Src_af: 2, Dest_ip: "172.17.95.252", Dest_af: 2, Des_hostname: "172.17.95.252", Rtt: []float64{0.376}},
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
