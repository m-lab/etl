package parser

import (
	"io/ioutil"
	"reflect"
	"testing"
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
	hops, err := Parse(nil, "testdata/20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris", rawData)
	if err != nil {
		t.Fatalf(err.Error())
	}

	expected_hops := []ParisTracerouteHop{
		ParisTracerouteHop{protocal: "tcp", src_ip: "64.233.174.109", src_af: 2, dest_ip: "74.125.224.100", dest_af: 2, src_hostname: "sr05-te1-8.nuq04.net.google.com", des_hostname: "74.125.224.100", rtt: []float64{0.895}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.232.136", src_af: 2, dest_ip: "64.233.174.109", dest_af: 2, src_hostname: "bb01-ae7.nuq04.net.google.com", des_hostname: "sr05-te1-8.nuq04.net.google.com", rtt: []float64{1.614}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.232.136", src_af: 2, dest_ip: "64.233.174.109", dest_af: 2, src_hostname: "bb01-ae7.nuq04.net.google.com", des_hostname: "sr05-te1-8.nuq04.net.google.com", rtt: []float64{1.614}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.232.136", src_af: 2, dest_ip: "64.233.174.109", dest_af: 2, src_hostname: "bb01-ae7.nuq04.net.google.com", des_hostname: "sr05-te1-8.nuq04.net.google.com", rtt: []float64{1.614}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.232.136", src_af: 2, dest_ip: "64.233.174.109", dest_af: 2, src_hostname: "bb01-ae7.nuq04.net.google.com", des_hostname: "sr05-te1-8.nuq04.net.google.com", rtt: []float64{1.614}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.232.136", src_af: 2, dest_ip: "64.233.174.109", dest_af: 2, src_hostname: "bb01-ae7.nuq04.net.google.com", des_hostname: "sr05-te1-8.nuq04.net.google.com", rtt: []float64{1.614}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.232.136", src_af: 2, dest_ip: "64.233.174.109", dest_af: 2, src_hostname: "bb01-ae7.nuq04.net.google.com", des_hostname: "sr05-te1-8.nuq04.net.google.com", rtt: []float64{1.614}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.232.136", src_af: 2, dest_ip: "64.233.174.109", dest_af: 2, src_hostname: "bb01-ae7.nuq04.net.google.com", des_hostname: "sr05-te1-8.nuq04.net.google.com", rtt: []float64{1.614}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.232.136", src_af: 2, dest_ip: "64.233.174.109", dest_af: 2, src_hostname: "bb01-ae7.nuq04.net.google.com", des_hostname: "sr05-te1-8.nuq04.net.google.com", rtt: []float64{1.614}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.232.136", src_af: 2, dest_ip: "64.233.174.109", dest_af: 2, src_hostname: "bb01-ae7.nuq04.net.google.com", des_hostname: "sr05-te1-8.nuq04.net.google.com", rtt: []float64{1.614}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.232.136", src_af: 2, dest_ip: "64.233.174.109", dest_af: 2, src_hostname: "bb01-ae7.nuq04.net.google.com", des_hostname: "sr05-te1-8.nuq04.net.google.com", rtt: []float64{1.614}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.232.136", src_af: 2, dest_ip: "64.233.174.109", dest_af: 2, src_hostname: "bb01-ae7.nuq04.net.google.com", des_hostname: "sr05-te1-8.nuq04.net.google.com", rtt: []float64{1.614}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.196.8", src_af: 2, dest_ip: "72.14.232.136", dest_af: 2, src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", des_hostname: "bb01-ae7.nuq04.net.google.com", rtt: []float64{1.693}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.196.8", src_af: 2, dest_ip: "72.14.232.136", dest_af: 2, src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", des_hostname: "bb01-ae7.nuq04.net.google.com", rtt: []float64{1.693}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.196.8", src_af: 2, dest_ip: "72.14.232.136", dest_af: 2, src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", des_hostname: "bb01-ae7.nuq04.net.google.com", rtt: []float64{1.693}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.196.8", src_af: 2, dest_ip: "72.14.232.136", dest_af: 2, src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", des_hostname: "bb01-ae7.nuq04.net.google.com", rtt: []float64{1.693}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.196.8", src_af: 2, dest_ip: "216.239.49.250", dest_af: 2, src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", des_hostname: "bb01-ae3.nuq04.net.google.com", rtt: []float64{1.386}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.196.8", src_af: 2, dest_ip: "216.239.49.250", dest_af: 2, src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", des_hostname: "bb01-ae3.nuq04.net.google.com", rtt: []float64{1.386}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.196.8", src_af: 2, dest_ip: "216.239.49.250", dest_af: 2, src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", des_hostname: "bb01-ae3.nuq04.net.google.com", rtt: []float64{1.386}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.196.8", src_af: 2, dest_ip: "216.239.49.250", dest_af: 2, src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", des_hostname: "bb01-ae3.nuq04.net.google.com", rtt: []float64{1.386}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.196.8", src_af: 2, dest_ip: "216.239.49.250", dest_af: 2, src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", des_hostname: "bb01-ae3.nuq04.net.google.com", rtt: []float64{1.386}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.196.8", src_af: 2, dest_ip: "216.239.49.250", dest_af: 2, src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", des_hostname: "bb01-ae3.nuq04.net.google.com", rtt: []float64{1.386}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "72.14.196.8", src_af: 2, dest_ip: "216.239.49.250", dest_af: 2, src_hostname: "pr02-xe-3-0-1.pao03.net.google.com", des_hostname: "bb01-ae3.nuq04.net.google.com", rtt: []float64{1.386}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "172.25.253.46", src_af: 2, dest_ip: "72.14.196.8", dest_af: 2, src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", des_hostname: "pr02-xe-3-0-1.pao03.net.google.com", rtt: []float64{0.556}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "172.25.253.46", src_af: 2, dest_ip: "72.14.196.8", dest_af: 2, src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", des_hostname: "pr02-xe-3-0-1.pao03.net.google.com", rtt: []float64{0.556}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "172.25.253.46", src_af: 2, dest_ip: "72.14.196.8", dest_af: 2, src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", des_hostname: "pr02-xe-3-0-1.pao03.net.google.com", rtt: []float64{0.556}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "172.25.253.46", src_af: 2, dest_ip: "72.14.196.8", dest_af: 2, src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", des_hostname: "pr02-xe-3-0-1.pao03.net.google.com", rtt: []float64{0.556}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "172.25.253.46", src_af: 2, dest_ip: "72.14.218.190", dest_af: 2, src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", des_hostname: "pr01-xe-7-1-0.pao03.net.google.com", rtt: []float64{0.53}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "172.25.253.46", src_af: 2, dest_ip: "72.14.218.190", dest_af: 2, src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", des_hostname: "pr01-xe-7-1-0.pao03.net.google.com", rtt: []float64{0.53}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "172.25.253.46", src_af: 2, dest_ip: "72.14.218.190", dest_af: 2, src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", des_hostname: "pr01-xe-7-1-0.pao03.net.google.com", rtt: []float64{0.53}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "172.25.253.46", src_af: 2, dest_ip: "72.14.218.190", dest_af: 2, src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", des_hostname: "pr01-xe-7-1-0.pao03.net.google.com", rtt: []float64{0.53}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "172.25.253.46", src_af: 2, dest_ip: "72.14.218.190", dest_af: 2, src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", des_hostname: "pr01-xe-7-1-0.pao03.net.google.com", rtt: []float64{0.53}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "172.25.253.46", src_af: 2, dest_ip: "72.14.218.190", dest_af: 2, src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", des_hostname: "pr01-xe-7-1-0.pao03.net.google.com", rtt: []float64{0.53}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "172.25.253.46", src_af: 2, dest_ip: "72.14.218.190", dest_af: 2, src_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", des_hostname: "pr01-xe-7-1-0.pao03.net.google.com", rtt: []float64{0.53}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "172.25.252.166", src_af: 2, dest_ip: "172.25.253.46", dest_af: 2, src_hostname: "us-mtv-ply1-bb1-tengigabitethernet2-3.n.corp.google.com", des_hostname: "us-mtv-ply1-br1-xe-1-1-0-706.n.corp.google.com", rtt: []float64{0.343}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "172.25.252.172", src_af: 2, dest_ip: "172.25.252.166", dest_af: 2, src_hostname: "us-mtv-cl4-core1-gigabitethernet1-1.n.corp.google.com", des_hostname: "us-mtv-ply1-bb1-tengigabitethernet2-3.n.corp.google.com", rtt: []float64{0.501}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "172.17.95.252", src_af: 2, dest_ip: "172.25.252.172", dest_af: 2, src_hostname: "172.17.95.252", des_hostname: "us-mtv-cl4-core1-gigabitethernet1-1.n.corp.google.com", rtt: []float64{0.407}},
		ParisTracerouteHop{protocal: "tcp", src_ip: "64.86.132.75", src_af: 2, dest_ip: "172.17.95.252", dest_af: 2, des_hostname: "172.17.95.252", rtt: []float64{0.376}},
	}
	if len(hops) != len(expected_hops) {
		t.Fatalf("Wrong results for PT hops!")
	}
	for i := 0; i < len(hops); i++ {
		if !reflect.DeepEqual(hops[i], expected_hops[i]) {
			t.Fatalf("Wrong results for PT hops!")
		}
	}
}
