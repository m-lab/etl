package tcpip_test

import (
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
	"testing"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/m-lab/annotation-service/site"
	"github.com/m-lab/etl/headers"
	"github.com/m-lab/etl/tcpip"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func getTestfileForBenchmark(b *testing.B, name string) []byte {
	f, err := os.Open(path.Join(`testdata/`, name))
	if err != nil {
		b.Fatal(err)
	}
	data, err := ioutil.ReadAll(f)
	if err != nil {
		b.Fatal(err)
	}
	return data
}

func getTestfile(t *testing.T, name string) []byte {
	f, err := os.Open(path.Join(`testdata/`, name))
	if err != nil {
		t.Fatal(err)
	}
	data, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// SlowGetIP decodes the IP layers and returns some basic information.
// It is a bit slow and does memory allocation.
func SlowGetIP(p *tcpip.Packet) (net.IP, net.IP, uint8, uint16, error) {
	// Decode a packet.
	pkt := gopacket.NewPacket(p.Data, layers.LayerTypeEthernet, gopacket.DecodeOptions{
		Lazy:                     true,
		NoCopy:                   true,
		SkipDecodeRecovery:       true,
		DecodeStreamsAsDatagrams: false,
	})

	if ipLayer := pkt.Layer(layers.LayerTypeIPv4); ipLayer != nil {
		ip, _ := ipLayer.(*layers.IPv4)
		// For IPv4, the TTL length is the ip.Length adjusted for the header length.
		return ip.SrcIP, ip.DstIP, ip.TTL, ip.Length - uint16(4*ip.IHL), nil
	} else if ipLayer := pkt.Layer(layers.LayerTypeIPv6); ipLayer != nil {
		ip, _ := ipLayer.(*layers.IPv6)
		// In IPv6, the Length field is the payload length.
		return ip.SrcIP, ip.DstIP, ip.HopLimit, ip.Length, nil
	} else {
		return nil, nil, 0, 0, tcpip.ErrNoIPLayer
	}
}

func TestIPLayer(t *testing.T) {
	site.MustLoad(time.Minute)

	type test struct {
		name             string
		fn               string
		packets          int64
		duration         time.Duration
		srcIP, dstIP     string
		srcPort, dstPort layers.TCPPort
		TTL              uint8
		totalPayload     int
	}
	tests := []test{
		{name: "retransmits", fn: "ndt-nnwk2_1611335823_00000000000C2DFE.pcap.gz",
			packets: 336, duration: 15409174000, srcIP: "173.49.19.128", srcPort: 40337, dstPort: 443},
		{name: "ipv6", fn: "ndt-nnwk2_1611335823_00000000000C2DA8.pcap.gz",
			packets: 15, duration: 134434000, srcIP: "2a0d:5600:24:a71::1d", srcPort: 1894, dstPort: 443},
		{name: "protocolErrors2", fn: "ndt-nnwk2_1611335823_00000000000C2DA9.pcap.gz",
			packets: 5180, duration: 13444117000, srcIP: "2a0d:5600:24:a71::1d", srcPort: 1896, dstPort: 443},

		{name: "large_ipv4_1", fn: "ndt-m6znc_1632401351_000000000005BA77.pcap.gz",
			packets: 40797, duration: 10719662000, srcIP: "70.187.37.14", srcPort: 60232, dstPort: 443, totalPayload: 239251626},
		{name: "large_ipv6", fn: "ndt-m6znc_1632401351_000000000005B9EA.pcap.gz",
			packets: 146172, duration: 15081049000, srcIP: "2600:1700:42d0:67b0:71e7:d89:1d89:9484", srcPort: 49319, dstPort: 443, totalPayload: 158096007},
		{name: "large_ipv4_2", fn: "ndt-m6znc_1632401351_000000000005B90B.pcap.gz",
			packets: 30097, duration: 11415041000, srcIP: "104.129.205.7", srcPort: 15227, dstPort: 443, totalPayload: 126523401},

		{name: "Nops", fn: "ndt-nnwk2_1611335823_00000000000C2DA2.pcap.gz", srcIP: "69.124.153.192", srcPort: 3855, dstPort: 3010,
			packets: 18, duration: 173433000},
	}
	for _, tt := range tests {
		data := getTestfile(t, tt.fn)
		summary, err := tcpip.ProcessPackets("none", tt.fn, data)
		if err != nil {
			t.Fatal(err)
		}
		duration := summary.LastTime.Sub(summary.StartTime)
		if duration != tt.duration {
			t.Errorf("%s: duration = %v, want %v", tt.name, duration, tt.duration)
		}
		if summary.Packets != int(tt.packets) {
			t.Errorf("%s: expected %d packets, got %d", tt.name, tt.packets, summary.Packets)
		}

		if !summary.Client().SrcIP.Equal(net.ParseIP(tt.srcIP)) {
			t.Errorf("%s: srcIP = %s, want %s", tt.name, summary.Server().SrcIP, tt.srcIP)
		}

		t.Logf("%+v\n", summary)
	}
}

func ProcessShortPackets(t *testing.T, data []byte) {
	pr, err := headers.NewPCAPReader(data)
	if err != nil {
		t.Fatal(err)
	}

	// Check magic number?
	if len(data) < 4 {
		//	return summary, ErrTruncatedPcap
	}
	if data[0] != 0xd4 && data[1] != 0xc3 && data[2] != 0xb2 && data[3] != 0xa1 {
		// For compressed data, the 8x factor is based on testing with a few large gzipped files.
	}

	p := tcpip.Packet{}
	hp := headers.Packet{}
	for err := pr.Next(&hp); err == nil; err = pr.Next(&hp) {
		for i := 0; i < len(data); i++ {
			p.From(hp)
			p.From(hp)
		}
	}
}

func TestShortData(t *testing.T) {
	type test struct {
		name             string
		fn               string
		packets          int64
		duration         time.Duration
		srcIP, dstIP     string
		srcPort, dstPort layers.TCPPort
		TTL              uint8
		totalPayload     int
	}
	tests := []test{
		{name: "retransmits", fn: "ndt-nnwk2_1611335823_00000000000C2DFE.pcap.gz",
			packets: 336, duration: 15409174000, srcIP: "173.49.19.128", srcPort: 40337, dstPort: 443},
		{name: "ipv6", fn: "ndt-nnwk2_1611335823_00000000000C2DA8.pcap.gz",
			packets: 15, duration: 134434000, srcIP: "2a0d:5600:24:a71::1d", srcPort: 1894, dstPort: 443},
	}
	for _, tt := range tests {
		data := getTestfile(t, tt.fn)
		ProcessShortPackets(t, data)
	}
}

func TestPCAPGarbage(t *testing.T) {
	data := []byte{0xd4, 0xc3, 0xb2, 0xa1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	_, err := tcpip.ProcessPackets("none", "garbage", data)
	if err != io.ErrUnexpectedEOF {
		t.Fatal(err)
	}

	data = append(data, data...)
	_, err = tcpip.ProcessPackets("none", "garbage", data)
	if err == nil {
		//	t.Fatal(err)
	}
}

// goos: darwin goarch: amd64 pkg: github.com/m-lab/etl/tcpip cpu: Intel(R) Core(TM) i7-7920HQ CPU @ 3.10GHz
// BenchmarkProcessPackets2-8   	     219	   5546192 ns/op	 318.32 MB/s	     36616 packets/op	 2146663 B/op	   98347 allocs/op
// BenchmarkProcessPackets2-8   	     261	   4763381 ns/op	 370.63 MB/s	     36694 packets/op	 1259030 B/op	   25171 allocs/op

func BenchmarkProcessPackets2(b *testing.B) {
	type tt struct {
		data                  []byte
		numPkts               int
		ipPayloadBytes        int
		leftSacks, rightSacks int
	}
	tests := []tt{
		// Approximately 220K packets, so this is about 140nsec/packet, and about 100 bytes/packet allocated,
		// which is roughly the footprint of the packets themselves.
		{getTestfileForBenchmark(b, "ndt-nnwk2_1611335823_00000000000C2DA8.pcap.gz"), 15, 4574, 0, 0},
		{getTestfileForBenchmark(b, "ndt-nnwk2_1611335823_00000000000C2DFE.pcap.gz"), 336, 167003, 31, 24}, // retransmits and SACKs
		{getTestfileForBenchmark(b, "ndt-nnwk2_1611335823_00000000000C2DA9.pcap.gz"), 5180, 81408294, 0, 0},
		{getTestfileForBenchmark(b, "ndt-m6znc_1632401351_000000000005BA77.pcap.gz"), 40797, 239251626, 70557, 207},
		{getTestfileForBenchmark(b, "ndt-m6znc_1632401351_000000000005B9EA.pcap.gz"), 146172, 158096007, 7, 195},
		{getTestfileForBenchmark(b, "ndt-m6znc_1632401351_000000000005B90B.pcap.gz"), 30097, 126523401, 0, 0},
	}
	b.ReportAllocs()
	b.ResetTimer()

	b.ReportMetric(220000, "packets/op")

	i := 0

	numPkts := 0
	ops := 0
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			test := tests[i%len(tests)]
			ops++
			numPkts += test.numPkts
			i++
			summary, err := tcpip.ProcessPackets("foo", "bar", test.data)
			if err != nil {
				b.Fatal(err)
			}
			if summary.Packets != test.numPkts {
				b.Errorf("expected %d packets, got %d", test.numPkts, summary.Packets)
			}
			b.SetBytes(int64(len(test.data)))
		}
	})
	b.Log("total packets", numPkts, "total ops", ops)
	b.ReportMetric(float64(numPkts/ops), "packets/op")
}
