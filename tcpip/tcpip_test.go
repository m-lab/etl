package tcpip_test

import (
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/m-lab/etl/parser"
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

func TestIPLayer(t *testing.T) {
	type test struct {
		name             string
		fn               string
		packets          int64
		duration         time.Duration
		srcIP, dstIP     string
		srcPort, dstPort uint16
		TTL              uint8
	}
	tests := []test{
		{name: "retransmits", fn: "ndt-nnwk2_1611335823_00000000000C2DFE.pcap.gz",
			packets: 336, duration: 15409174000, srcIP: "173.49.19.128", srcPort: 17664, dstPort: 60},
		{name: "ipv6", fn: "ndt-nnwk2_1611335823_00000000000C2DA8.pcap.gz",
			packets: 15, duration: 134434000, srcIP: "2a0d:5600:24:a71::1d", srcPort: 24576, dstPort: 31324},
		{name: "protocolErrors2", fn: "ndt-nnwk2_1611335823_00000000000C2DA9.pcap.gz",
			packets: 5180, duration: 13444117000, srcIP: "2a0d:5600:24:a71::1d", srcPort: 24587, dstPort: 43482},
	}
	for _, tt := range tests {
		data := getTestfile(t, tt.fn)
		packets, err := parser.GetPackets(data)
		if err != nil {
			t.Fatal(err)
		}
		for i := range packets {
			_, err := tcpip.Wrap(packets[i].Ci, packets[i].Data)
			if err != nil {
				t.Fatalf("%s %v", tt.name, err)
			}
		}

		start := packets[0].Ci.Timestamp
		end := packets[len(packets)-1].Ci.Timestamp
		duration := end.Sub(start)
		if duration != tt.duration {
			t.Errorf("%s: duration = %v, want %v", tt.name, duration, tt.duration)
		}
		if len(packets) != int(tt.packets) {
			t.Errorf("%s: expected %d packets, got %d", tt.name, tt.packets, len(packets))
		}

		all, err := tcpip.Wrap(packets[0].Ci, packets[0].Data)
		if err != nil {
			t.Fatal(err)
		}
		if !all.IP().SrcIP().Equal(net.ParseIP(tt.srcIP)) {
			t.Errorf("%s: srcIP = %v, want %v", tt.name, all.IP().SrcIP(), tt.srcIP)
		}
		if all.TCP().SrcPort() != tt.srcPort {
			t.Errorf("%s: srcPort = %v, want %v", tt.name, all.TCP().SrcPort(), tt.srcPort)
		}
		if all.TCP().DstPort() != tt.dstPort {
			t.Errorf("%s: dstPort = %v, want %v", tt.name, all.TCP().DstPort(), tt.dstPort)
		}

	}
}

func TestPCAPGarbage(t *testing.T) {
	data := []byte{0xd4, 0xc3, 0xb2, 0xa1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	_, err := parser.GetPackets(data)
	if err != io.ErrUnexpectedEOF {
		t.Fatal(err)
	}

	data = append(data, data...)
	_, err = parser.GetPackets(data)
	if err == nil || !strings.Contains(err.Error(), "Unknown major") {
		t.Fatal(err)
	}
}

// Original single file RunParallel:
// Just packet decoding: BenchmarkGetPackets-8   	    8678	    128426 ns/op	  165146 B/op	     381 allocs/op
// With IP decoding:     BenchmarkGetPackets-8   	    4279	    285547 ns/op	  376125 B/op	    1729 allocs/op
// Enhanced RunParallel: BenchmarkGetPackets-8   	    2311	    514898 ns/op	 1181138 B/op	    1886 allocs/op
//                       BenchmarkGetPackets-8   	    1766	    598328 ns/op	 1549714 B/op	    5576 allocs/op

// Enhanced RunParallel:
// Just packet decoding:     BenchmarkGetPackets-8   	    2311	    514898 ns/op	 1181138 B/op	    1886 allocs/op
// Fast Total Length:        BenchmarkGetPackets-8   	    1927	    595346 ns/op	 1550638 B/op	    5579 allocs/op
// Fast Total, len(data)/18: BenchmarkGetPackets-8   	    3130	    363601 ns/op	  613647 B/op	    5573 allocs/op
// Fast Total, len(data)/25: BenchmarkGetPackets-8   	    2299	    466285 ns/op	 1052745 B/op	    5573 allocs/op
// Fast Total, []*Packet:    BenchmarkGetPackets-8   	    2760	    419538 ns/op	  635526 B/op	    7410 allocs/op
// Fast Total, computed *6:  BenchmarkGetPackets-8   	    2769	    409313 ns/op	  850179 B/op	    5570 allocs/op
// Fast Total, computed *7:  BenchmarkGetPackets-8   	    3198	    379535 ns/op	  610168 B/op	    5570 allocs/op
// Wrap                      BenchmarkGetPackets-8   	    3045	    358205 ns/op	  610127 B/op	    5570 allocs/op

func BenchmarkGetPackets(b *testing.B) {
	type tt struct {
		data    []byte
		numPkts int
		total   int
	}
	tests := []tt{
		{getTestfileForBenchmark(b, "ndt-nnwk2_1611335823_00000000000C2DFE.pcap.gz"), 336, 167003},
		{getTestfileForBenchmark(b, "ndt-nnwk2_1611335823_00000000000C2DA8.pcap.gz"), 15, 4574},
		{getTestfileForBenchmark(b, "ndt-nnwk2_1611335823_00000000000C2DA9.pcap.gz"), 5180, 81408294},
	}
	b.ResetTimer()

	i := 0
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			test := tests[i%len(tests)]
			i++
			pkts, err := parser.GetPackets(test.data)
			if err != nil {
				b.Fatal(err)
			}
			if true {
				total := 0
				for i := range pkts {
					if err := pkts[i].GetLayers(); err != nil {
						b.Fatal(err)
					}
					_, _, _, tcpLength, _ := pkts[i].FastExtractIPFields()
					total += int(tcpLength)

					_, err := tcpip.Wrap(pkts[i].Ci, pkts[i].Data)
					if err != nil {
						b.Fatal(err)
					}
				}
				if total != test.total {
					b.Errorf("total = %d, want %d (%d)", total, test.total, len(test.data))
				}
			}
			if len(pkts) != test.numPkts {
				b.Errorf("expected %d packets, got %d", test.numPkts, len(pkts))
			}
		}
	})
}
