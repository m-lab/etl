package tcpip_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcapgo"
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
		if len(summary.Errors) > 0 {
			t.Error(summary.Errors)
		}

		duration := summary.LastTime.Sub(summary.StartTime)
		if duration != tt.duration {
			t.Errorf("%s: duration = %v, want %v", tt.name, duration, tt.duration)
		}
		if summary.Packets != int(tt.packets) {
			t.Errorf("%s: expected %d packets, got %d", tt.name, tt.packets, summary.Packets)
		}

		if !summary.SrcIP.Equal(net.ParseIP(tt.srcIP)) {
			t.Errorf("%s: srcIP = %s, want %s", tt.name, summary.SrcIP, tt.srcIP)
		}
		if summary.SrcPort != tt.srcPort {
			t.Errorf("%s: srcPort = %d, want %d", tt.name, summary.SrcPort, tt.srcPort)
		}
		if summary.DstPort != tt.dstPort {
			t.Errorf("%s: dstPort = %d, want %d", tt.name, summary.DstPort, tt.dstPort)
		}
		// Now check against gopacket values, too.
		if tcp, err := summary.GetFirstTCP(); err != nil {
			t.Error(err)
		} else {
			if summary.SrcPort != tcp.SrcPort {
				t.Errorf("%s: srcPort = %0.2x, want %0.2x", tt.name, summary.SrcPort, tcp.SrcPort)
			}
			if summary.DstPort != tcp.DstPort {
				t.Errorf("%s: dstPort = %d(%.2x), want %.2x", tt.name, summary.DstPort, summary.DstPort, tcp.DstPort)
			}
		}

	}
}

func ProcessShortPackets(t *testing.T, data []byte) {
	pcap, err := pcapgo.NewReader(bytes.NewReader(data))
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

	for data, ci, err := pcap.ReadPacketData(); err == nil; data, ci, err = pcap.ZeroCopyReadPacketData() {
		for i := 0; i < len(data); i++ {
			tcpip.Wrap(&ci, data[:i])
			tcpip.Wrap(&ci, data[i:])
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
	data := []byte{0xd4, 0xc3, 0xb2, 0xa1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	_, err := tcpip.ProcessPackets("none", "garbage", data)
	if err != io.ErrUnexpectedEOF {
		t.Fatal(err)
	}

	data = append(data, data...)
	_, err = tcpip.ProcessPackets("none", "garbage", data)
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
// Wrap, total, 2x2 files    BenchmarkGetPackets-8   	    1730	    590224 ns/op	 1078212 B/op	    8157 allocs/op
// Wrap, total, 2x3 files    BenchmarkGetPackets-8   	    2619	    395314 ns/op	  735944 B/op	    5452 allocs/op

// Wrap, total 1x6 files *6: BenchmarkGetPackets-8   	     129	   8372769 ns/op	19967908 B/op	   97448 allocs/op
// Wrap, total 1x6 files *7: BenchmarkGetPackets-8   	     129	   8372769 ns/op	19967908 B/op	   97448 allocs/op

// Correct ipv6 decoding:    BenchmarkGetPackets-8   	     100	  11350868 ns/op	19519358 B/op	  120908 allocs/op
// Use pointer fgor CI:      BenchmarkGetPackets-8   	     100	  10318408 ns/op	12376754 B/op	   96639 allocs/op

// This one makes a single copy of CaptureInfo, because pointer referent gets cleared:
// Don't wrap twice!!          BenchmarkGetPackets:          145	   7881966 ns/op	16814909 B/op	   98956 allocs/op
// Fix TCP header/decode opt:  BenchmarkGetPackets-8   	     136	   8110814 ns/op	16549726 B/op	   97431 allocs/op
//                             BenchmarkGetPackets-8   	     100	  13316191 ns/op	16989482 B/op	  132680 allocs/op
// Summary only:               BenchmarkGetPackets-8   	     223	   5492522 ns/op	 3666003 B/op	   99160 allocs/op
func BenchmarkGetPackets(b *testing.B) {
	type src struct {
		data    []byte
		numPkts int
		total   int
	}
	sources := []src{
		// Approximately 220K packets, so this is about 140nsec/packet, and about 100 bytes/packet allocated,
		// which is roughly the footprint of the packets themselves.
		{getTestfileForBenchmark(b, "ndt-nnwk2_1611335823_00000000000C2DFE.pcap.gz"), 336, 166963},
		{getTestfileForBenchmark(b, "ndt-nnwk2_1611335823_00000000000C2DA8.pcap.gz"), 15, 4534},
		{getTestfileForBenchmark(b, "ndt-nnwk2_1611335823_00000000000C2DA9.pcap.gz"), 5180, 81408254},
		{getTestfileForBenchmark(b, "ndt-m6znc_1632401351_000000000005BA77.pcap.gz"), 40797, 239251582},
		{getTestfileForBenchmark(b, "ndt-m6znc_1632401351_000000000005B9EA.pcap.gz"), 146172, 158095963},
		{getTestfileForBenchmark(b, "ndt-m6znc_1632401351_000000000005B90B.pcap.gz"), 30097, 126523361},
	}
	b.ResetTimer()

	i := 0
	pktCount := 0
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			test := sources[i%len(sources)]
			i++
			summary, err := tcpip.ProcessPackets("foo", "bar", test.data)
			if err != nil {
				b.Fatal(err)
			}

			if int(summary.PayloadBytes) != test.total {
				b.Fatalf("total = %d, want %d", summary.PayloadBytes, test.total)
			}
			if summary.Packets != test.numPkts {
				b.Errorf("expected %d packets, got %d", test.numPkts, summary.Packets)
			}
			pktCount += summary.Packets
		}
	})
}
