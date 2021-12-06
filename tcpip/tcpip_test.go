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

	"github.com/google/gopacket"
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

// GetGopacketFirstTCP uses gopacket to decode the  TCP layer for the first packet.
// It is a bit slow and does memory allocation.
func GetGopacketFirstTCP(s *tcpip.Summary) (*layers.TCP, error) {
	// Decode a packet.
	pkt := gopacket.NewPacket(s.FirstPacket, layers.LayerTypeEthernet, gopacket.DecodeOptions{
		Lazy:                     true,
		NoCopy:                   true,
		SkipDecodeRecovery:       true,
		DecodeStreamsAsDatagrams: false,
	})

	if tcpLayer := pkt.Layer(layers.LayerTypeTCP); tcpLayer != nil {
		tcp, _ := tcpLayer.(*layers.TCP)
		// For IPv4, the TTL length is the ip.Length adjusted for the header length.
		return tcp, nil
	} else {
		return nil, tcpip.ErrNoTCPLayer
	}
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
		if tcp, err := GetGopacketFirstTCP(&summary); err != nil {
			t.Error(err)
		} else {
			if summary.SrcPort != tcp.SrcPort {
				t.Errorf("%s: srcPort = %d, want %d", tt.name, summary.SrcPort, tcp.SrcPort)
			}
			if summary.DstPort != tcp.DstPort {
				t.Errorf("%s: dstPort = %d, want %d", tt.name, summary.DstPort, tcp.DstPort)
			}
		}

		// if summary.Packets != tt.packets {
		// 	t.Errorf("test:%s: Packets = %v, want %v", tt.name, summary.Packets, tt.packets)
		// }
		// // TODO - replace these with LeftStats and RightStats.
		// if summary.LeftStats.RetransmitPackets != tt.leftRetransmits {
		// 	t.Errorf("test:%s: Left.Retransmits = %v, want %v", tt.name, summary.LeftStats.RetransmitPackets, tt.leftRetransmits)
		// }
		// if summary.RightStats.RetransmitPackets != tt.rightRetransmits {
		// 	t.Errorf("test:%s: Right.Retransmits = %v, want %v", tt.name, summary.RightStats.RetransmitPackets, tt.rightRetransmits)
		// }
		// if summary.TruncatedPackets != tt.truncated {
		// 	t.Errorf("test:%s: TruncatedPackets = %v, want %v", tt.name, summary.TruncatedPackets, tt.truncated)
		// }
		// if summary.RightStats.SendNextExceededLimit != tt.exceeded {
		// 	t.Errorf("test:%s: SendNextExceededLimit = %v, want %v", tt.name, summary.RightStats.SendNextExceededLimit, tt.exceeded)
		// }
		// if summary.LeftStats.OptionCounts[layers.TCPOptionKindNop] != tt.nopCount {
		// 	t.Errorf("test:%s: OptionCounts = %v, want %v", tt.name, summary.LeftStats.OptionCounts, tt.nopCount)
		// }
		t.Logf("%+v\n", summary)
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

	p := tcpip.Packet{}
	for data, ci, err := pcap.ReadPacketData(); err == nil; data, ci, err = pcap.ZeroCopyReadPacketData() {
		for i := 0; i < len(data); i++ {
			p.From(&ci, data[:i])
			p.From(&ci, data[i:])
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
// Don't wrap twice!!        BenchmarkGetPackets:            145	   7881966 ns/op	16814909 B/op	   98956 allocs/op
//
// Costly option decoding:   BenchmarkGetPackets:			 100	  18097318 ns/op	20975383 B/op	  658780 allocs/op
//                                                           100	  18369065 ns/op	25097387 B/op	  843705 allocs/op

// Explicit byte reversal:   BenchmarkGetPackets-8:          100	  13365349 ns/op	17741200 B/op	  453337 allocs/op
//     														 100	  12855457 ns/op	17030489 B/op	  423784 allocs/op
// remove log escapes, and handle options differently:
// 							 BenchmarkGetPackets-8   	     100	  10343073 ns/op	11277722 B/op	  241635 allocs/op
func BenchmarkGetPackets(b *testing.B) {
	type src struct {
		data    []byte
		numPkts int
		total   int
	}
	sources := []src{
		// Approximately 220K packets, so this is about 140nsec/packet, and about 100 bytes/packet allocated,
		// which is roughly the footprint of the packets themselves.
		{getTestfileForBenchmark(b, "ndt-nnwk2_1611335823_00000000000C2DFE.pcap.gz"), 336, 167003},
		{getTestfileForBenchmark(b, "ndt-nnwk2_1611335823_00000000000C2DA8.pcap.gz"), 15, 4574},
		{getTestfileForBenchmark(b, "ndt-nnwk2_1611335823_00000000000C2DA9.pcap.gz"), 5180, 81408294},
		{getTestfileForBenchmark(b, "ndt-m6znc_1632401351_000000000005BA77.pcap.gz"), 40797, 239251626},
		{getTestfileForBenchmark(b, "ndt-m6znc_1632401351_000000000005B9EA.pcap.gz"), 146172, 158096007},
		{getTestfileForBenchmark(b, "ndt-m6znc_1632401351_000000000005B90B.pcap.gz"), 30097, 126523401},
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
				b.Errorf("total = %d, want %d", summary.PayloadBytes, test.total)
			}
			if summary.Packets != test.numPkts {
				b.Errorf("expected %d packets, got %d", test.numPkts, summary.Packets)
			}
			pktCount += summary.Packets
		}
	})
}

// cpu: Intel(R) Core(TM) i7-7920HQ CPU @ 3.10GHz
// Before packet count opt:    128	   8052268 ns/op	 219.25 MB/s	     36522 packets/op	28021501 B/op	   36747 allocs/op
// After packet count opt:     234	   5896273 ns/op	 299.42 MB/s	     37099 packets/op	11927524 B/op	   37314 allocs/op
//							   235	   5228191 ns/op	 337.68 MB/s	     37436 packets/op	12051418 B/op	   37652 allocs/op
//							   236	   5022948 ns/op	 351.48 MB/s	     36786 packets/op	11827143 B/op	   37000 allocs/op
//   ...                       159	   9528634 ns/op	 185.28 MB/s	     72868 packets/op	 9735622 B/op	  174743 allocs/op
// Approximately 700 bytes/packet on average.
// 							   100	  17760632 ns/op	  99.40 MB/s	     36078 packets/op	20975442 B/op	  658780 allocs/op
//						       100	  17696214 ns/op	  99.77 MB/s	     72157 packets/op	20974294 B/op	  658772 allocs/op
// Early top level only:     235	   5228191 ns/op	 337.68 MB/s	     37436 packets/op	12051418 B/op	   37652 allocs/op
//    Approximately 300 bytes/packet on average.
// Full jitter decoding      100	  18095458 ns/op	  97.56 MB/s	     36078 packets/op	20974779 B/op	  658774 allocs/op
//     (rebasing)			 100	  22061225 ns/op	  80.03 MB/s	     72157 packets/op	20976420 B/op	  658777 allocs/op
// Many optimizations 	     100	  10629111 ns/op	 166.10 MB/s	     36078 packets/op	11279236 B/op	  241639 allocs/op
// Better jitter compute     100	  11072329 ns/op	 159.45 MB/s	     36078 packets/op	10408367 B/op	  169478 allocs/op
//                           100	  11393172 ns/op	 154.96 MB/s	     36078 packets/op	 9543012 B/op	  169480 allocs/op
// Packet.From				 100	  11223040 ns/op	 157.31 MB/s	     36078 packets/op	 8676454 B/op	  133400 allocs/op - 240 bytes/packet
//							 100	  10483849 ns/op	 168.40 MB/s	     36078 packets/op	 4733919 B/op	   97330 allocs/op
//  opts in place			 127	   9179728 ns/op	 192.32 MB/s	     36807 packets/op	 4864737 B/op	   99568 allocs/op
//  timestamps -> UnixNano   150	   8352165 ns/op	 211.38 MB/s	     37099 packets/op	 3779400 B/op	  100432 allocs/op
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
			if int(summary.PayloadBytes) != test.ipPayloadBytes {
				b.Fatalf("total = %d, want %d", summary.PayloadBytes, test.ipPayloadBytes)
			}
			if summary.LeftState.Stats.Sacks != int64(test.leftSacks) ||
				summary.RightState.Stats.Sacks != int64(test.rightSacks) {
				b.Log(test.numPkts, "packets -> SACKs:", summary.LeftState.Stats.Sacks, summary.RightState.Stats.Sacks)
			}
			b.SetBytes(int64(len(test.data)))
		}
	})
	b.Log("total packets", numPkts, "total ops", ops)
	b.ReportMetric(float64(numPkts/ops), "packets/op")
}
