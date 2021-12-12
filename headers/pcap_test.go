package headers_test

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"testing"
	"time"

	"github.com/google/gopacket/layers"

	"github.com/m-lab/annotation-service/site"
	"github.com/m-lab/etl/headers"
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

type Errorer interface {
	Error(...interface{})
	Errorf(string, ...interface{})
	Fatal(...interface{})
	Fatalf(string, ...interface{})
	Log(...interface{})
}

func parse(t Errorer, data []byte) int {
	pr, err := headers.NewPCAPReader(data)
	if err != nil {
		t.Fatal(err)
	}

	pkt := headers.Packet{}
	packets := 0
	for err = pr.Next(&pkt); true; err = pr.Next(&pkt) {
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Error(err)
			break
		}
		if packets == 0 {
			// TODO - handle the timestamp
		}
		packets++
	}

	return packets
}

func TestPCAPReader(t *testing.T) {
	site.MustLoad(time.Minute)

	type test struct {
		name             string
		fn               string
		packets          int
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
	for i := range tests {
		test := &tests[i]
		data := getTestfile(t, test.fn)

		packets := parse(t, data)

		if packets != test.packets {
			t.Errorf("%s: expected %d packets, got %d", test.fn, test.packets, packets)
		}
	}
}

// func TestPCAPGarbage(t *testing.T) {
// 	data := []byte{0xd4, 0xc3, 0xb2, 0xa1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
// 	_, err := tcpip.ProcessPackets("none", "garbage", data)
// 	if err != io.ErrUnexpectedEOF {
// 		t.Fatal(err)
// 	}

// 	data = append(data, data...)
// 	_, err = tcpip.ProcessPackets("none", "garbage", data)
// 	if err == nil || !strings.Contains(err.Error(), "Unknown major") {
// 		t.Fatal(err)
// 	}
// }

// goos: darwin goarch: amd64 pkg: github.com/m-lab/etl/tcpip cpu: Intel(R) Core(TM) i7-7920HQ CPU @ 3.10GHz
// BenchmarkPCAPHeaders-8   	     352	   3513927 ns/op	 502.42 MB/s	     36809 packets/op	   56441 B/op	     204 allocs/op
// BenchmarkPCAPHeaders-8   	     378	   3075351 ns/op	 574.07 MB/s	     37099 packets/op	   56329 B/op	     203 allocs/op
func BenchmarkPCAPHeaders(b *testing.B) {
	type tt struct {
		data                  []byte
		fn                    string
		numPkts               int
		ipPayloadBytes        int
		leftSacks, rightSacks int
	}
	tests := []tt{
		// Approximately 220K packets, so this is about 140nsec/packet, and about 100 bytes/packet allocated,
		// which is roughly the footprint of the packets themselves.
		{nil, "ndt-nnwk2_1611335823_00000000000C2DA8.pcap.gz", 15, 4574, 0, 0},
		{nil, "ndt-nnwk2_1611335823_00000000000C2DFE.pcap.gz", 336, 167003, 31, 24}, // retransmits and SACKs
		{nil, "ndt-nnwk2_1611335823_00000000000C2DA9.pcap.gz", 5180, 81408294, 0, 0},
		{nil, "ndt-m6znc_1632401351_000000000005BA77.pcap.gz", 40797, 239251626, 70557, 207},
		{nil, "ndt-m6znc_1632401351_000000000005B9EA.pcap.gz", 146172, 158096007, 7, 195},
		{nil, "ndt-m6znc_1632401351_000000000005B90B.pcap.gz", 30097, 126523401, 0, 0},
	}
	for i := range tests {
		tests[i].data = getTestfileForBenchmark(b, tests[i].fn)
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

			packets := parse(b, test.data)

			if packets != test.numPkts {
				b.Errorf("%s: expected %d packets, got %d", test.fn, test.numPkts, packets)
			}
			b.SetBytes(int64(len(test.data)))
		}
	})
	b.Log("total packets", numPkts, "total ops", ops)
	b.ReportMetric(float64(numPkts/ops), "packets/op")
}
