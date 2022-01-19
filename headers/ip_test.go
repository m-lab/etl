package headers_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcapgo"

	nano "github.com/m-lab/etl/internal/nano"

	"github.com/m-lab/etl/headers"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

//lint:ignore U1000 unused
func assertV6isIP(ip *headers.IPv6Header) {
	func(headers.IP) {}(ip)
}

//lint:ignore U1000 unused
func assertV4isIP(ip *headers.IPv4Header) {
	func(headers.IP) {}(ip)
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

func ProcessPackets(data []byte) (int, error) {
	pcap, err := pcapgo.NewReader(bytes.NewReader(data))
	if err != nil {
		return 0, err
	}

	p := headers.Packet{}
	count := 0
	for data, ci, err := pcap.ZeroCopyReadPacketData(); err == nil; data, ci, err = pcap.ZeroCopyReadPacketData() {
		err := p.Overlay(nano.UnixNano(ci.Timestamp.UnixNano()), data)
		if err != nil {
			return count, err
		}
		count++
	}

	return count, nil
}

func ProcessShortPackets(t *testing.T, data []byte) {
	pcap, err := pcapgo.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	p := headers.Packet{}
	for data, ci, err := pcap.ReadPacketData(); err == nil; data, ci, err = pcap.ZeroCopyReadPacketData() {
		for i := 0; i < len(data); i++ {
			p.Overlay(nano.UnixNano(ci.Timestamp.UnixNano()), data[:i])
			p.Overlay(nano.UnixNano(ci.Timestamp.UnixNano()), data[i:])
		}
	}
}

func TestShortData(t *testing.T) {
	type test struct {
		name             string
		fn               string
		packets          int64
		duration         time.Duration
		srcIP            string
		srcPort, dstPort layers.TCPPort
		TTL              uint8
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
	_, err := ProcessPackets(data)
	if err != io.ErrUnexpectedEOF {
		t.Fatal(err)
	}

	data = append(data, data...)
	_, err = ProcessPackets(data)
	if err == nil || !strings.Contains(err.Error(), "Unknown major") {
		t.Fatal(err)
	}
}

// goos: darwin goarch: amd64 cpu: Intel(R) Core(TM) i7-7920HQ CPU @ 3.10GHz
// BenchmarkProcessPackets2-8   	     316	   3763748 ns/op	 469.07 MB/s	     36776 packets/op	   60491 B/op	     207 allocs/op
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
			count, err := ProcessPackets(test.data)
			if err != nil {
				b.Fatal(err)
			}
			if count != test.numPkts {
				b.Errorf("expected %d packets, got %d", test.numPkts, count)
			}
			b.SetBytes(int64(len(test.data)))
		}
	})
	b.Log("total packets", numPkts, "total ops", ops)
	b.ReportMetric(float64(numPkts/ops), "packets/op")
}
