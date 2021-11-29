package tcp_test

import (
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/google/gopacket/layers"
	"github.com/m-lab/etl/tcp"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestTracker_Seq(t *testing.T) {
	stats := &tcp.StatsWrapper{}

	tr := tcp.NewTracker()

	now := time.Now()
	tr.Seq(0, now, 1234, 0, true, stats) // SYN, no data
	tr.Seq(0, now, 1235, 20, false, stats)
	tr.Seq(0, now, 1255, 10, false, stats)
	if tr.SendNext() != 1265 {
		t.Errorf("SendNext() = %v, want %v", tr.SendNext(), 1265)
	}

	// Retransmit
	if _, b := tr.Seq(0, now, 1240, 12, false, stats); !b {
		t.Errorf("Seq() = %v, want %v", b, true)
	}
	// SendNext should be unchanged.
	if tr.SendNext() != 1265 {
		t.Errorf("SendNext() = %v, want %v", tr.SendNext(), 1265)
	}

	if _, b := tr.Seq(0, now, tr.SendNext(), 10, false, stats); b {
		t.Errorf("Seq() = %v, want %v", b, false)
	}
	if tr.SendNext() != 1275 {
		t.Errorf("SendNext() = %v, want %v", tr.SendNext(), 1275)
	}
	if stats.RetransmitBytes != 12 {
		t.Errorf("RetransmitBytes = %v, want %v", stats.RetransmitBytes, 12)
	}
	// TODO - the parser should likely detect that the Syn/Ack is late.
	tr.Ack(0, now, 1234, false, stats)
	if tr.Acked() != 0 {
		t.Errorf("Acked() = %v, want %v", tr.Acked(), 0)
	}
	tr.Ack(0, now, 1244, false, stats)
	if tr.Acked() != 10 {
		t.Errorf("Acked() = %v, want %v", tr.Acked(), 10)
	}

	tr.Seq(0, now, 5<<28, 0, false, stats)
	if tr.SendNext() != 1275 {
		t.Errorf("SendNext() = %v, want %v", tr.SendNext(), 1275)
	}
	if stats.BadDeltas != 1 {
		t.Errorf("Stats().BadDeltas = %v, want %v", stats.BadDeltas, 1)
	}

	// Seq that doesn't match previous data length.
	tr.Seq(0, now, 1300, 0, false, stats)
	// Seq should advance, but we should also observe an error.
	if tr.SendNext() != 1300 {
		t.Errorf("SendNext() = %v, want %v", tr.SendNext(), 1300)
	}
	if stats.MissingPackets != 1 {
		t.Errorf("Stats() = %v, want %v", stats.MissingPackets, 1)
	}

}

// TestParse exercises a lot of code, on the ipv4 path.  It only checks
// some basic stats, which are assumed to be correct, but not inspected.
func TestParse(t *testing.T) {
	type test struct {
		name                              string
		fn                                string
		packets                           int64
		leftRetransmits, rightRetransmits int64
		truncated                         int64
		exceeded                          int64
		nopCount                          int64
	}
	tests := []test{
		{name: "retransmits", fn: "testfiles/ndt-nnwk2_1611335823_00000000000C2DFE.pcap",
			packets: 336, leftRetransmits: 11, rightRetransmits: 8, truncated: 0, exceeded: 0, nopCount: 1157},
		{name: "ipv6", fn: "testfiles/ndt-nnwk2_1611335823_00000000000C2DA8.pcap.gz",
			packets: 15, leftRetransmits: 0, rightRetransmits: 0, truncated: 0, exceeded: 0, nopCount: 47},
		{name: "protocolErrors2", fn: "testfiles/ndt-nnwk2_1611335823_00000000000C2DA9.pcap.gz",
			packets: 5180, leftRetransmits: 0, rightRetransmits: 0, truncated: 0, exceeded: 2880, nopCount: 17087},
		{name: "foobar", fn: "testfiles/ndt-xkrzj_1632230485_0000000000AE8EE2.pcap.gz",
			packets: 49, leftRetransmits: 0, rightRetransmits: 0, truncated: 0, exceeded: 0, nopCount: 1510},
	}
	for _, tt := range tests {
		f, err := os.Open(tt.fn)
		if err != nil {
			t.Fatal(err)
		}
		data, err := ioutil.ReadAll(f)
		if err != nil {
			t.Fatal(err)
		}
		tcp := tcp.NewParser()
		summary, err := tcp.Parse(data)
		if err != nil {
			t.Fatal(err)
		}
		if summary.Packets != tt.packets {
			t.Errorf("test:%s: Packets = %v, want %v", tt.name, summary.Packets, tt.packets)
		}
		// TODO - replace these with LeftStats and RightStats.
		if summary.LeftStats.RetransmitPackets != tt.leftRetransmits {
			t.Errorf("test:%s: Left.Retransmits = %v, want %v", tt.name, summary.LeftStats.RetransmitPackets, tt.leftRetransmits)
		}
		if summary.RightStats.RetransmitPackets != tt.rightRetransmits {
			t.Errorf("test:%s: Right.Retransmits = %v, want %v", tt.name, summary.RightStats.RetransmitPackets, tt.rightRetransmits)
		}
		if summary.TruncatedPackets != tt.truncated {
			t.Errorf("test:%s: TruncatedPackets = %v, want %v", tt.name, summary.TruncatedPackets, tt.truncated)
		}
		if summary.RightStats.SendNextExceededLimit != tt.exceeded {
			t.Errorf("test:%s: SendNextExceededLimit = %v, want %v", tt.name, summary.RightStats.SendNextExceededLimit, tt.exceeded)
		}
		if summary.LeftStats.OptionCounts[layers.TCPOptionKindNop] != tt.nopCount {
			t.Errorf("test:%s: OptionCounts = %v, want %v", tt.name, summary.LeftStats.OptionCounts, tt.nopCount)
		}
	}
}

func TestJitter(t *testing.T) {
	j := tcp.JitterTracker{}
	rand.Seed(12345)
	t0 := rand.Uint32()
	for p := time.Date(2016, time.November, 10, 1, 1, 1, 1, time.UTC); p.Before(time.Date(2016, time.November, 10, 1, 1, 2, 2, time.UTC)); p = p.Add(10 * time.Millisecond) {
		//t := -5e10 + time.Duration(rand.Int63n(1e7)-5e6)
		t := t0 + uint32(rand.Intn(7)) - 3
		t0 += 10
		j.Add(t, p)
		//j.AddEcho(-5e10 - 10*time.Millisecond)
	}
	if j.Jitter() > .005 || j.Jitter() < .001 {
		t.Error(j.Jitter(), j.Delay(), j.ValCount)
	}
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func getTestfileForBenchmark(b *testing.B, name string) []byte {
	f, err := os.Open(path.Join(`testfiles/`, name))
	if err != nil {
		b.Fatal(err)
	}
	data, err := ioutil.ReadAll(f)
	if err != nil {
		b.Fatal(err)
	}
	return data
}

// cpu: Intel(R) Core(TM) i7-7920HQ CPU @ 3.10GHz,  BenchmarkGetPackets-8
// Old gfr-rtt-jitter using gopacket layers:     100	  41001560 ns/op	56535609 B/op	  620864 allocs/op
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
	pktCount := int64(0)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			test := sources[i%len(sources)]
			i++
			tcp := tcp.NewParser()
			alpha, err := tcp.Parse(test.data)
			if err != nil {
				b.Fatal(err)
			}

			pktCount += alpha.Packets
		}
	})
}
