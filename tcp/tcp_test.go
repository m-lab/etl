package tcp_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcapgo"
	"github.com/m-lab/etl/tcp"
	"github.com/m-lab/etl/tcpip"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestJitter(t *testing.T) {
	j := tcp.JitterTracker{}
	rand.Seed(12345)
	t0 := rand.Uint32()
	for p := time.Date(2016, time.November, 10, 1, 1, 1, 1, time.UTC); p.Before(time.Date(2016, time.November, 10, 1, 1, 2, 2, time.UTC)); p = p.Add(10 * time.Millisecond) {
		//t := -5e10 + time.Duration(rand.Int63n(1e7)-5e6)
		t := t0 + uint32(rand.Intn(7)) - 3
		t0 += 10
		j.Add(t, tcp.UnixNano(p.UnixNano()))
		//j.AddEcho(-5e10 - 10*time.Millisecond)
	}
	if j.Jitter() > .005 || j.Jitter() < .001 {
		t.Error(j.Jitter(), j.Delay(), j.ValCount)
	}
}

func ProcessPackets(data []byte) (tcpip.Summary, error) {
	summary := tcpip.Summary{
		OptionCounts: make(map[layers.TCPOptionKind]int),
		Errors:       make(map[int]error, 1),
	}

	pcap, err := pcapgo.NewReader(bytes.NewReader(data))
	if err != nil {
		log.Print(err)
		return summary, err
	}

	summary.Details = make([]string, 0, 1000)

	p := tcpip.Packet{}
	for data, ci, err := pcap.ReadPacketData(); err == nil; data, ci, err = pcap.ReadPacketData() {
		// Pass ci by pointer, but Wrap will make a copy, since gopacket NoCopy doesn't preserve the values.
		err := p.From(&ci, data)
		if err != nil {
			summary.Errors[summary.Packets] = err
			continue
		}
		summary.Add(&p)
	}
	return summary, nil
}

// TestSummary exercises a lot of code.  It only checks
// some basic stats, which are assumed to be correct, but not inspected.
// TODO - we should add analysis to identify the problems with ndt-m6znc_1632401351_000000000005B9EA.pcap.gz
func TestSummary(t *testing.T) {
	type test struct {
		name                              string
		fn                                string
		packets                           int
		leftRetransmits, rightRetransmits int64
		leftSacks, rightSacks             int64
		exceeded                          int64
		leftTimestamps, rightTimestamps   int64
	}
	tests := []test{
		// Some of these have mysteriously changed, so we should determine why.  Perhaps related to retransmits?
		// tcp_test.go:174: test:retransmits: Right.Sacks = 55, want 86
		// tcp_test.go:177: test:retransmits: Left.Sacks = 55, want 79  - seems odd that they are the same now.
		// tcp_test.go:187: test:retransmits: Timestamps = 336, want 510  - this makes sense - all packets have TS.
		// tcp_test.go:187: test:ipv6: Timestamps = 15, want 22
		// tcp_test.go:187: test:protocolErrors2: Timestamps = 5178, want 8542
		// tcp_test.go:187: test:foobar: Timestamps = 49, want 77
		{name: "retransmits", fn: "testfiles/ndt-nnwk2_1611335823_00000000000C2DFE.pcap", packets: 336,
			leftRetransmits: 11, rightRetransmits: 8, exceeded: 0, leftSacks: 24, rightSacks: 31, leftTimestamps: 162, rightTimestamps: 174},
		{name: "ipv6", fn: "testfiles/ndt-nnwk2_1611335823_00000000000C2DA8.pcap.gz", packets: 15,
			leftRetransmits: 0, rightRetransmits: 0, exceeded: 0, leftSacks: 0, rightSacks: 0, leftTimestamps: 8, rightTimestamps: 7},
		{name: "NOT-protocolErrors2", fn: "testfiles/ndt-nnwk2_1611335823_00000000000C2DA9.pcap.gz", packets: 5180,
			leftRetransmits: 0, rightRetransmits: 0, exceeded: 0, leftSacks: 0, rightSacks: 0, leftTimestamps: 1814, rightTimestamps: 3364},
		{name: "foobar", fn: "testfiles/ndt-xkrzj_1632230485_0000000000AE8EE2.pcap.gz", packets: 49,
			leftRetransmits: 0, rightRetransmits: 0, exceeded: 0, leftSacks: 0, rightSacks: 0, leftTimestamps: 21, rightTimestamps: 28},
		{name: "big", fn: "testfiles/ndt-4dh2l_1591894023_00000000003638D0.pcap.gz", packets: 210322,
			leftRetransmits: 29, rightRetransmits: 0, exceeded: 0, leftSacks: 478, rightSacks: 0, leftTimestamps: 140938, rightTimestamps: 69384},

		// This contains an ACK that is observed about 200 usec before the corresponding packet is observed.
		// 367	1.017318000	2001:668:1f:1c::203	2600:1700:42d0:67b0:71e7:d89:1d89:9484	TCP	86	443	[TCP ACKed unseen segment] 443 → 49319 [ACK] Seq=13116 Ack=262516 Win=327296 Len=0 TSval=3783599016 TSecr=1746186507
		{name: "more retrans", fn: "testfiles/ndt-m6znc_1632401351_000000000005B9EA.pcap.gz", packets: 146172,
			leftRetransmits: 175, rightRetransmits: 238, exceeded: 0, leftSacks: 195, rightSacks: 7, leftTimestamps: 96459, rightTimestamps: 49477},
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
		summary, err := ProcessPackets(data)
		if err != nil {
			t.Fatal(err)
		}
		rs := summary.RightState
		ls := summary.LeftState

		if summary.LeftState.Stats.SrcPortErrors > 0 || summary.LeftState.Stats.DstPortErrors > 0 {
			t.Fatal("Mismatching ports", summary.SrcPort, summary.LeftState.Stats.SrcPortErrors,
				summary.DstPort, summary.LeftState.Stats.DstPortErrors)
		}
		if summary.RightState.Stats.SrcPortErrors > 0 || summary.LeftState.Stats.DstPortErrors > 0 {
			t.Fatal("Mismatching ports", summary.SrcPort, summary.RightState.Stats.SrcPortErrors,
				summary.DstPort, summary.RightState.Stats.DstPortErrors)
		}
		if err != nil {
			t.Fatal(err)
		}
		if summary.Packets != tt.packets {
			t.Errorf("test:%s: Packets = %v, want %v", tt.name, summary.Packets, tt.packets)
		}
		if summary.LeftState.Stats.RetransmitPackets != tt.leftRetransmits {
			t.Errorf("test:%s: Left.Retransmits = %v, want %v", tt.name, summary.LeftState.Stats.RetransmitPackets, tt.leftRetransmits)
		}
		if summary.RightState.Stats.RetransmitPackets != tt.rightRetransmits {
			t.Errorf("test:%s: Right.Retransmits = %v, want %v", tt.name, summary.RightState.Stats.RetransmitPackets, tt.rightRetransmits)
		}
		if summary.RightState.Stats.Sacks != tt.rightSacks {
			t.Errorf("test:%s: Right.Sacks = %v, want %v", tt.name, summary.RightState.Stats.Sacks, tt.rightSacks)
		}
		if summary.LeftState.Stats.Sacks != tt.leftSacks {
			t.Errorf("test:%s: Left.Sacks = %v, want %v", tt.name, summary.LeftState.Stats.Sacks, tt.leftSacks)
		}

		// if summary.TruncatedPackets != tt.truncated {
		// 	t.Errorf("test:%s: TruncatedPackets = %v, want %v", tt.name, summary.TruncatedPackets, tt.truncated)
		// }
		if summary.RightState.Stats.SendNextExceededLimit != tt.exceeded {
			t.Errorf("test:%s: SendNextExceededLimit = %v, want %v", tt.name, summary.RightState.Stats.SendNextExceededLimit, tt.exceeded)
		}
		if summary.LeftState.Stats.OptionCounts[layers.TCPOptionKindTimestamps] != tt.leftTimestamps {
			t.Errorf("test:%s: Left Timestamps = %v, want %v", tt.name,
				summary.LeftState.Stats.OptionCounts[layers.TCPOptionKindTimestamps], tt.leftTimestamps)
		}
		if summary.RightState.Stats.OptionCounts[layers.TCPOptionKindTimestamps] != tt.rightTimestamps {
			t.Errorf("test:%s: Right Timestamps = %v, want %v", tt.name,
				summary.RightState.Stats.OptionCounts[layers.TCPOptionKindTimestamps], tt.rightTimestamps)
		}
		t.Log(ls.Jitter.ValLR(), rs.Jitter.ValLR())
		t.Log(ls.Jitter.EchoLR(), rs.Jitter.EchoLR())
		// t.Errorf("Right: jitter %6.4f(%6.4f)    delay %10v(%9.4f)\n        "+
		// 	"     Left:  jitter %6.4f(%6.4f)    delay %10v(%9.4f) ",
		// 	rs.Jitter.LRJitter(), rs.Jitter.Jitter(), rs.Jitter.LRDelay0(), rs.Jitter.Delay(),
		// 	ls.Jitter.LRJitter(), ls.Jitter.Jitter(), ls.Jitter.LRDelay0(), ls.Jitter.Delay())
	}
}

func BenchmarkTCPOptions2(b *testing.B) {
	port := layers.TCPPort(80)
	s := tcp.NewState(net.IP{}, port)
	pTime := tcp.UnixNano(time.Now().UnixNano())
	s.SeqTracker.Seq(0, pTime, 123, 0, true, &s.Stats)
	s.SeqTracker.Seq(1, pTime, 124, 1000, false, &s.Stats)
	s.SeqTracker.Seq(2, pTime, 1124, 2000, true, &s.Stats)
	fakeOptions := []byte{
		layers.TCPOptionKindMSS, 4, 0, 0,
		layers.TCPOptionKindTimestamps, 10, 0, 1, 2, 3, 4, 5, 6, 7,
		layers.TCPOptionKindSACK, 18, 0, 0, 1, 1, 0, 0, 1, 2, 0, 0, 2, 3, 0, 0, 2, 4,
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Explicit byte reversal improves this from 480 nsec (6 allocs, 120 bytes) to 56 nsec (no allocs) per op.
		s.Options2(port, false, pTime, fakeOptions)
	}
}

func BenchmarkTCPHeaderGo_From(b *testing.B) {
	// These byte values are taken from a WireShark decoded packet.
	hex := "9d 91 01 bb 31 f4 e2 0c 46 f4 b1 ba 80 10 02 a4 29 e1 00 00 01 01 08 0a 0b 62 9d 29 2b b5 a7 0e"
	hexArray := strings.Split(hex, " ")
	data := make([]byte, len(hexArray))
	for i, v := range hexArray {
		b, _ := strconv.ParseInt(v, 16, 16)
		data[i] = byte(b)
	}

	var out tcp.TCPHeaderGo

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out.From(data)
	}
}

// cpu: Intel(R) Core(TM) i7-7920HQ CPU @ 3.10GHz
// BenchmarkTCPSummary-8   	       7	 161298078 ns/op	22228477 B/op	  653324 allocs/op
func BenchmarkTCPSummary(b *testing.B) {
	pprof.StopCPUProfile()
	type test struct {
		name    string
		fn      string
		packets int
		data    []byte
	}
	tests := []test{
		{name: "protocolErrors2", fn: "testfiles/ndt-nnwk2_1611335823_00000000000C2DA9.pcap.gz", packets: 5180},
		{name: "big", fn: "testfiles/ndt-4dh2l_1591894023_00000000003638D0.pcap.gz", packets: 210322},
	}
	for i := range tests {
		var err error
		data, err := ioutil.ReadFile(tests[i].fn)
		if err != nil {
			b.Fatal(err)
		}
		r, err := gzip.NewReader(bytes.NewReader(data))
		if err == nil {
			tests[i].data, err = ioutil.ReadAll(r)
			if err != nil {
				tests[i].data = data
			}
		} else {
			tests[i].data = data
		}
	}

	f, err := os.OpenFile("../profile.cpu", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		b.Fatal(err)
	}

	pprof.StartCPUProfile(io.Writer(f))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, tt := range tests {
			_, err := ProcessPackets(tt.data)
			if err != nil {
				b.Fatal(err, len(tt.data))
			}
		}
	}
	pprof.StopCPUProfile()
}
