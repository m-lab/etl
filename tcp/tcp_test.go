package tcp_test

import (
	"bytes"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
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

	for data, ci, err := pcap.ReadPacketData(); err == nil; data, ci, err = pcap.ReadPacketData() {
		// Pass ci by pointer, but Wrap will make a copy, since gopacket NoCopy doesn't preserve the values.
		p, err := tcpip.Wrap(&ci, data)
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
			leftRetransmits: 11, rightRetransmits: 8, exceeded: 57, leftSacks: 31, rightSacks: 24, leftTimestamps: 162, rightTimestamps: 174},
		{name: "ipv6", fn: "testfiles/ndt-nnwk2_1611335823_00000000000C2DA8.pcap.gz", packets: 15,
			leftRetransmits: 0, rightRetransmits: 0, exceeded: 5, leftSacks: 0, rightSacks: 0, leftTimestamps: 8, rightTimestamps: 7},
		{name: "protocolErrors2", fn: "testfiles/ndt-nnwk2_1611335823_00000000000C2DA9.pcap.gz", packets: 5180,
			leftRetransmits: 0, rightRetransmits: 0, exceeded: 2890, leftSacks: 0, rightSacks: 0, leftTimestamps: 1814, rightTimestamps: 3364},
		{name: "foobar", fn: "testfiles/ndt-xkrzj_1632230485_0000000000AE8EE2.pcap.gz", packets: 49,
			leftRetransmits: 0, rightRetransmits: 0, exceeded: 22, leftSacks: 0, rightSacks: 0, leftTimestamps: 21, rightTimestamps: 28},
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
	}
}

func BenchmarkStateOptions(b *testing.B) {
	port := layers.TCPPort(80)
	s := tcp.NewState(net.IP{}, port)
	pTime := time.Now()
	s.SeqTracker.Seq(0, pTime, 123, 0, true, &s.Stats)
	s.SeqTracker.Seq(1, pTime, 124, 1000, false, &s.Stats)
	s.SeqTracker.Seq(2, pTime, 1124, 2000, true, &s.Stats)
	fakeOptions := []byte{
		layers.TCPOptionKindMSS, 4, 0, 0,
		layers.TCPOptionKindTimestamps, 10, 0, 1, 2, 3, 4, 5, 6, 7,
		layers.TCPOptionKindSACK, 18, 0, 0, 1, 1, 0, 0, 1, 2, 0, 0, 2, 3, 0, 0, 2, 4,
	}
	opts, err := tcp.ParseTCPOptions(fakeOptions)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, opt := range opts {
			// Explicit byte reversal improves this from 480 nsec (6 allocs, 120 bytes) to 56 nsec (no allocs) per op.
			s.ObsoleteOption(port, false, pTime, &opt)
		}
	}
}

// func TestToTCPHeaderGo(t *testing.T) {
// 	// These values are mostly taken from WireShark.
// 	want := tcp.TCPHeaderGo{
// 		SrcPort:    40337,
// 		DstPort:    443,
// 		SeqNum:     838132236,
// 		AckNum:     1190441402,
// 		DataOffset: 128,
// 		Flags:      0x10,
// 		Window:     676,
// 		Checksum:   10721,
// 		Urgent:     0,
// 	}
// 	t.Logf("Want: %#v", want)

// 	// These byte values are taken from a WireShark decoded packet.
// 	hex := "9d 91 01 bb 31 f4 e2 0c 46 f4 b1 ba 80 10 02 a4 29 e1 00 00 01 01 08 0a 0b 62 9d 29 2b b5 a7 0e"
// 	hexArray := strings.Split(hex, " ")
// 	data := make([]byte, len(hexArray))
// 	for i, v := range hexArray {
// 		b, _ := strconv.ParseInt(v, 16, 16)
// 		data[i] = byte(b)
// 	}

// 	hw := tcp.TCPHeaderWrapper{}
// 	tcp.WrapTCP(data, &hw)
// 	if diff := deep.Equal(&hw.TCPHeaderGo, &want); diff != nil {
// 		t.Error(diff)
// 	}

// 	if len(hw.Options) != 1 {
// 		t.Errorf("Options = %v, want 1", len(hw.Options))
// 	}

// 	tsVal, tsEcn, err := hw.Options[0].GetTimestamps()
// 	if err != nil {
// 		t.Fatalf("getTimestamps() = %v", err)
// 	}
// 	if tsVal != 191012137 {
// 		t.Errorf("TimestampValue = %v, want 191012137", tsVal)
// 	}
// 	if tsEcn != 733325070 {
// 		t.Errorf("TimestampECN = %v, want 733325070", tsEcn)
// 	}
// }

// func BenchmarkToTCPHeaderBinary_Read(b *testing.B) {
// 	var in tcp.TCPHeader
// 	var out tcp.TCPHeaderGo
// 	for i := 0; i < b.N; i++ {
// 		_ = in.XToTCPHeaderGo(&out)
// 	}
// 	log.Println(out)
// }

func BenchmarkToTCPHeaderGo_Swaps(b *testing.B) {
	// These byte values are taken from a WireShark decoded packet.
	hex := "9d 91 01 bb 31 f4 e2 0c 46 f4 b1 ba 80 10 02 a4 29 e1 00 00 01 01 08 0a 0b 62 9d 29 2b b5 a7 0e"
	hexArray := strings.Split(hex, " ")
	data := make([]byte, len(hexArray))
	for i, v := range hexArray {
		b, _ := strconv.ParseInt(v, 16, 16)
		data[i] = byte(b)
	}

	//hw := tcp.TCPHeaderWrapper{}
	var in tcp.TCPHeader
	var out tcp.TCPHeaderGo

	for i := 0; i < b.N; i++ {
		in.ToTCPHeaderGo2(&out)
	}
}

func BenchmarkWrapTCP(b *testing.B) {
	// These byte values are taken from a WireShark decoded packet.
	hex := "9d 91 01 bb 31 f4 e2 0c 46 f4 b1 ba 80 10 02 a4 29 e1 00 00 01 01 08 0a 0b 62 9d 29 2b b5 a7 0e"
	hexArray := strings.Split(hex, " ")
	data := make([]byte, len(hexArray))
	for i, v := range hexArray {
		b, _ := strconv.ParseInt(v, 16, 16)
		data[i] = byte(b)
	}

	hw := tcp.TCPHeaderWrapper{}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tcp.WrapTCP(data, &hw)
	}
}
