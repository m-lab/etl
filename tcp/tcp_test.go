package tcp_test

import (
	"bytes"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
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
		truncated                         int64
		exceeded                          int64
		tsCount                           int64
	}
	tests := []test{
		{name: "retransmits", fn: "testfiles/ndt-nnwk2_1611335823_00000000000C2DFE.pcap",
			packets: 336, leftRetransmits: 11, rightRetransmits: 8, truncated: 0, exceeded: 0, tsCount: 510},
		{name: "ipv6", fn: "testfiles/ndt-nnwk2_1611335823_00000000000C2DA8.pcap.gz",
			packets: 15, leftRetransmits: 0, rightRetransmits: 0, truncated: 0, exceeded: 0, tsCount: 22},
		{name: "protocolErrors2", fn: "testfiles/ndt-nnwk2_1611335823_00000000000C2DA9.pcap.gz",
			packets: 5180, leftRetransmits: 0, rightRetransmits: 0, truncated: 0, exceeded: 2880, tsCount: 8542},
		{name: "foobar", fn: "testfiles/ndt-xkrzj_1632230485_0000000000AE8EE2.pcap.gz",
			packets: 49, leftRetransmits: 0, rightRetransmits: 0, truncated: 0, exceeded: 0, tsCount: 77},
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
		if summary.Packets != tt.packets {
			t.Errorf("test:%s: Packets = %v, want %v", tt.name, summary.Packets, tt.packets)
		}
		// TODO - replace these with LeftStats and RightStats.
		if summary.LeftState.Stats.RetransmitPackets != tt.leftRetransmits {
			t.Errorf("test:%s: Left.Retransmits = %v, want %v", tt.name, summary.LeftState.Stats.RetransmitPackets, tt.leftRetransmits)
		}
		if summary.RightState.Stats.RetransmitPackets != tt.rightRetransmits {
			t.Errorf("test:%s: Right.Retransmits = %v, want %v", tt.name, summary.RightState.Stats.RetransmitPackets, tt.rightRetransmits)
		}
		// if summary.TruncatedPackets != tt.truncated {
		// 	t.Errorf("test:%s: TruncatedPackets = %v, want %v", tt.name, summary.TruncatedPackets, tt.truncated)
		// }
		if summary.RightState.Stats.SendNextExceededLimit != tt.exceeded {
			t.Errorf("test:%s: SendNextExceededLimit = %v, want %v", tt.name, summary.RightState.Stats.SendNextExceededLimit, tt.exceeded)
		}
		if summary.LeftState.Stats.OptionCounts[layers.TCPOptionKindTimestamps] != tt.tsCount {
			t.Errorf("test:%s: Timestamps = %v, want %v", tt.name,
				summary.LeftState.Stats.OptionCounts[layers.TCPOptionKindTimestamps], tt.tsCount)
		}
	}
}
