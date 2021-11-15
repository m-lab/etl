package tcp_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/google/gopacket/layers"
	"github.com/m-lab/etl/tcp"
)

func TestTracker_SendNext(t *testing.T) {
	tr := tcp.Tracker{}
	tr.Seq(1234, 0, true) // SYN, no data
	tr.Seq(1235, 20, false)
	tr.Seq(1255, 10, false)
	if tr.SendNext() != 1265 {
		t.Errorf("SendNext() = %v, want %v", tr.SendNext(), 1265)
	}

	// Retransmit
	if _, b := tr.Seq(1240, 12, false); !b {
		t.Errorf("Seq() = %v, want %v", b, true)
	}
	// SendNext should be unchanged.
	if tr.SendNext() != 1265 {
		t.Errorf("SendNext() = %v, want %v", tr.SendNext(), 1265)
	}

	if _, b := tr.Seq(tr.SendNext(), 10, false); b {
		t.Errorf("Seq() = %v, want %v", b, false)
	}
	if tr.SendNext() != 1275 {
		t.Errorf("SendNext() = %v, want %v", tr.SendNext(), 1275)
	}
	if tr.Sent() != 40 {
		t.Errorf("Sent() = %v, want %v", tr.Sent(), 40)
	}
	tr.Ack(1234, false)
	if tr.Acked() != 0 {
		t.Errorf("Acked() = %v, want %v", tr.Acked(), 0)
	}
	tr.Ack(1244, false)
	if tr.Acked() != 10 {
		t.Errorf("Acked() = %v, want %v", tr.Acked(), 10)
	}

	tr.Seq(5<<28, 0, false)
	if tr.SendNext() != 1275 {
		t.Errorf("SendNext() = %v, want %v", tr.SendNext(), 1275)
	}
	if tr.Stats().BadDeltas != 1 {
		t.Errorf("Stats().BadDeltas = %v, want %v", tr.Stats().BadDeltas, 1)
	}

	// Seq that doesn't match previous data length.
	tr.Seq(1300, 0, false)
	// Seq should advance, but we should also observe an error.
	if tr.SendNext() != 1300 {
		t.Errorf("SendNext() = %v, want %v", tr.SendNext(), 1300)
	}
	if tr.Stats().MissingPackets != 1 {
		t.Errorf("Stats() = %v, want %v", tr.Stats().MissingPackets, 1)
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
