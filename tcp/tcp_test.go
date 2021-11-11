package tcp_test

import (
	"io/ioutil"
	"os"
	"testing"

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
	if tr.Errors() != 1 {
		t.Errorf("Errors() = %v, want %v", tr.Errors(), 1)
	}

	// Seq that doesn't match previous data length.
	tr.Seq(1300, 0, false)
	// Seq should advance, but we should also observe an error.
	if tr.SendNext() != 1300 {
		t.Errorf("SendNext() = %v, want %v", tr.SendNext(), 1300)
	}
	if tr.Errors() != 2 {
		t.Errorf("Errors() = %v, want %v", tr.Errors(), 2)
	}

	/*
		tests := []struct {
			name   string
			fields fields
			want   uint32
		}{
			// TODO: Add test cases.
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				w := &Tracker{
					initialized:    tt.fields.initialized,
					packets:        tt.fields.packets,
					seq:            tt.fields.seq,
					syn:            tt.fields.syn,
					sent:           tt.fields.sent,
					retransmits:    tt.fields.retransmits,
					ack:            tt.fields.ack,
					acked:          tt.fields.acked,
					sacks:          tt.fields.sacks,
					sackBytes:      tt.fields.sackBytes,
					lastDataLength: tt.fields.lastDataLength,
				}
				if got := w.SendNext(); got != tt.want {
					t.Errorf("Tracker.SendNext() = %v, want %v", got, tt.want)
				}
			})
		}*/
}

// TestParse exercises a lot of code, on the ipv4 path.  It only checks
// some basic stats, which are assumed to be correct, but not inspected.
func TestParse(t *testing.T) {
	f, err := os.Open("testfiles/ndt-nnwk2_1611335823_00000000000C2DFE.pcap")
	if err != nil {
		t.Fatal(err)
	}
	data, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	tcp := tcp.Parser{}
	summary, err := tcp.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Packets != 336 {
		t.Errorf("Packets = %v, want %v", summary.Packets, 336)
	}
	if summary.FirstRetransmits != 11 {
		t.Errorf("FirstRetransmits = %v, want %v", summary.FirstRetransmits, 11)
	}
	if summary.SecondRetransmits != 8 {
		t.Errorf("SecondRetransmits = %v, want %v", summary.SecondRetransmits, 8)
	}
	if summary.TruncatedPackets != 0 {
		t.Errorf("TruncatedPackets = %v, want %v", summary.TruncatedPackets, 0)
	}
}
