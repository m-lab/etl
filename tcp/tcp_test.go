package tcp_test

import (
	"testing"

	"github.com/m-lab/etl/tcp"
)

func TestTracker_NextSeq(t *testing.T) {
	tr := tcp.Tracker{}
	tr.Seq(1234, 0, true) // SYN, no data
	tr.Seq(1235, 20, false)
	tr.Seq(1255, 10, false)
	if tr.NextSeq() != 1265 {
		t.Errorf("NextSeq() = %v, want %v", tr.NextSeq(), 1265)
	}

	// Retransmit
	if b := tr.Seq(1240, 12, false); !b {
		t.Errorf("Seq() = %v, want %v", b, true)
	}
	// NextSeq should be unchanged.
	if tr.NextSeq() != 1265 {
		t.Errorf("NextSeq() = %v, want %v", tr.NextSeq(), 1265)
	}

	if b := tr.Seq(tr.NextSeq(), 10, false); b {
		t.Errorf("Seq() = %v, want %v", b, false)
	}
	if tr.NextSeq() != 1275 {
		t.Errorf("NextSeq() = %v, want %v", tr.NextSeq(), 1275)
	}
	if tr.Sent() != 40 {
		t.Errorf("Sent() = %v, want %v", tr.Sent(), 40)
	}
	tr.Ack(1234)
	if tr.Acked() != 0 {
		t.Errorf("Acked() = %v, want %v", tr.Acked(), 0)
	}
	tr.Ack(1244)
	if tr.Acked() != 10 {
		t.Errorf("Acked() = %v, want %v", tr.Acked(), 10)
	}

	tr.Seq(5<<28, 0, false)
	if tr.NextSeq() != 1275 {
		t.Errorf("NextSeq() = %v, want %v", tr.NextSeq(), 1275)
	}
	if tr.Errors() != 1 {
		t.Errorf("Errors() = %v, want %v", tr.Errors(), 1)
	}

	// Seq that doesn't match previous data length.
	tr.Seq(1300, 0, false)
	// Seq should advance, but we should also observe an error.
	if tr.NextSeq() != 1300 {
		t.Errorf("NextSeq() = %v, want %v", tr.NextSeq(), 1300)
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
				if got := w.NextSeq(); got != tt.want {
					t.Errorf("Tracker.NextSeq() = %v, want %v", got, tt.want)
				}
			})
		}*/
}
