package tcp_test

import (
	"log"
	"math/rand"
	"testing"
	"time"

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
