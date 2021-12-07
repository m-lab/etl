package tcp

import (
	"fmt"
	"time"
)

var (
	ErrTrackerNotInitialized = fmt.Errorf("tracker not initialized")
	ErrInvalidDelta          = fmt.Errorf("invalid delta")
	ErrInvalidSackBlock      = fmt.Errorf("invalid sack block")
	ErrLateSackBlock         = fmt.Errorf("sack block to left of ack")
)

type SeqNum uint32

func (sn SeqNum) diff(next SeqNum) (int32, error) {
	delta := int32(sn - next)
	if !(-1<<30 < delta && delta < 1<<30) {
		//	info.Printf("invalid sequence delta %d->%d (%d)", previous, clock, delta)
		return delta, ErrInvalidDelta
	}
	return delta, nil
}

// TODO - build a sackblock model, that consolidates new sack blocks into existing state.
type sackBlock struct {
	Left, Right SeqNum
}

type seqInfo struct {
	count int
	seq   SeqNum
	pTime UnixNano // packet capture time
}

// Ack matcher keeps track of past sequence numbers, and matches them
// when they are acked.
type ackMatcher struct {
	backing []seqInfo
	seqs    []seqInfo
}

// When we add seqs, we discard any out of order.
func (m *ackMatcher) Add(seq SeqNum, pNum int, pTime UnixNano) {
	if len(m.seqs) > 0 {
		if diff, err := seq.diff(m.seqs[len(m.seqs)-1].seq); diff <= 0 || err != nil {
			return
		}
	}
	if len(m.seqs)+1 > cap(m.seqs) {
		if len(m.seqs) > 3*cap(m.backing)/4 {
			m.backing = make([]seqInfo, 0, 2*cap(m.backing))
		}
		m.seqs = append(m.backing[:0], m.seqs...)
		m.backing = m.seqs[:0]
	}
	m.seqs = append(m.seqs, seqInfo{seq: seq, count: pNum, pTime: pTime})
	return
}

// When an ack comes in, we try to match it, and delete all entries earlier than the ack number.
func (m *ackMatcher) Match(ack SeqNum) (UnixNano, int, bool) {
	if len(m.seqs) == 0 {
		return 0, 0, false
	}
	for i := 0; i < len(m.seqs); i++ {
		seq := m.seqs[i]
		if diff, err := seq.seq.diff(ack); err != nil {
			return 0, 0, false
		} else if diff > 0 {
			// If new ack is after this seq, then remove all earlier seqs.
			m.seqs = m.seqs[i:]
			return 0, 0, false
		} else if diff == 0 {
			// If new ack is same as this seq, then remove all earlier seqs.
			m.seqs = m.seqs[i:]
			return seq.pTime, seq.count, true
		}
	}
	m.seqs = m.seqs[:0]
	return 0, 0, false
}

func newMatcher() *ackMatcher {
	back := make([]seqInfo, 0, 200)
	return &ackMatcher{
		seqs:    back,
		backing: back,
	}
}

// TODO estimate the event times at the remote end, using the Timestamp option TSVal and TSecr fields.
type Tracker struct {
	initialized bool
	startTime   UnixNano // Initial packet time
	packets     uint32   // Number of calls to Seq function
	seq         SeqNum   // The last sequence number observed, not counting retransmits
	synFin      uint32   // zero, one or two, depending on whether SYN and FIN have been sent

	sendUNA     SeqNum   // greatest observed ack
	sendUNATime UnixNano // time we saw greatest ack
	acks        uint32   // number of acks (from other side)
	onlyAcks    uint32   // Number of packets that only have ACKs, no data.
	acked       uint64   // bytes acked
	maxGap      int32    // Max observed gap between acked and NextSeq()

	sent      uint64      // actual bytes sent, including retransmits, but not SYN or FIN
	sacks     []sackBlock // keeps track of outstanding SACK blocks
	sackBytes uint64      // keeps track of total bytes reported missing in SACK blocks

	lastDataLength uint16 // Used to compute NextSeq()

	// This will get very large - one entry per packet.
	// TODO - investigate using a circular buffer or linked list instead?
	seqTimes *ackMatcher

	*LogHistogram
}

func NewTracker() *Tracker {
	iat, _ := NewLogHistogram(.00001, 0.1, 6.0)
	return &Tracker{seqTimes: newMatcher(), LogHistogram: &iat}
}

func (t *Tracker) updateSendUNA(seq SeqNum, time UnixNano) {
	t.sendUNA = seq
	t.sendUNATime = time
}

func (t *Tracker) Summary() string {
	return fmt.Sprintf("%5d packets, %5d/%5d acks w/data, %5d max gap\n",
		t.packets, t.acks-t.onlyAcks, t.acks, t.maxGap)
}

// SendNext returns the uint32 value of the expected next sequence number.
func (t *Tracker) SendNext() SeqNum {
	return t.seq + SeqNum(t.lastDataLength) // wraps at 2^32
}

// ByteCount returns the number of bytes sent so far, not including retransmits
func (t *Tracker) ByteCount() uint64 {
	return t.sent
}

func diff(clock uint32, previous uint32) (int32, error) {
	delta := int32(clock - previous)
	if !(-1<<30 < delta && delta < 1<<30) {
		//	info.Printf("invalid sequence delta %d->%d (%d)", previous, clock, delta)
		return delta, ErrInvalidDelta
	}
	return delta, nil
}

//var badSeq = logx.NewLogEvery(sparseLogger, 100*time.Millisecond)
//var badAck = logx.NewLogEvery(sparseLogger, 100*time.Millisecond)

// Seq updates the tracker based on an observed packet with sequence number seq and content size length.
// Initializes the tracker if it hasn't been initialized yet.
// Returns the bytes in flight (not including retransmits) and boolean indicator if this is a retransmit
func (t *Tracker) Seq(count int, pTime UnixNano, clock SeqNum, length uint16, synFin bool, sw *StatsWrapper) (int32, bool) {
	t.packets++ // Some of these may be retransmits.

	if !t.initialized {
		t.startTime = pTime
		t.seq = clock
		t.sendUNA = clock // nothing acked so far
		t.initialized = true
	}
	// Use this unless we are sending new data.
	// TODO - correct this for sum of sizes of sack block scoreboard.
	// This does not include retransmits!!
	// NOTE: This may be greater than the window size if capture missed some packets.
	inflight, err := t.SendNext().diff(t.sendUNA)
	if err != nil {
		//sparse500.Println("inflight diff error", t.SendNext(), t.sendUNA)
	}

	// TODO handle errors
	delta, err := clock.diff(t.seq)
	if err != nil {
		sw.BadDeltas++
		//badSeq.Printf("Bad seq %4X -> %4X\n", t.seq, clock)
		return inflight, false
	}
	if delta < 0 {
		// DO NOT update w.seq or w.lastDataLength, as this is a retransmit
		t.sent += uint64(length)
		sw.Retransmit(length)
		return inflight, true
	}

	// Everything below applies only to new data packets, not retransmits
	if delta != int32(t.lastDataLength) {
		sw.MissingPackets++
		//sparse500.Printf("%d: Missing packet?  delta (%d) does not match last data size (%d)\n", t.packets, delta, t.lastDataLength) // VERBOSE
	}

	if synFin {
		t.synFin++ // Should we check if this is greater than 2?
		// Should this include length?
		t.lastDataLength = 1 + length
	} else {
		t.lastDataLength = length
	}

	t.sent += uint64(length)
	t.seq = clock
	t.seqTimes.Add(clock, count, pTime)

	gap, err := t.seq.diff(t.sendUNA)
	if err != nil {
		//sparse2.Println("gap diff error:", t.seq, t.sendUNA)
	}
	if gap > t.maxGap {
		t.maxGap = gap
	}

	inflight, err = t.SendNext().diff(t.sendUNA)
	if err != nil {
		//sparse2.Println("inflight diff error:", t.SendNext(), t.sendUNA)
	}

	return inflight, false
}

func (t *Tracker) Acked() uint64 {
	return t.acked
}

// Ack updates the tracker based on an observed ack value.
// Returns the time observed by the packet capture since the correponding sequence number was sent.
func (t *Tracker) Ack(count int, pTime UnixNano, clock SeqNum, withData bool, sw *StatsWrapper) (int, time.Duration) {
	if !t.initialized {
		sw.OtherErrors++
		//info.Printf("PKT: %d Ack called before Seq", count)
	}
	delta, err := clock.diff(t.sendUNA)
	if err != nil {
		sw.BadDeltas++
		// TODO should this sometimes update the sendUNA, or always?
		t.updateSendUNA(clock, pTime)
		return 0, 0
	}
	if delta > 0 {
		t.acked += uint64(delta)
		t.acks++
	}
	if !withData {
		t.onlyAcks++
	}
	defer t.updateSendUNA(clock, pTime)

	seqTime, count, ok := t.seqTimes.Match(clock)
	if ok {
		// Only update InterArrivalTime when we find the corresponding sequence number in map.
		t.LogHistogram.Add(pTime.Sub(t.sendUNATime).Seconds())

		return count, pTime.Sub(seqTime)
	} else {
		//sparse500.Printf("Ack out of order? %7d (%7d) %7d..%7d", t.sendUNA, clock, t.seq, t.SendNext())
		return 0, 0
	}
}

func (t *Tracker) SendUNA() SeqNum {
	return t.sendUNA
}

// Check checks that a sack block is consistent with the current window.
func (t *Tracker) checkSack(sb sackBlock) error {
	// block should ALWAYS have positive width
	if width, err := sb.Right.diff(sb.Left); err != nil || width <= 0 {
		//sparse500.Println(ErrInvalidSackBlock, err, width, t.Acked())
		return ErrInvalidSackBlock
	}
	// block Right should ALWAYS be to the left of NextSeq()
	// If not, we may have missed recording a packet!
	if overlap, err := t.SendNext().diff(sb.Right); err != nil || overlap < 0 {
		//sparse500.Println(ErrInvalidSackBlock, err, overlap, t.Acked())
		return ErrInvalidSackBlock
	}
	// Left should be to the right of ack
	if overlap, err := sb.Left.diff(t.sendUNA); err != nil || overlap < 0 {
		// These often correspond to packets that show up as spurious retransmits in WireShark.
		//sparse500.Println(ErrLateSackBlock, err, overlap, t.Acked())
		return ErrLateSackBlock
	}
	return nil
}

// Sack updates the counter with sack information (from other direction)
// For some reason, this code causes a lot of runtime.newobject calls.
func (t *Tracker) Sack(sb sackBlock, sw *StatsWrapper) {
	if !t.initialized {
		sw.OtherErrors++
		//info.Println(ErrTrackerNotInitialized)
	}
	sw.Sacks++
	// Auto gen code
	if err := t.checkSack(sb); err != nil {
		sw.BadSacks++
		//sparse500.Println(ErrInvalidSackBlock, t.sendUNA, sb, t.SendNext())
	}
	//t.sacks = append(t.sacks, block)
	t.sackBytes += uint64(sb.Right - sb.Left)
}
