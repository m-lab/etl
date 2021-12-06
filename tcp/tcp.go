// Package tcp provides tools to reconstruct tcp state from pcap files.
// It is structured as a model that consumes packets, and maintains
// state and statistics about the connection.
package tcp

import (
	"fmt"
	"log"
	"math"
	"net"
	"time"
	"unsafe"

	"github.com/google/gopacket/layers"
)

// The Model should consume raw IP payloads of type IPProtocolTCP and update the model.
// It might also take a tcpip.IP interface object for access to things like IP addresses???

/*
models for:
 * seq and acks
 * options, including timestamps and sack blocks
 * window size
 * connection characteristics, such as MSS, wscale

*/

var (
	// NOTE: These logs are causing a lot of objects to escape the stack.  Removing them
	// reduces the allocations from 16.4MB to 13.9MB.

	//info         = log.New(os.Stdout, "info: ", log.LstdFlags|log.Lshortfile)
	//sparseLogger = log.New(os.Stdout, "sparse: ", log.LstdFlags|log.Lshortfile)
	//sparse500    = logx.NewLogEvery(sparseLogger, 500*time.Millisecond)
	//sparse2      = logx.NewLogEvery(sparseLogger, 501*time.Millisecond)
	ErrNoIPLayer = fmt.Errorf("no IP layer")

	ErrNotTCP             = fmt.Errorf("not a TCP packet")
	ErrTruncatedTCPHeader = fmt.Errorf("truncated TCP header")
	ErrBadOption          = fmt.Errorf("bad option")
	ErrNoMoreOptions      = fmt.Errorf("no more options")
)

type UnixNano int64

func (un UnixNano) Sub(other UnixNano) time.Duration {
	return time.Duration(un - other)
}

type BE16 [2]byte

func (b BE16) Uint16() uint16 {
	swap := [2]byte{b[1], b[0]}
	return *(*uint16)(unsafe.Pointer(&swap))
}
func (b BE16) PutUint16(t *uint16) {
	tt := (*[2]byte)(unsafe.Pointer(t))
	*tt = [2]byte{b[1], b[0]}
}
func (b BE16) PutTCPPort(t *layers.TCPPort) {
	tt := (*[2]byte)(unsafe.Pointer(t))
	*tt = [2]byte{b[1], b[0]}
}

type BE32 [4]byte

func (b BE32) Uint32() uint32 {
	swap := [4]byte{b[3], b[2], b[1], b[0]}
	return *(*uint32)(unsafe.Pointer(&swap))
}
func (b BE32) PutUint32(t *uint32) {
	tt := (*[4]byte)(unsafe.Pointer(t))
	*tt = [4]byte{b[3], b[2], b[1], b[0]}
}

/******************************************************************************
 * TCP Header and state machine
******************************************************************************/
type TCPHeader struct {
	// TODO update these to use BE16 and BE32
	srcPort, dstPort BE16  // Source and destination port
	seqNum           BE32  // Sequence number
	ackNum           BE32  // Acknowledgement number
	dataOffset       uint8 //  DataOffset: upper 4 bits
	Flags                  // Flags
	window           BE16  // Window
	checksum         BE16  // Checksum
	urgent           BE16  // Urgent pointer
}

var TCPHeaderSize = int(unsafe.Sizeof(TCPHeader{}))

type Flags uint8

func (f Flags) FIN() bool {
	return (f & 0x01) != 0
}

func (f Flags) SYN() bool {
	return (f & 0x02) != 0
}

func (f Flags) RST() bool {
	return (f & 0x04) != 0
}

func (f Flags) PSH() bool {
	return (f & 0x08) != 0
}

func (f Flags) ACK() bool {
	return (f & 0x10) != 0
}

func (f Flags) URG() bool {
	return (f & 0x20) != 0
}

func (f Flags) ECE() bool {
	return (f & 0x40) != 0
}

func (f Flags) CWR() bool {
	return (f & 0x80) != 0
}

type TCPHeaderGo struct {
	SrcPort, DstPort layers.TCPPort // Source and destination port
	SeqNum           uint32         // Sequence number
	AckNum           uint32         // Acknowledgement number
	DataOffset       uint8          // The actual data offset (different from binary tcp field)
	Flags                           // Flags
	Window           uint16         // Window
	Checksum         uint16         // Checksum
	Urgent           uint16         // Urgent pointer
}

// From populates the header from binary TCPHeader data, in bigendian.
// Works correctly only on LittleEndian architecture machines.
func (hdr *TCPHeaderGo) From(data []byte) error {
	if len(data) < TCPHeaderSize {
		return ErrTruncatedTCPHeader
	}
	tcp := (*TCPHeader)(unsafe.Pointer(&data[0]))
	tcp.srcPort.PutTCPPort(&hdr.SrcPort)
	tcp.dstPort.PutTCPPort(&hdr.DstPort)
	tcp.seqNum.PutUint32(&hdr.SeqNum)
	tcp.ackNum.PutUint32(&hdr.AckNum)
	hdr.DataOffset = 4 * (tcp.dataOffset >> 4)
	hdr.Flags = tcp.Flags
	tcp.window.PutUint16(&hdr.Window)
	tcp.checksum.PutUint16(&hdr.Checksum)
	tcp.urgent.PutUint16(&hdr.Urgent)

	if int(hdr.DataOffset) > len(data) {
		return ErrTruncatedTCPHeader
	}

	return nil
}

type tcpOption struct {
	kind layers.TCPOptionKind // Kind of option
	len  uint8                // Length of entire option including kind and length.
	data [38]byte             // Overlay of actual binary option fields, not likely to be full 38 bytes.
}

// USE WITH CAUTION:  This accesses an unsafe pointer.
func (o *tcpOption) getUint32(i int) uint32 {
	be := (*[10]BE32)(unsafe.Pointer(&o.data[0]))[i]
	return be.Uint32()
}

// USE WITH CAUTION:  This accesses an unsafe pointer.
func (o *tcpOption) getUint16(i int) uint16 {
	be := (*[20]BE16)(unsafe.Pointer(&o.data[0]))[i]
	return be.Uint16()
}

func (o *tcpOption) GetMSS() (uint16, error) {
	if o.kind != layers.TCPOptionKindMSS || o.len != 4 {
		return 0, ErrBadOption
	}
	return o.getUint16(0), nil
}

func (o *tcpOption) GetWS() (uint8, error) {
	if o.kind != layers.TCPOptionKindWindowScale || o.len != 3 {
		return 0, ErrBadOption
	}
	return o.data[0], nil
}

func (o *tcpOption) GetTimestamps() (uint32, uint32, error) {
	if o.kind != layers.TCPOptionKindTimestamps || o.len != 10 {
		return 0, 0, ErrBadOption
	}
	return o.getUint32(0), o.getUint32(1), nil
}

// This does not cause sackBlock to escape from stack.
func (o *tcpOption) fillSackBlock(sb *sackBlock, i int) error {
	if o.kind != layers.TCPOptionKindSACK || (o.len-2)%8 != 0 || i > int(o.len-2)/8 || sb == nil {
		return ErrBadOption
	}
	sb.Left = o.getUint32(2 * i)
	sb.Right = o.getUint32(2*i + 1)
	return nil
}

func (o *tcpOption) getSackBlock(i int) (sb sackBlock, err error) {
	if o.kind != layers.TCPOptionKindSACK || (o.len-2)%8 != 0 || i > int(o.len-2)/8 {
		return sb, ErrBadOption
	}
	sb.Left = o.getUint32(2 * i)
	sb.Right = o.getUint32(2*i + 1)
	return sb, nil
}

// Could we avoid escapes by moving this to a stats object?
func (o *tcpOption) processSACKs(f func(sackBlock, *StatsWrapper), sw *StatsWrapper) error {
	if o.kind != layers.TCPOptionKindSACK || (o.len-2)%8 != 0 {
		log.Println("TCP option is not SACK")
		return ErrBadOption
	}
	numBlocks := (int(o.len) - 2) / 8
	// This alloc seems to cost only 15 nsec/call.
	// unclear why this isn't on the stack.
	//sb := sackBlock{} // This is faster than var sb sackBlock, though both do 1 alloc.
	for i := 0; i < numBlocks; i++ {
		//err := o.fillSackBlock(&sb, i)
		sb, err := o.getSackBlock(i)
		if err != nil {
			return err
		}
		f(sb, sw)
	}
	return nil
}

// This skips Nop options, and returns nil data there are no more options.
func NextOptionInPlace(data []byte) ([]byte, *tcpOption, error) {
	// For loop to handle Nop options.
	for len(data) > 0 && data[0] == layers.TCPOptionKindNop {
		data = data[1:]
	}
	if len(data) == 0 {
		return nil, nil, ErrNoMoreOptions
	}

	opt := (*tcpOption)(unsafe.Pointer(&data[0]))
	if opt.kind > 15 {
		return nil, opt, ErrBadOption
	}
	switch opt.kind {
	// This won't be a nop, because we already handled those above.
	case layers.TCPOptionKindEndList:
		return nil, opt, ErrNoMoreOptions // Technically we are returning one, but effect is the same.
	default:
		if len(data) < 2 || int(opt.len) > len(data) {
			return nil, nil, ErrTruncatedTCPHeader
		}
		if opt.len > 40 {
			return nil, nil, ErrBadOption
		}
		// Could also use a byte array copy here.
		return data[opt.len:], opt, nil
	}
}

type TcpStats struct {
	Packets   int64
	Truncated int64

	OptionCounts []int64 // 16 counts, indicating how often each option type occurred.

	RetransmitPackets int64
	RetransmitBytes   int64

	Sacks int64

	ECECount      int64
	WindowChanges int64

	// Errors and anomalies
	BadSacks              int64 // Number of sacks with bad boundaries
	BadDeltas             int64 // Number of seqs and acks that were more than 1<<30 off from previous value.
	MissingPackets        int64 // Observations of packet sequence numbers that didn't match previous payload length.
	SendNextExceededLimit int64 // Number of times SendNext() returned a value that exceeded the receiver window limit.
	TTLChanges            int64 // Observed number of TTL values that don't match first IP header.
	SrcPortErrors         int64 // Observed number of source ports that don't match first IP header.
	DstPortErrors         int64 // Observed number of dest ports that don't match tcp.DstPort
	OtherErrors           int64 // Number of other errors that occurred.

}

type StatsWrapper struct {
	TcpStats
}

func (sw *StatsWrapper) Retransmit(bytes uint16) {
	sw.RetransmitPackets++
	sw.RetransmitBytes += int64(bytes)
}

func (sw *StatsWrapper) Option(opt layers.TCPOptionKind) {
	if opt < 16 {
		sw.OptionCounts[opt]++
	}

}

// JitterTracker
// TSVal is an incoming (or outgoing) tick value.  It's noise can be used to calculate one way jitter.
// TSEcho is returned by the remote host.  It's value is used to calculate delay.
// Only the earliest echo value should be used to compute delay.
//
//  Likely need to look for the first occurance, or perhaps the lowest delay occurance, of each TSVal,
// and the corresponding first occurance of TSEcr.
type JitterTracker struct {
	initialized  bool
	firstTSVal   uint32
	firstPktTime UnixNano

	tickRate time.Duration // Interval between ticks.  For server side this is always 1 msec.

	// We should end up with tick lines, possibly converging or diverging.
	// The actual delay is the gap between the two tick lines.
	// NOTE: We use floating point Unix seconds, which have a precision of about 300 nsec.
	// So, we lose any precision beyond that.
	valLR  LinReg
	echoLR LinReg

	ValCount       int
	ValOffsetSum   float64
	ValOffsetSumSq float64
	SumProdValPkt  float64 // sum of product of ValOffset and PktTimeOffset

	EchoCount     int
	EchoOffsetSum float64
}

func (t *JitterTracker) ValLR() string {
	return t.valLR.String()
}

func (t *JitterTracker) TickInterval() time.Duration {
	return time.Duration(1000.0 / t.valLR.Slope())
}

func (t *JitterTracker) EchoLR() string {
	return t.echoLR.String()
}
func (jt *JitterTracker) LRDelay(pktTime UnixNano) float64 {
	if jt.ValCount < 3 || jt.EchoCount < 3 {
		return 0
	}
	dt := pktTime.Sub(jt.firstPktTime)
	jitter := jt.valLR.Estimate(dt.Seconds())
	delay := jt.echoLR.Estimate(dt.Seconds())
	return (delay - jitter) / jt.valLR.Slope() // In seconds.
}

func (jt *JitterTracker) LRDelay0() time.Duration {
	if jt.ValCount < 3 || jt.EchoCount < 3 {
		return 0
	}
	t := jt.valLR.MeanX()
	jitter := jt.valLR.Estimate(t) // This is the mean of Y in ticks.
	delay := jt.echoLR.Estimate(t) // This is the mean of Y in ticks.
	return time.Duration(1000000 * float64(jitter-delay) * float64(jt.TickInterval()))
}

// Adjust attempts to adjust the TSVal and pktTime to interval since the first reported packet.
// The TSVal is adjusted based on the inferred tick rate.
func (j *JitterTracker) Adjust(tsval uint32, pktTime UnixNano) (time.Duration, time.Duration) {
	return time.Duration(tsval-j.firstTSVal) * j.tickRate, pktTime.Sub(j.firstPktTime)
}

// Add adds a new offset between TSVal and packet capture time to the jitter tracker.
// offset should be TSVal - packet capture time.
func (jt *JitterTracker) Add(tsval uint32, pktTime UnixNano) {
	if !jt.initialized {
		jt.tickRate = time.Millisecond
		jt.firstTSVal = tsval
		jt.firstPktTime = pktTime
		//log.Println("Jitter init")
		jt.initialized = true
		return
	}
	dt := pktTime.Sub(jt.firstPktTime)                // actual elapsed time.
	ticks := tsval - jt.firstTSVal                    // elapsed ticks.
	jt.valLR.Add(dt.Seconds(), float64(ticks)/1000.0) // Compute as if ticks are in milliseconds.

	t, p := jt.Adjust(tsval, pktTime)
	offset := t - p
	jt.ValCount++
	jt.ValOffsetSum += offset.Seconds()
	jt.ValOffsetSumSq += offset.Seconds() * offset.Seconds()
}

// Add adds a new offset between TSEcr and packet capture time to the jitter tracker.
// offset should be TSEcr - packet capture time.
// TODO - deal with TSEcr wrapping
func (jt *JitterTracker) AddEcho(tsecr uint32, pktTime UnixNano) {
	if !jt.initialized {
		return
	}
	dt := pktTime.Sub(jt.firstPktTime)                 // actual elapsed time.
	ticks := tsecr - jt.firstTSVal                     // elapsed ticks.
	jt.echoLR.Add(dt.Seconds(), float64(ticks)/1000.0) // Compute as if ticks are in milliseconds.

	t, p := jt.Adjust(tsecr, pktTime)
	offset := t - p
	jt.EchoCount++
	jt.EchoOffsetSum += offset.Seconds()
}

func (jt *JitterTracker) Mean() float64 {
	if jt.ValCount == 0 {
		return 0
	}
	return jt.ValOffsetSum / float64(jt.ValCount)
}

func (jt *JitterTracker) LRJitter() float64 {
	if jt.ValCount == 0 {
		return 0
	}
	return math.Sqrt(jt.valLR.YVar())
}

func (jt *JitterTracker) Jitter() float64 {
	if jt.ValCount == 0 {
		return 0
	}

	return math.Sqrt(jt.ValOffsetSumSq/float64(jt.ValCount) - jt.Mean()*jt.Mean())
}

func (jt *JitterTracker) Delay() float64 {
	if jt.EchoCount == 0 || jt.ValCount == 0 {
		return 0
	}
	return jt.ValOffsetSum/float64(jt.ValCount) - jt.EchoOffsetSum/float64(jt.EchoCount)
}

// State keeps track of the TCP State of one side of a connection.
// TODO - add histograms for Ack inter-arrival time.
type State struct {
	// These should be static characteristics
	StartTime   UnixNano // Convenience, for computing relative time for all other packets.
	SrcIP       net.IP
	SrcPort     layers.TCPPort // When this port is SrcPort, we update this stat struct.
	TTL         uint8
	WindowScale uint8

	// LastPacketTimeUsec uint64 // This comes from the IP layer.

	MSS    uint16
	Window uint16
	Limit  uint32 // The limit on data that can be sent, based on receiver window and ack data.

	//lastHeader *layers.TCP

	SeqTracker *Tracker // Track the seq/ack/sack related state.

	Stats StatsWrapper

	Jitter JitterTracker
}

func NewState(srcIP net.IP, srcPort layers.TCPPort) *State {
	return &State{SrcIP: srcIP, SrcPort: srcPort, SeqTracker: NewTracker(),
		Stats: StatsWrapper{TcpStats: TcpStats{OptionCounts: make([]int64, 16)}}}
}

// TODO - should only handle the earliest response for each value???
func (s *State) handleTimestamp(pktTime UnixNano, retransmit bool, isOutgoing bool, opt *tcpOption) {
	tsVal, tsEcr, err := opt.GetTimestamps()
	if err != nil {
		log.Println(err, "on timestamp option")
	}
	if isOutgoing && !retransmit {
		if tsVal != 0 {
			s.Jitter.Add(tsVal, pktTime)
			//log.Println(s.SrcPort, "TSVal", binary.BigEndian.Uint32(opt.OptionData[0:4]))
			// t, p := s.Jitter.Adjust(TSVal, pktTime)
			// delta := t - p
			// avgSeconds := s.Jitter.Mean()
			// log.Printf("%20v Avg: %10.4f T: %6.3f P: %6.3f Delta: %6.3f RTT: %8.4f Jitter: %8.4f at %v\n", s.SrcPort,
			// 	avgSeconds, float32(t)/1e9, float32(p)/1e9, float32(delta)/1e9, s.Jitter.Delay(), s.Jitter.Jitter(), pktTime)
		}
	} else if tsEcr != 0 {
		// TODO - what if !isOutgoing and retransmit?
		//log.Println(s.SrcPort, "TSEcr", binary.BigEndian.Uint32(opt.OptionData[4:8]))
		s.Jitter.AddEcho(tsEcr, pktTime)
	}
}

// ObsoleteOption handles all options, both incoming and outgoing.
// The relTime value is used for Timestamp analysis.
func (s *State) ObsoleteOption(port layers.TCPPort, retransmit bool, pTime UnixNano, opt *tcpOption) {
	// TODO test case for wrong index.
	optionType := opt.kind
	if optionType > 15 {
		//info.Printf("TCP Option has illegal option type %d", opt.kind)
		return
	}
	// TODO should some of these be counted in the opposite direction?
	s.Stats.Option(optionType)

	switch optionType {
	case layers.TCPOptionKindSACK:
		err := opt.processSACKs(s.SeqTracker.Sack, &s.Stats)
		if err != nil {
			log.Println(err, "on SACK option")
			return
		}

	case layers.TCPOptionKindMSS:
		s.MSS, _ = opt.GetMSS()
	case layers.TCPOptionKindTimestamps:
		s.handleTimestamp(pTime, retransmit, port == s.SrcPort, opt)

	case layers.TCPOptionKindWindowScale:
		s.WindowScale, _ = opt.GetWS()
	default:
	}
}

// Options2 handles all options, both incoming and outgoing.
// It operates on the raw option byte slice, so it's benchmark appears slower,
// but is actually faster.
func (s *State) Options2(port layers.TCPPort, retransmit bool, pTime UnixNano, optData []byte) error {
	// TODO test case for wrong index.

	for {
		var opt *tcpOption
		var err error
		optData, opt, err = NextOptionInPlace(optData)
		if err != nil {
			return err
		}
		if opt.kind == layers.TCPOptionKindEndList {
			break
		}
		// We need to process timestamp going both directions.
		if opt.kind == layers.TCPOptionKindTimestamps {
			s.handleTimestamp(pTime, retransmit, port == s.SrcPort, opt)
		}
		// All others, we handle only outgoing.
		if port == s.SrcPort {
			// Just count (all) the non-trivial options for the matching direction.
			s.Stats.Option(opt.kind)

			switch opt.kind {
			case layers.TCPOptionKindSACK:
				err := opt.processSACKs(s.SeqTracker.Sack, &s.Stats)
				if err != nil {
					log.Println(err, "on SACK option")
				}
			case layers.TCPOptionKindMSS:
				s.MSS, _ = opt.GetMSS()
			case layers.TCPOptionKindWindowScale:
				s.WindowScale, _ = opt.GetWS()
			default:
				// Do nothing for now.
			}
		}
		if len(optData) == 0 {
			break
		}
	}
	return nil
}

func (s *State) Update(count int, srcIP, dstIP net.IP, tcpLength uint16, tcp *TCPHeaderGo, optData []byte, pTime UnixNano) {
	dataLength := tcpLength - uint16(tcp.DataOffset)
	// pTime := ci.Timestamp
	var retransmit bool
	if s.SrcIP.Equal(srcIP) {
		if s.SrcPort != tcp.SrcPort {
			s.Stats.SrcPortErrors++
		}
		//window := int64(s.Window) << s.WindowScale
		//var inflight int32
		//info.Printf("Port:%20v packet:%d Seq:%10d Length:%5d SYN:%5v ACK:%5v", tcp.SrcPort, s.SeqTracker.packets, tcp.Seq, tcpLength, tcp.SYN, tcp.ACK)
		if _, retransmit = s.SeqTracker.Seq(count, pTime, tcp.SeqNum, dataLength, tcp.SYN() || tcp.FIN(), &s.Stats); retransmit {
			// TODO
		}
		// TODO handle error here?
		remaining, err := diff(s.Limit, s.SeqTracker.SendNext())
		if err != nil {
			//sn := s.SeqTracker.SendNext()
			//sparse500.Println("remaining diff err", s.Limit, sn)
		}
		if !tcp.SYN() {
			if remaining < 0 {
				// TODO: This is currently triggering more often than expected.
				// The stack should not send data beyond the window limit, which would have been
				// specified in the last ack.  But if pcaps did not capture the last ack, and
				// that ack increased the window, then this might be a valid send, and an
				// indication that we missed a packet in the capture.
				//q	sparse500.Println("Protocol violation, SendNext > Limit:", s.SrcPort, s.SeqTracker.SendNext(), s.Limit, s.SeqTracker.packets)
				s.Stats.SendNextExceededLimit++
			} else if remaining < int32(s.MSS) {
				//	sparse500.Println("Window limited", s.SrcPort, s.SeqTracker.packets, ": ", window, remaining, s.MSS)
			}
		}
		// s.LastPacketTimeUsec = uint64(pTime / 1000)
		if tcp.ECE() {
			s.Stats.ECECount++
		}
		//log.Printf("%5d: %2d.%6d %9d %20v %5v inflight: %6d / %6d %6d\n", count, pTime.Second(), pTime.Nanosecond()/1000, s.SeqTracker.sent, s.SrcPort, retransmit, inflight, window, remaining)
	} else if s.SrcIP.Equal(dstIP) {
		if s.SrcPort != tcp.DstPort {
			s.Stats.DstPortErrors++
		}
		// Process ACKs and SACKs from the other direction
		// Handle all options, including SACKs from other direction
		// TODO - should some of these be associated with the other direction?
		// for i := 0; i < len(options); i++ {
		// 	s.Option(tcp.SrcPort, retransmit, pTime, &options[i])
		// }
		if s.Window != tcp.Window {
			s.Stats.WindowChanges++
			s.Window = tcp.Window
		}
		if tcp.ACK() {
			_, delay := s.SeqTracker.Ack(count, pTime, tcp.AckNum, dataLength > 0, &s.Stats) // TODO
			if delay > 0 {
				//xxx BUG in sent?
				//	log.Printf("%5d: %2d.%6d %9d %20v Packet: %5d Delay: %8v\n", count, pTime.Second(), pTime.Nanosecond()/1000, s.SeqTracker.sent, s.SrcPort, pn, delay)
			}
			s.Limit = s.SeqTracker.sendUNA + uint32(s.Window)<<s.WindowScale
		}
	}
	// Handle options
	s.Options2(tcp.SrcPort, retransmit, pTime, optData)
}

func (s State) String() string {
	return fmt.Sprintf("[%v:%5d %d %12d/%10d/%10d  win:%5d sacks:%4d retrans:%4d ece:%4d]",
		s.SrcIP, s.SrcPort, s.Stats.TTLChanges, s.SeqTracker.SendNext(), s.SeqTracker.seq, s.SeqTracker.Acked(),
		s.Window, s.Stats.Sacks, s.Stats.RetransmitPackets, s.Stats.ECECount)
}
