// Package tcp provides tools to reconstruct tcp state from pcap files.
package tcp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"strings"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcapgo"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/go/logx"
)

/*
models for:
 * seq and acks
 * options, including timestamps and sack blocks
 * window size
 * connection characteristics, such as MSS, wscale

*/

var info = log.New(os.Stdout, "info: ", log.LstdFlags|log.Lshortfile)
var sparseLogger = log.New(os.Stdout, "sparse: ", log.LstdFlags|log.Lshortfile)
var sparse = logx.NewLogEvery(sparseLogger, 50*time.Millisecond)
var sparse2 = logx.NewLogEvery(sparseLogger, 50*time.Millisecond)

var ErrTrackerNotInitialized = fmt.Errorf("tracker not initialized")
var ErrInvalidDelta = fmt.Errorf("invalid delta")
var ErrInvalidSackBlock = fmt.Errorf("invalid sack block")
var ErrLateSackBlock = fmt.Errorf("sack block to left of ack")
var ErrNoIPLayer = fmt.Errorf("no IP layer")

// TODO - build a sackblock model, that consolidates new sack blocks into existing state.
type sackBlock struct {
	Left  uint32
	Right uint32
}

type StatsWrapper struct {
	schema.TcpStats
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

type seqInfo struct {
	count int
	pTime time.Time
}
type Tracker struct {
	initialized bool
	startTime   time.Time // Initial packet time
	packets     uint32    // Number of calls to Seq function
	seq         uint32    // The last sequence number observed, not counting retransmits
	synFin      uint32    // zero, one or two, depending on whether SYN and FIN have been sent

	sendUNA  uint32 // greatest observed ack
	acks     uint32 // number of acks (from other side)
	onlyAcks uint32 // Number of packets that only have ACKs, no data.
	acked    uint64 // bytes acked
	maxGap   int32  // Max observed gap between acked and NextSeq()

	sent      uint64      // actual bytes sent, including retransmits, but not SYN or FIN
	sacks     []sackBlock // keeps track of outstanding SACK blocks
	sackBytes uint64      // keeps track of total bytes reported missing in SACK blocks

	lastDataLength uint16 // Used to compute NextSeq()

	// This will get very large - one entry per packet.
	seqTimes map[uint32]seqInfo
}

func NewTracker() *Tracker {
	return &Tracker{seqTimes: make(map[uint32]seqInfo, 100)}
}

func (t *Tracker) Summary() string {
	return fmt.Sprintf("%5d packets, %5d/%5d acks w/data, %5d max gap\n",
		t.packets, t.acks-t.onlyAcks, t.acks, t.maxGap)
}

// SendNext returns the uint32 value of the expected next sequence number.
func (t *Tracker) SendNext() uint32 {
	return t.seq + uint32(t.lastDataLength) // wraps at 2^32
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

var badSeq = logx.NewLogEvery(sparseLogger, 100*time.Millisecond)
var badAck = logx.NewLogEvery(sparseLogger, 100*time.Millisecond)

// Seq updates the tracker based on an observed packet with sequence number seq and content size length.
// Initializes the tracker if it hasn't been initialized yet.
// Returns the bytes in flight (not including retransmits) and boolean indicator if this is a retransmit
func (t *Tracker) Seq(count int, pTime time.Time, clock uint32, length uint16, synFin bool, sw *StatsWrapper) (int32, bool) {
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
	inflight, err := diff(t.SendNext(), t.sendUNA)
	if err != nil {
		log.Println("inflight diff error", t.SendNext(), t.sendUNA)
	}

	// TODO handle errors
	delta, err := diff(clock, t.seq)
	if err != nil {
		sw.BadDeltas++
		badSeq.Printf("Bad seq %4X -> %4X\n", t.seq, clock)
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
		sparse.Printf("%d: Missing packet?  delta (%d) does not match last data size (%d)\n", t.packets, delta, t.lastDataLength) // VERBOSE
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
	t.seqTimes[clock] = seqInfo{count, pTime}

	gap, err := diff(t.seq, t.sendUNA)
	if err != nil {
		log.Println("gap diff error:", t.seq, t.sendUNA)
	}
	if gap > t.maxGap {
		t.maxGap = gap
	}

	inflight, err = diff(t.SendNext(), t.sendUNA)
	if err != nil {
		log.Println("inflight diff error:", t.SendNext(), t.sendUNA)
	}

	return inflight, false
}

func (t *Tracker) Acked() uint64 {
	return t.acked
}

// Ack updates the tracker based on an observed ack value.
// Returns the time observed by the packet capture since the correponding sequence number was sent.
func (t *Tracker) Ack(count int, pTime time.Time, clock uint32, withData bool, sw *StatsWrapper) (int, time.Duration) {
	if !t.initialized {
		sw.OtherErrors++
		info.Printf("PKT: %d Ack called before Seq", count)
	}
	delta, err := diff(clock, t.sendUNA)
	if err != nil {
		sw.BadDeltas++
		badAck.Printf("Bad ack %4X -> %4X\n", t.sendUNA, clock)
		// TODO should this sometimes update the sendUNA, or always?
		t.sendUNA = clock // Let's assume we missed many many packets.
		return 0, 0
	}
	if delta > 0 {
		t.acked += uint64(delta)
		t.acks++
	}
	if !withData {
		t.onlyAcks++
	}
	defer func() { t.sendUNA = clock }()
	si, ok := t.seqTimes[clock]
	if ok {
		// TODO should we keep the entry but mark it as acked?  Or keep a limited cache?
		delete(t.seqTimes, clock)
		return si.count, pTime.Sub(si.pTime)
	} else {
		sparse.Printf("Ack out of order? %7d (%7d) %7d..%7d", t.sendUNA, clock, t.seq, t.SendNext())
		return 0, 0
	}
}

func (t *Tracker) SendUNA() uint32 {
	return t.sendUNA
}

// Check checks that a sack block is consistent with the current window.
func (t *Tracker) checkSack(sb sackBlock) error {
	// block should ALWAYS have positive width
	if width, err := diff(sb.Right, sb.Left); err != nil || width <= 0 {
		sparse.Println(ErrInvalidSackBlock, err, width, t.Acked())
		return ErrInvalidSackBlock
	}
	// block Right should ALWAYS be to the left of NextSeq()
	// If not, we may have missed recording a packet!
	if overlap, err := diff(t.SendNext(), sb.Right); err != nil || overlap < 0 {
		sparse.Println(ErrInvalidSackBlock, err, overlap, t.Acked())
		return ErrInvalidSackBlock
	}
	// Left should be to the right of ack
	if overlap, err := diff(sb.Left, t.sendUNA); err != nil || overlap < 0 {
		// These often correspond to packets that show up as spurious retransmits in WireShark.
		sparse.Println(ErrLateSackBlock, err, overlap, t.Acked())
		return ErrLateSackBlock
	}
	return nil
}

// Sack updates the counter with sack information (from other direction)
func (t *Tracker) Sack(sb sackBlock, sw *StatsWrapper) {
	if !t.initialized {
		sw.OtherErrors++
		info.Println(ErrTrackerNotInitialized)
	}
	// Auto gen code
	if err := t.checkSack(sb); err != nil {
		sw.BadSacks++
		sparse.Println(ErrInvalidSackBlock, t.sendUNA, sb, t.SendNext())
	}
	//t.sacks = append(t.sacks, block)
	t.sackBytes += uint64(sb.Right - sb.Left)
}

// JitterTracker TODO
//  Likely need to look for the first occurance, or perhaps the lowest delay occurance, of each TSVal,
// and the corresponding first occurance of TSEcr.
type JitterTracker struct {
	initialized  bool
	firstTSVal   uint32
	firstPktTime time.Time
	tickRate     time.Duration // Interval between ticks.  For server side this is always 1 msec.

	ValCount       int
	ValOffsetSum   float64
	ValOffsetSumSq float64

	EchoCount       int
	EchoOffsetSum   float64
	EchoOffsetSumSq float64
}

// Adjust attempts to adjust the TSVal and pktTime to interval since the first reported packet.
// The TSVal is adjusted based on the inferred tick rate.
func (j *JitterTracker) Adjust(tsval uint32, pktTime time.Time) (time.Duration, time.Duration) {
	return time.Duration(tsval-j.firstTSVal) * j.tickRate, pktTime.Sub(j.firstPktTime)
}

// Add adds a new offset between TSVal and packet capture time to the jitter tracker.
// offset should be TSVal - packet capture time.
func (jt *JitterTracker) Add(tsval uint32, pktTime time.Time) {
	if !jt.initialized {
		jt.tickRate = time.Millisecond
		jt.firstTSVal = tsval
		jt.firstPktTime = pktTime
		log.Println("Jitter init")
		jt.initialized = true
		return
	}
	t, p := jt.Adjust(tsval, pktTime)
	offset := t - p
	jt.ValCount++
	jt.ValOffsetSum += offset.Seconds()
	jt.ValOffsetSumSq += offset.Seconds() * offset.Seconds()
}

// Add adds a new offset between TSEcr and packet capture time to the jitter tracker.
// offset should be TSEcr - packet capture time.
// TODO - deal with TSEcr wrapping
func (jt *JitterTracker) AddEcho(tsecr uint32, pktTime time.Time) {
	if !jt.initialized {
		return
	}
	t, p := jt.Adjust(tsecr, pktTime)
	offset := t - p
	jt.EchoCount++
	jt.EchoOffsetSum += offset.Seconds()
	jt.EchoOffsetSumSq += offset.Seconds() * offset.Seconds()
}

func (jt *JitterTracker) Mean() float64 {
	if jt.ValCount == 0 {
		return 0
	}
	return jt.ValOffsetSum / float64(jt.ValCount)
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

// state keeps track of the TCP state of one side of a connection.
// TODO - add histograms for Ack inter-arrival time.
type state struct {
	// These should be static characteristics
	StartTime   time.Time // Convenience, for computing relative time for all other packets.
	SrcIP       net.IP
	SrcPort     layers.TCPPort // When this port is SrcPort, we update this stat struct.
	TTL         uint8
	WindowScale uint8

	LastPacketTimeUsec uint64 // This comes from the IP layer.

	MSS    uint16
	Window uint16
	Limit  uint32 // The limit on data that can be sent, based on receiver window and ack data.

	//lastHeader *layers.TCP

	SeqTracker *Tracker // Track the seq/ack/sack related state.

	Stats StatsWrapper

	Jitter JitterTracker
}

func NewState() *state {
	return &state{SeqTracker: NewTracker(),
		Stats: StatsWrapper{TcpStats: schema.TcpStats{OptionCounts: make([]int64, 16)}}}
}

func (s *state) handleTimestamp(pktTime time.Time, retransmit bool, isOutgoing bool, opt layers.TCPOption) {
	// These are arbitrary time offsets, but we will be
	TSVal := binary.BigEndian.Uint32(opt.OptionData[0:4])
	TSEcr := binary.BigEndian.Uint32(opt.OptionData[4:8])

	if isOutgoing && !retransmit {
		if TSVal != 0 {
			s.Jitter.Add(TSVal, pktTime)
			//log.Println(s.SrcPort, "TSVal", binary.BigEndian.Uint32(opt.OptionData[0:4]))
			t, p := s.Jitter.Adjust(TSVal, pktTime)
			delta := t - p
			avgSeconds := s.Jitter.Mean()
			log.Printf("%20v Avg: %10.4f T: %6.3f P: %6.3f Delta: %6.3f RTT: %8.4f Jitter: %8.4f at %v\n", s.SrcPort,
				avgSeconds, float32(t)/1e9, float32(p)/1e9, float32(delta)/1e9, s.Jitter.Delay(), s.Jitter.Jitter(), pktTime)
		}
	} else if TSEcr != 0 {
		//log.Println(s.SrcPort, "TSEcr", binary.BigEndian.Uint32(opt.OptionData[4:8]))
		s.Jitter.AddEcho(TSEcr, pktTime)
	}
}

// Option handles all options, both incoming and outgoing.
// The relTime value is used for Timestamp analysis.
func (s *state) Option(port layers.TCPPort, retransmit bool, pTime time.Time, opt layers.TCPOption) {
	// TODO test case for wrong index.
	optionType := opt.OptionType
	if optionType > 15 {
		info.Printf("TCP Option has illegal option type %d", opt.OptionType)
		return
	}
	// TODO should some of these be counted in the opposite direction?
	s.Stats.Option(optionType)

	switch optionType {
	case layers.TCPOptionKindSACK:
		data := opt.OptionData
		sacks := make([]sackBlock, len(data)/8)
		binary.Read(bytes.NewReader(data), binary.BigEndian, &sacks)
		for _, block := range sacks {
			s.SeqTracker.Sack(block, &s.Stats)
		}
	case layers.TCPOptionKindMSS:
		if len(opt.OptionData) != 2 {
			info.Println("Invalid MSS option length", len(opt.OptionData))
		} else {
			s.MSS = binary.BigEndian.Uint16(opt.OptionData)
		}
	case layers.TCPOptionKindTimestamps:
		s.handleTimestamp(pTime, retransmit, port == s.SrcPort, opt)

	case layers.TCPOptionKindWindowScale:
		if len(opt.OptionData) != 1 {
			info.Println("Invalid WindowScale option length", len(opt.OptionData))
		} else {
			// TODO should this change after initialization?
			sparse.Printf("%v WindowScale change %d -> %d\n", port, s.WindowScale, opt.OptionData[0])
			s.WindowScale = opt.OptionData[0]
		}
	default:
	}
}

func (s *state) Update(count int, srcIP, dstIP net.IP, tcpLength uint16, tcp *layers.TCP, ci gopacket.CaptureInfo) {
	dataLength := tcpLength - uint16(4*tcp.DataOffset)
	pTime := ci.Timestamp
	var retransmit bool
	if s.SrcIP.Equal(srcIP) {
		if s.SrcPort != tcp.SrcPort {
			s.Stats.SrcPortErrors++
		}
		window := int64(s.Window) << s.WindowScale
		//var inflight int32
		//info.Printf("Port:%20v packet:%d Seq:%10d Length:%5d SYN:%5v ACK:%5v", tcp.SrcPort, s.SeqTracker.packets, tcp.Seq, tcpLength, tcp.SYN, tcp.ACK)
		if _, retransmit = s.SeqTracker.Seq(count, pTime, tcp.Seq, dataLength, tcp.SYN || tcp.FIN, &s.Stats); retransmit {
			// TODO
		}
		// TODO handle error here?
		remaining, err := diff(s.Limit, s.SeqTracker.SendNext())
		if err != nil {
			log.Println("remaining diff err", s.Limit, s.SeqTracker.SendNext())
		}
		if !tcp.SYN {
			if remaining < 0 {
				// TODO: This is currently triggering more often than expected.
				// The stack should not send data beyond the window limit, which would have been
				// specified in the last ack.  But if pcaps did not capture the last ack, and
				// that ack increased the window, then this might be a valid send, and an
				// indication that we missed a packet in the capture.
				sparse.Println("Protocol violation, SendNext > Limit:", s.SrcPort, s.SeqTracker.SendNext(), s.Limit, s.SeqTracker.packets)
				s.Stats.SendNextExceededLimit++
			} else if remaining < int32(s.MSS) {
				sparse.Println("Window limited", s.SrcPort, s.SeqTracker.packets, ": ", window, remaining, s.MSS)
			}
		}
		s.LastPacketTimeUsec = uint64(ci.Timestamp.UnixNano() / 1000)
		if tcp.ECE {
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
		for i := 0; i < len(tcp.Options); i++ {
			s.Option(tcp.SrcPort, retransmit, pTime, tcp.Options[i])
		}
		if s.Window != tcp.Window {
			s.Stats.WindowChanges++
			s.Window = tcp.Window
		}
		if tcp.ACK {
			_, delay := s.SeqTracker.Ack(count, pTime, tcp.Ack, dataLength > 0, &s.Stats) // TODO
			if delay > 0 {
				//xxx BUG in sent?
				//	log.Printf("%5d: %2d.%6d %9d %20v Packet: %5d Delay: %8v\n", count, pTime.Second(), pTime.Nanosecond()/1000, s.SeqTracker.sent, s.SrcPort, pn, delay)
			}
			s.Limit = s.SeqTracker.sendUNA + uint32(s.Window)<<s.WindowScale
		}
	}
	// Handle options
	for _, opt := range tcp.Options {
		s.Option(tcp.SrcPort, retransmit, pTime, opt)
	}

}

func (s state) String() string {
	return fmt.Sprintf("[%v:%5d %d %12d/%10d/%10d %8d win:%5d sacks:%4d retrans:%4d ece:%4d]", s.SrcIP, s.SrcPort, s.Stats.TTLChanges, s.SeqTracker.SendNext(), s.SeqTracker.seq, s.SeqTracker.Acked(), s.LastPacketTimeUsec%10000000, s.Window, s.Stats.Sacks, s.Stats.RetransmitPackets, s.Stats.ECECount)
}

type Parser struct {
	LeftState  *state // This is the state associated with the source of the first packet
	RightState *state // This is the state associated with the destination of the first packet
}

func NewParser() *Parser {
	return &Parser{
		// TODO Consider initializing these when the Syn and SynAck are seen.
		LeftState:  NewState(),
		RightState: NewState(),
	}
}

func (p *Parser) tcpLayer(srcIP, dstIP net.IP, tcp *layers.TCP, ci gopacket.CaptureInfo, tcpLength uint16, alpha *schema.AlphaFields) error {
	// Special case handling for first two packets.
	switch alpha.Packets {
	case 0:
		p.LeftState.StartTime = ci.Timestamp
		p.RightState.StartTime = ci.Timestamp
		p.LeftState.SrcPort = tcp.SrcPort
		p.RightState.SrcPort = tcp.DstPort
	case 1:
		// TODO - consider checking against IP address, too.
		if p.RightState.SrcPort != tcp.SrcPort || !tcp.ACK {
			// Use log for advisory/info logging.
			log.Println("Unexpected second packet", p.LeftState, p.RightState, tcp.DstPort, tcp.ACK)
		}
	default:
	}

	// Update both state structs.
	p.LeftState.Update(int(alpha.Packets), srcIP, dstIP, tcpLength, tcp, ci)
	p.RightState.Update(int(alpha.Packets), srcIP, dstIP, tcpLength, tcp, ci)

	if tcp.SYN {
		if tcp.ACK {
			alpha.SynAckTime = ci.Timestamp
			alpha.SynAckPacket = alpha.Packets
		} else {
			alpha.SynTime = ci.Timestamp
			alpha.SynPacket = alpha.Packets
		}
	}
	return nil
}

func extractIPFields(packet gopacket.Packet) (srcIP, dstIP net.IP, TTL uint8, tcpLength uint16, err error) {
	if ipLayer := packet.Layer(layers.LayerTypeIPv4); ipLayer != nil {
		ip, _ := ipLayer.(*layers.IPv4)
		// For IPv4, the TTL length is the ip.Length adjusted for the header length.
		return ip.SrcIP, ip.DstIP, ip.TTL, ip.Length - uint16(4*ip.IHL), nil
	} else if ipLayer := packet.Layer(layers.LayerTypeIPv6); ipLayer != nil {
		ip, _ := ipLayer.(*layers.IPv6)
		// In IPv6, the Length field is the payload length.
		return ip.SrcIP, ip.DstIP, ip.HopLimit, ip.Length, nil
	} else {
		return nil, nil, 0, 0, ErrNoIPLayer
	}
}

type packet struct {
	ci  gopacket.CaptureInfo
	pkt gopacket.Packet
	err error
}

type packetSummary struct {
	srcIP, dstIP net.IP
	tcpLength    uint16
	pTime        time.Time
	tsVal        uint32
	tsEcr        uint32
}

func (p *packet) GetTimestamps() (summary packetSummary, err error) {
	srcIP, dstIP, _, tcpLength, err := extractIPFields(p.pkt)
	summary = packetSummary{srcIP, dstIP, tcpLength, p.ci.Timestamp, 0, 0}
	if err != nil {
		return
	}

	err = fmt.Errorf("No timestamp field")
	if tcpLayer := p.pkt.Layer(layers.LayerTypeTCP); tcpLayer != nil {
		tcp, _ := tcpLayer.(*layers.TCP)
		for _, opt := range tcp.Options {
			if opt.OptionType == layers.TCPOptionKindTimestamps {
				summary.tsVal = binary.BigEndian.Uint32(opt.OptionData[0:4])
				summary.tsEcr = binary.BigEndian.Uint32(opt.OptionData[4:8])
				err = nil
				break
			}
		}
	}
	return
}

// Parse parses an entire pcap file.
func (p *Parser) Parse(data []byte) (*schema.AlphaFields, error) {
	pcap, err := pcapgo.NewReader(strings.NewReader(string(data)))
	if err != nil {
		info.Print(err)
		return nil, err
	}

	packets := make([]*packet, 0, 1000)

	start := time.Now()
	for data, ci, err := pcap.ZeroCopyReadPacketData(); err == nil; data, ci, err = pcap.ReadPacketData() {
		// Decode a packet
		pkt := gopacket.NewPacket(data, layers.LayerTypeEthernet, gopacket.DecodeOptions{
			Lazy:                     true,
			NoCopy:                   true,
			SkipDecodeRecovery:       true,
			DecodeStreamsAsDatagrams: false,
		})
		packets = append(packets, &packet{ci: ci, pkt: pkt, err: err})
	}
	mid := time.Now()

	final := make(map[string]packetSummary, 2)

	//last := len(packets) - 1
	for i := len(packets) - 1; i >= 0; i-- {
		summary, err := packets[i].GetTimestamps()
		if err == nil {
			if _, ok := final[summary.srcIP.String()]; !ok {
				final[summary.srcIP.String()] = summary
				if len(final) == 2 {
					log.Println("Found two IPs", final)
					break
				}
			}
		}
		if len(packets)-i > 100 {
			break // Give up if we can't find two IPs in the last 100 packets.
		}
	}

	// This is used to keep track of some of the TCP state.
	// TODO replace this with local variables, and copy at the end.
	alpha := schema.AlphaFields{}
	//var firstTimestamp time.Time
	for _, rec := range packets {
		packet := rec.pkt
		if packet.ErrorLayer() != nil {
			sparse.Printf("Error decoding packet: %v", packet.ErrorLayer().Error()) // Somewhat VERBOSE
			continue
		}

		srcIP, dstIP, ttl, tcpLength, err := extractIPFields(packet)
		if err != nil {
			return nil, err
		}

		switch alpha.Packets {
		case 0:
			//firstTimestamp = ci.Timestamp
			p.LeftState.SrcIP = srcIP
			p.RightState.SrcIP = dstIP
			p.LeftState.TTL = ttl
		case 1:
			p.RightState.TTL = ttl
			fallthrough
		default:
			//			log.Println(ci.Timestamp.Sub(firstTimestamp), srcIP)

			if p.LeftState.SrcIP.Equal(srcIP) {
				if !p.RightState.SrcIP.Equal(dstIP) {
					alpha.IPAddrErrors++
				}
				if p.LeftState.TTL != ttl {
					p.LeftState.Stats.TTLChanges++
				}
			} else if p.RightState.SrcIP.Equal(srcIP) {
				if !p.LeftState.SrcIP.Equal(dstIP) {
					alpha.IPAddrErrors++
				}
				if p.RightState.TTL != ttl {
					p.RightState.Stats.TTLChanges++
				}
			} else {
				alpha.IPAddrErrors++
			}
		}

		// Get the TCP layer from this packet
		if tcpLayer := packet.Layer(layers.LayerTypeTCP); tcpLayer != nil {
			tcp, _ := tcpLayer.(*layers.TCP)
			p.tcpLayer(srcIP, dstIP, tcp, rec.ci, tcpLength, &alpha)
		} else {
			alpha.WithoutTCPLayer++
		}
		alpha.Packets++
	}
	end := time.Now()

	log.Println(mid.Sub(start), end.Sub(mid))

	info.Printf("%20s: %v", p.LeftState.SrcPort, p.LeftState.SeqTracker.Summary())
	info.Printf("%20s: %v", p.RightState.SrcPort, p.RightState.SeqTracker.Summary())

	alpha.LeftStats = p.LeftState.Stats.TcpStats
	alpha.RightStats = p.RightState.Stats.TcpStats

	return &alpha, nil
}
