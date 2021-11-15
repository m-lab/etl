// Package tcp provides tools to reconstruct tcp state from pcap files.
package tcp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
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

type Tracker struct {
	initialized bool
	errors      uint32 // Number of errors encountered while parsing
	packets     uint32 // Number of calls to Seq function
	seq         uint32 // The last sequence number observed, not counting retransmits
	synFin      uint32 // zero, one or two, depending on whether SYN and FIN were sent
	sent        uint64 // actual bytes sent, including retransmits, but not SYN or FIN

	sendUNA  uint32 // greatest observed ack
	acks     uint32 // number of acks (from other side)
	onlyAcks uint32 // Number of packets that only have ACKs, no data.
	acked    uint64 // bytes acked
	maxGap   int32  // Max observed gap between acked and NextSeq()

	// sacks keeps track of outstanding SACK blocks
	sacks     []sackBlock
	sackBytes uint64 // keeps track of total bytes reported missing in SACK blocks

	lastDataLength uint16 // Used to compute NextSeq()

	// These stats are useful for bigquery summaries.
	schema.TcpStats
}

func (t *Tracker) Summary() string {
	return fmt.Sprintf("%8d bytes sent, %5d packets, %5d/%5d acks w/data, %5d max gap\n",
		t.Sent(), t.packets, t.acks-t.onlyAcks, t.acks, t.maxGap)
}

// Errors returns the number of errors encountered while parsing.
func (t *Tracker) Stats() schema.TcpStats {
	return t.TcpStats
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
func (t *Tracker) Seq(clock uint32, length uint16, synFin bool) (int32, bool) {
	t.packets++ // Some of these may be retransmits.

	if !t.initialized {
		t.seq = clock
		t.sendUNA = clock // nothing acked so far
		t.initialized = true
	}
	// Use this unless we are sending new data.
	// TODO - correct this for sum of sizes of sack block scoreboard.
	inflight, err := diff(t.SendNext(), t.sendUNA)
	if err != nil {
		log.Println("inflight diff error", t.SendNext(), t.sendUNA)
	}

	// TODO handle errors
	delta, err := diff(clock, t.seq)
	if err != nil {
		t.BadDeltas++
		badSeq.Printf("Bad seq %4X -> %4X\n", t.seq, clock)
		return inflight, false
	}
	if delta < 0 {
		// DO NOT update w.seq or w.lastDataLength, as this is a retransmit
		t.sent += uint64(length)
		t.RetransmitPackets++
		t.RetransmitBytes += int64(length)
		return inflight, true
	}
	// delta is non-negative (not a retransmit)
	if delta != int32(t.lastDataLength) {
		t.MissingPackets++
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

	gap, err := diff(t.seq, t.sendUNA)
	if err != nil {
		log.Println("gap diff error:", t.seq, t.sendUNA)
	}
	if gap > t.maxGap {
		t.maxGap = gap
		//info.Printf("%8p - %5d: MaxGap = %d\n", t, t.packets, gap)
	}

	inflight, err = diff(t.SendNext(), t.sendUNA)
	if err != nil {
		log.Println("inflight diff error:", t.SendNext(), t.sendUNA)
	}

	return inflight, false
}

// Total bytes transmitted, not including retransmits.
// TODO should this include the 1 byte SYN?
func (t *Tracker) Sent() uint64 {
	return t.sent - uint64(t.RetransmitBytes)
}

func (t *Tracker) Acked() uint64 {
	return t.acked
}

func (t *Tracker) Ack(clock uint32, withData bool) {
	if !t.initialized {
		t.errors++
		info.Print("Ack called before Seq")
	}
	delta, err := diff(clock, t.sendUNA)
	if err != nil {
		t.BadDeltas++
		badAck.Printf("Bad ack %4X -> %4X\n", t.sendUNA, clock)
		return
	}
	if delta > 0 {
		t.acked += uint64(delta)
		t.acks++
	}
	if !withData {
		t.onlyAcks++
	}
	t.sendUNA = clock
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
func (t *Tracker) Sack(sb sackBlock) {
	if !t.initialized {
		t.errors++
		info.Println(ErrTrackerNotInitialized)
	}
	// Auto gen code
	if err := t.checkSack(sb); err != nil {
		t.BadSacks++
		sparse.Println(ErrInvalidSackBlock, t.sendUNA, sb, t.SendNext())
	}
	//t.sacks = append(t.sacks, block)
	t.sackBytes += uint64(sb.Right - sb.Left)
}

type endpoint struct {
	SrcIP net.IP
	TTL   uint8

	SrcPort layers.TCPPort // When this port is SrcPort, we update this stat struct.
}

type stats struct {
	ECECount   uint64
	TTLChanges uint64 // Observed number of TTL values that don't match first IP header.
}

// state keep track of the TCP state of one side of a connection.
// TODO - add histograms for Ack inter-arrival time.
type state struct {
	endpoint // TODO
	stats    // TODO

	// TODO move these to SeqTracker, so that we can observe whether we are window limited.
	WindowScale uint8
	Window      uint16
	Limit       uint32 // The limit on data that can be sent, based on receiver window and ack data.
	MSS         uint16

	lastHeader *layers.TCP

	SeqTracker         Tracker // This should match the previous value of Sent.
	Sacks              uint64
	LastPacketTimeUsec uint64
}

func NewState() *state {
	return &state{SeqTracker: Tracker{TcpStats: schema.TcpStats{OptionCounts: make([]int64, 16)}}}
}

func (s *state) Option(port layers.TCPPort, opt layers.TCPOption) {
	// TODO test case for wrong index.
	optionType := opt.OptionType
	if optionType > 15 {
		info.Printf("TCP Option has illegal option type %d", opt.OptionType)
		return
	}
	// TODO should some of these be counted in the opposite direction?
	s.SeqTracker.OptionCounts[optionType]++

	switch optionType {
	case layers.TCPOptionKindSACK:
		data := opt.OptionData
		sacks := make([]sackBlock, len(data)/8)
		binary.Read(bytes.NewReader(data), binary.BigEndian, &sacks)
		for _, block := range sacks {
			s.SeqTracker.Sack(block)
		}
	case layers.TCPOptionKindMSS:
		if len(opt.OptionData) != 2 {
			info.Println("Invalid MSS option length", len(opt.OptionData))
		} else {
			s.MSS = binary.BigEndian.Uint16(opt.OptionData)
		}
	case layers.TCPOptionKindTimestamps:
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

func (s *state) Update(tcpLength uint16, tcp *layers.TCP, ci gopacket.CaptureInfo) {
	dataLength := tcpLength - uint16(4*tcp.DataOffset)
	if tcp.SrcPort == s.SrcPort {
		//info.Printf("Port:%20v packet:%d Seq:%10d Length:%5d SYN:%5v ACK:%5v", tcp.SrcPort, s.SeqTracker.packets, tcp.Seq, tcpLength, tcp.SYN, tcp.ACK)
		if _, retrans := s.SeqTracker.Seq(tcp.Seq, dataLength, tcp.SYN || tcp.FIN); retrans {
			// TODO
		}
		if !tcp.SYN {
			if remaining, err := diff(s.Limit, s.SeqTracker.SendNext()); remaining < 0 {
				if err != nil {
					log.Println("remaining diff err", s.Limit, s.SeqTracker.SendNext())
				}
				// TODO: This is currently triggering more often than expected.
				// The stack should not send data beyond the window limit, which would have been
				// specified in the last ack.  But if pcaps did not capture the last ack, and
				// that ack increased the window, then this might be a valid send, and an
				// indication that we missed a packet in the capture.
				log.Println("Protocol violation, SendNext > Limit:", s.SrcPort, s.SeqTracker.SendNext(), s.Limit, s.SeqTracker.packets)
				s.SeqTracker.TcpStats.SendNextExceededLimit++
			} else if remaining < int32(s.MSS) {
				sparse.Println("Window limited", s.SrcPort, s.SeqTracker.packets, ": ", int64(s.Window)<<s.WindowScale, remaining, s.MSS)
			}
		}
		s.LastPacketTimeUsec = uint64(ci.Timestamp.UnixNano() / 1000)
		if tcp.ECE {
			s.ECECount++
		}
	} else {
		// Process ACKs and SACKs from the other direction
		// Handle all options, including SACKs from other direction
		// TODO - should some of these be associated with the other direction?
		for i := 0; i < len(tcp.Options); i++ {
			s.Option(tcp.SrcPort, tcp.Options[i])
		}
		if s.Window != tcp.Window {
			// VERY VERBOSE
			sparse2.Printf("Remote %v window changed from %d to %d\n", tcp.SrcPort, uint32(s.Window)<<s.WindowScale, uint32(tcp.Window)<<s.WindowScale)
			s.Window = tcp.Window
		}
		if tcp.ACK {
			s.SeqTracker.Ack(tcp.Ack, dataLength > 0) // TODO
			s.Limit = s.SeqTracker.sendUNA + uint32(s.Window)<<s.WindowScale
		}
	}
	// Handle options
	for _, opt := range tcp.Options {
		s.Option(tcp.SrcPort, opt)
	}

}

func (s state) String() string {
	return fmt.Sprintf("[%v:%5d %d %12d/%10d/%10d %8d win:%5d sacks:%4d retrans:%4d ece:%4d]", s.SrcIP, s.SrcPort, s.TTLChanges, s.SeqTracker.SendNext(), s.SeqTracker.seq, s.SeqTracker.Acked(), s.LastPacketTimeUsec%10000000, s.Window, s.Sacks, s.SeqTracker.RetransmitPackets, s.ECECount)
}

type Parser struct {
	SrcIP   net.IP
	DstIP   net.IP
	SrcPort layers.TCPPort
	DstPort layers.TCPPort

	LeftState  *state
	RightState *state
}

func NewParser() *Parser {
	return &Parser{
		// TODO Consider initializing these when the Syn and SynAck are seen.
		LeftState:  NewState(),
		RightState: NewState(),
	}
}

func (p *Parser) tcpLayer(tcp *layers.TCP, ci gopacket.CaptureInfo, tcpLength uint16, alpha *schema.AlphaFields) error {
	// Special case handling for first two packets.
	switch alpha.Packets {
	case 0:
		if tcp.SrcPort == 443 || tcp.DstPort == 443 {
			// info.Println(p.GetUUID(testName))
		}
		p.LeftState.SrcPort = tcp.SrcPort
		p.RightState.SrcPort = tcp.DstPort
	case 1:
		if p.RightState.SrcPort != tcp.SrcPort || !tcp.ACK {
			// Use log for advisory/info logging.
			info.Println("Bad sack block", p.RightState, p.LeftState, tcp.DstPort, tcp.ACK)
		}
	default:
	}

	// Update both state structs.
	p.LeftState.Update(tcpLength, tcp, ci)
	p.RightState.Update(tcpLength, tcp, ci)

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

func extractIPFields(packet gopacket.Packet) (srcIP net.IP, TTL uint8, tcpLength uint16, err error) {
	if ipLayer := packet.Layer(layers.LayerTypeIPv4); ipLayer != nil {
		ip, _ := ipLayer.(*layers.IPv4)
		return ip.SrcIP, ip.TTL, ip.Length - uint16(4*ip.IHL), nil
	} else if ipLayer := packet.Layer(layers.LayerTypeIPv6); ipLayer != nil {
		ip, _ := ipLayer.(*layers.IPv6)
		// In IPv6, the Length field is the payload length.
		return ip.SrcIP, ip.HopLimit, ip.Length, nil
	} else {
		return nil, 0, 0, ErrNoIPLayer
	}
}

// Parse parses an entire pcap file.
func (p *Parser) Parse(data []byte) (*schema.AlphaFields, error) {
	pcap, err := pcapgo.NewReader(strings.NewReader(string(data)))
	if err != nil {
		info.Print(err)
		return nil, err
	}

	// This is used to keep track of some of the TCP state.
	// TODO replace this with local variables, and copy at the end.
	alpha := schema.AlphaFields{}

	for data, ci, err := pcap.ReadPacketData(); err == nil; data, ci, err = pcap.ReadPacketData() {
		// Decode a packet
		packet := gopacket.NewPacket(data, layers.LayerTypeEthernet, gopacket.DecodeOptions{
			Lazy:                     true,
			NoCopy:                   true,
			SkipDecodeRecovery:       true,
			DecodeStreamsAsDatagrams: false,
		})

		if packet.ErrorLayer() != nil {
			sparse.Printf("Error decoding packet: %v", packet.ErrorLayer().Error()) // Somewhat VERBOSE
			continue
		}

		srcIP, ttl, tcpLength, err := extractIPFields(packet)
		if err != nil {
			return nil, err
		}

		switch alpha.Packets {
		case 0:
			p.LeftState.SrcIP = srcIP
			p.LeftState.TTL = ttl
		case 1:
			p.RightState.SrcIP = srcIP
			p.RightState.TTL = ttl
		default:
			if p.LeftState.SrcIP.Equal(srcIP) {
				if p.LeftState.TTL != ttl {
					alpha.TTLChanges++
					p.LeftState.TTLChanges++
				}
			} else if p.RightState.SrcIP.Equal(srcIP) {
				if p.RightState.TTL != ttl {
					alpha.TTLChanges++
					p.RightState.TTLChanges++
				}
			} else {
				alpha.IPChanges++
			}
		}

		// Get the TCP layer from this packet
		if tcpLayer := packet.Layer(layers.LayerTypeTCP); tcpLayer != nil {
			tcp, _ := tcpLayer.(*layers.TCP)
			p.tcpLayer(tcp, ci, tcpLength, &alpha)
		}
		alpha.Packets++
	}

	info.Printf("%20s: %v", p.LeftState.SrcPort, p.LeftState.SeqTracker.Summary())
	info.Printf("%20s: %v", p.RightState.SrcPort, p.RightState.SeqTracker.Summary())

	alpha.LeftStats = p.LeftState.SeqTracker.TcpStats
	alpha.RightStats = p.RightState.SeqTracker.TcpStats

	return &alpha, nil
}
