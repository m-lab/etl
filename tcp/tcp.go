// Package tcp provides tools to reconstruct tcp state from pcap files.
package tcp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcapgo"
	"github.com/m-lab/etl/schema"
)

/*
models for:
 * seq and acks
 * options, including timestamps and sack blocks
 * window size
 * connection characteristics, such as MSS, wscale

*/

var ErrTrackerNotInitialized = fmt.Errorf("tracker not initialized")
var ErrInvalidDelta = fmt.Errorf("invalid delta")
var ErrInvalidSackBlock = fmt.Errorf("invalid sack block")
var ErrLateSackBlock = fmt.Errorf("sack block to left of ack")

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
	retransmits uint64 // bytes retransmitted

	ack      uint32 // last observed ack
	acks     uint32 // number of acks (from other side)
	onlyAcks uint32 // Number of packets that only have ACKs, no data.
	acked    uint64 // bytes acked
	maxGap   int32  // Max observed gap between acked and NextSeq()

	// sacks keeps track of outstanding SACK blocks
	sacks     []sackBlock
	sackBytes uint64 // keeps track of total bytes reported missing in SACK blocks

	lastDataLength uint16 // Used to compute NextSeq()
}

func (t *Tracker) Summary() string {
	return fmt.Sprintf("%8d bytes sent, %5d packets, %5d/%5d acks w/data, %5d max gap\n",
		t.Sent(), t.packets, t.acks-t.onlyAcks, t.acks, t.maxGap)
}

// Errors returns the number of errors encountered while parsing.
func (t *Tracker) Errors() uint32 {
	return t.errors
}

// NextSeq returns the uint32 value of the expected next sequence number.
func (t *Tracker) NextSeq() uint32 {
	return t.seq + uint32(t.lastDataLength) // wraps at 2^32
}

// ByteCount returns the number of bytes sent so far, not including retransmits
func (t *Tracker) ByteCount() uint64 {
	return t.sent
}

func diff(clock uint32, previous uint32) (int32, error) {
	delta := int32(clock - previous)
	if !(-1<<30 < delta && delta < 1<<30) {
		fmt.Printf("invalid sequence delta %d->%d (%d)", previous, clock, delta)
		return delta, ErrInvalidDelta
	}
	return delta, nil
}

// Seq updates the tracker based on an observed packet with sequence number seq and content size length.
// Initializes the tracker if it hasn't been initialized yet.
// Returns the bytes in flight (not including retransmits) and boolean indicator if this is a retransmit
func (t *Tracker) Seq(clock uint32, length uint16, synFin bool) (int32, bool) {
	t.packets++ // Some of these may be retransmits.

	if !t.initialized {
		t.seq = clock
		t.ack = clock // nothing acked so far
		t.initialized = true
	}
	// Use this unless we are sending new data.
	inflight, _ := diff(t.NextSeq(), t.ack)

	// TODO handle errors
	delta, err := diff(clock, t.seq)
	if err != nil {
		t.errors++
		fmt.Printf("Bad seq %4X %4X\n", t.seq, clock)
		return inflight, false
	}
	if delta < 0 {
		// DO NOT update w.seq or w.lastDataLength, as this is a retransmit
		t.sent += uint64(length)
		t.retransmits += uint64(length)
		return inflight, true
	}
	// delta is non-negative (not a retransmit)
	if delta != int32(t.lastDataLength) {
		t.errors++
		fmt.Printf("%d: Missing packet?  delta (%d) does not match last data size (%d)\n", t.packets, delta, t.lastDataLength) // VERBOSE
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

	gap, err := diff(t.seq, t.ack)
	if gap > t.maxGap {
		t.maxGap = gap
		//fmt.Printf("%8p - %5d: MaxGap = %d\n", t, t.packets, gap)
	}

	inflight, _ = diff(t.NextSeq(), t.ack)

	return inflight, false
}

// Total bytes transmitted, not including retransmits.
// TODO should this include the 1 byte SYN?
func (t *Tracker) Sent() uint64 {
	return t.sent - t.retransmits
}

func (t *Tracker) Acked() uint64 {
	return t.acked
}

func (t *Tracker) Ack(clock uint32, withData bool) {
	if !t.initialized {
		t.errors++
		fmt.Print("Ack called before Seq")
	}
	delta, err := diff(clock, t.ack)
	if err != nil {
		t.errors++
		fmt.Printf("Bad ack %4X %4X\n", t.ack, clock)
		return
	}
	t.acked += uint64(delta)
	t.acks++
	if !withData {
		t.onlyAcks++
	}
	t.ack = clock
}

// Check checks that a sack block is consistent with the current window.
func (t *Tracker) checkSack(sb sackBlock) error {
	// block should ALWAYS have positive width
	if width, err := diff(sb.Right, sb.Left); err != nil || width <= 0 {
		log.Println(ErrInvalidSackBlock, err, width, t.Acked())
		return ErrInvalidSackBlock
	}
	// block Right should ALWAYS be to the left of NextSeq()
	if overlap, err := diff(t.NextSeq(), sb.Right); err != nil || overlap < 0 {
		log.Println(ErrInvalidSackBlock, err, overlap, t.Acked())
		return ErrInvalidSackBlock
	}
	// Left should be to the right of ack
	if overlap, err := diff(sb.Left, t.ack); err != nil || overlap < 0 {
		// These often correspond to packets that show up as spurious retransmits in WireShark.
		log.Println(ErrLateSackBlock, err, overlap, t.Acked())
		return ErrLateSackBlock
	}
	return nil
}

// Sack updates the counter with sack information (from other direction)
func (t *Tracker) Sack(sb sackBlock) {
	if !t.initialized {
		t.errors++
		fmt.Println(ErrTrackerNotInitialized)
	}
	// Auto gen code
	if err := t.checkSack(sb); err != nil {
		t.errors++
		fmt.Println(ErrInvalidSackBlock, t.ack, sb, t.NextSeq())
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

	lastHeader *layers.TCP

	SeqTracker         Tracker // This should match the previous value of Sent.
	Sacks              uint64
	LastPacketTimeUsec uint64
}

func (s *state) Option(port layers.TCPPort, opt layers.TCPOption) {
	switch opt.OptionType {
	case layers.TCPOptionKindSACK:
		data := opt.OptionData
		sacks := make([]sackBlock, len(data)/8)
		binary.Read(bytes.NewReader(data), binary.BigEndian, &sacks)
		for _, block := range sacks {
			s.SeqTracker.Sack(block)
		}
	case layers.TCPOptionKindTimestamps:
	case layers.TCPOptionKindWindowScale:
		// TODO should this change after initialization?
		fmt.Printf("%v WindowScale change %d -> %d\n", port, s.WindowScale, opt.OptionData[0])
		s.WindowScale = opt.OptionData[0]
	default:
	}
}

func (s *state) Update(tcpLength uint16, tcp *layers.TCP, ci gopacket.CaptureInfo) {
	dataLength := tcpLength - uint16(4*tcp.DataOffset)
	if tcp.SrcPort == s.SrcPort {
		//log.Printf("Port:%20v packet:%d Seq:%10d Length:%5d SYN:%5v ACK:%5v", tcp.SrcPort, s.SeqTracker.packets, tcp.Seq, tcpLength, tcp.SYN, tcp.ACK)
		if inflight, retrans := s.SeqTracker.Seq(tcp.Seq, dataLength, tcp.SYN || tcp.FIN); retrans {
			// TODO
		} else if s.Window > 0 {
			window := int64(s.Window) << s.WindowScale
			fraction := float64(inflight) / float64(window)
			if fraction > 0.95 {
				fmt.Printf("Window limited? %d / %d\n", inflight, window)
			}
		}
		s.LastPacketTimeUsec = uint64(ci.Timestamp.UnixNano() / 1000)
		if tcp.ECE {
			s.ECECount++
		}
	} else {
		// Process ACKs and SACKs from the other direction
		if tcp.ACK {
			s.SeqTracker.Ack(tcp.Ack, dataLength > 0) // TODO
		}
		// Handle all options, including SACKs from other direction
		// TODO - should some of these be associated with the other direction?
		for i := 0; i < len(tcp.Options); i++ {
			s.Option(tcp.SrcPort, tcp.Options[i])
		}
		if s.Window != tcp.Window {
			fmt.Printf("Remote %v window changed from %d to %d\n", tcp.SrcPort, uint32(s.Window)<<s.WindowScale, uint32(tcp.Window)<<s.WindowScale)
			s.Window = tcp.Window
		}
	}
}

func (s state) String() string {
	return fmt.Sprintf("[%v:%5d %d %12d/%10d/%10d %8d win:%5d sacks:%4d retrans:%4d ece:%4d]", s.SrcIP, s.SrcPort, s.TTLChanges, s.SeqTracker.NextSeq(), s.SeqTracker.seq, s.SeqTracker.Acked(), s.LastPacketTimeUsec%10000000, s.Window, s.Sacks, s.SeqTracker.retransmits, s.ECECount)
}

type Parser struct {
	SrcIP   net.IP
	DstIP   net.IP
	SrcPort layers.TCPPort
	DstPort layers.TCPPort

	LeftState  state
	RightState state
}

// Parse parses an entire pcap file.
func (p *Parser) Parse(data []byte) (*schema.AlphaFields, error) {
	pcap, err := pcapgo.NewReader(strings.NewReader(string(data)))
	if err != nil {
		fmt.Print(err)
		return nil, err
	}

	// This is used to keep track of some of the TCP state.
	alpha := schema.AlphaFields{
		OptionCounts: make([]int64, 16),
	}

	data, ci, err := pcap.ReadPacketData()

	for err == nil {
		// Decode a packet
		packet := gopacket.NewPacket(data, layers.LayerTypeEthernet, gopacket.Default)
		if packet.ErrorLayer() != nil {
			fmt.Printf("Error decoding packet: %v", packet.ErrorLayer().Error()) // Somewhat VERBOSE
			continue
		}

		// TODO This really needs to be refactored and simplified.
		var tcpLength uint16
		if ipLayer := packet.Layer(layers.LayerTypeIPv4); ipLayer != nil {
			ip, _ := ipLayer.(*layers.IPv4)
			tcpLength = ip.Length - uint16(4*ip.IHL)
			switch alpha.Packets {
			case 0:
				p.LeftState.SrcIP = ip.SrcIP
				p.LeftState.TTL = ip.TTL
			case 1:
				p.RightState.SrcIP = ip.SrcIP
				p.RightState.TTL = ip.TTL
			default:
				if p.LeftState.SrcIP.Equal(ip.SrcIP) {
					if p.LeftState.TTL != ip.TTL {
						alpha.TTLChanges++
						p.LeftState.TTLChanges++
					}
				} else if p.RightState.SrcIP.Equal(ip.SrcIP) {
					if p.RightState.TTL != ip.TTL {
						alpha.TTLChanges++
						p.RightState.TTLChanges++
					}
				} else {
					alpha.IPChanges++
				}
			}
		} else if ipLayer := packet.Layer(layers.LayerTypeIPv6); ipLayer != nil {
			ip, _ := ipLayer.(*layers.IPv6)
			tcpLength = ip.Length // In IPv6, the Length field is the payload length.
			switch alpha.Packets {
			case 0:
				p.LeftState.SrcIP = ip.SrcIP
				p.LeftState.TTL = ip.HopLimit
			case 1:
				p.RightState.SrcIP = ip.SrcIP
				p.RightState.TTL = ip.HopLimit
			default:
				if p.LeftState.SrcIP.Equal(ip.SrcIP) {
					if p.LeftState.TTL != ip.HopLimit {
						alpha.TTLChanges++
						p.RightState.TTLChanges++
					}
				} else if p.RightState.SrcIP.Equal(ip.SrcIP) {
					if p.RightState.TTL != ip.HopLimit {
						alpha.TTLChanges++
						p.RightState.TTLChanges++
					}
				} else {
					alpha.IPChanges++
				}
			}
		}
		// Get the TCP layer from this packet
		if tcpLayer := packet.Layer(layers.LayerTypeTCP); tcpLayer != nil {
			tcp, _ := tcpLayer.(*layers.TCP)

			// Special case handling for first two packets.
			switch alpha.Packets {
			case 0:
				if tcp.SrcPort == 443 || tcp.DstPort == 443 {
					// log.Println(p.GetUUID(testName))
				}
				p.LeftState.SrcPort = tcp.SrcPort
				p.RightState.SrcPort = tcp.DstPort
			case 1:
				if p.RightState.SrcPort != tcp.SrcPort || !tcp.ACK {
					// Use fmt for advisory/info logging.
					fmt.Println("Bad sack block", p.RightState, p.LeftState, tcp.DstPort, tcp.ACK)
				}
			default:
			}

			var sack int
			// Handle options
			for i := 0; i < len(tcp.Options); i++ {
				// TODO test case for wrong index.
				if tcp.Options[i].OptionType > 15 {
					fmt.Printf("TCP Option %d has illegal option type %d", i, tcp.Options[i].OptionType)
					continue
				}
				alpha.OptionCounts[tcp.Options[i].OptionType]++
				if tcp.Options[i].OptionType == layers.TCPOptionKindSACK {
					// TODO This is overcounting.  We want to count the distinct packets that are skipped in the SACKs.
					sack = int(len(tcp.Options[i].OptionData) / 8)
					alpha.Sacks += int64(sack)
				}
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

			if alpha.Packets < 100 && (tcp.SrcPort == 443 || tcp.DstPort == 443) {
				//log.Printf("%2d, %10d, %10d, %s <--> %s", count, tcp.Seq, tcp.Ack, first, second)
			}
		}
		alpha.Packets++
		data, ci, err = pcap.ReadPacketData()
	}

	fmt.Printf("%20s: %v\n", p.LeftState.SrcPort, p.LeftState.SeqTracker.Summary())
	fmt.Printf("%20s: %v\n", p.RightState.SrcPort, p.RightState.SeqTracker.Summary())

	alpha.FirstECECount = p.LeftState.ECECount
	alpha.SecondECECount = p.RightState.ECECount
	alpha.FirstRetransmits = p.LeftState.SeqTracker.retransmits
	alpha.SecondRetransmits = p.RightState.SeqTracker.retransmits
	// TODO update these names
	alpha.TotalSrcSeq = int64(p.LeftState.SeqTracker.ByteCount())
	alpha.TotalDstSeq = int64(p.RightState.SeqTracker.ByteCount())

	return &alpha, nil
}
