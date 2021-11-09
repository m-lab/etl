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

// TODO - build a sackblock model, that consolidates new sack blocks into existing state.
type sackBlock struct {
	Left  uint32
	Right uint32
}

type Tracker struct {
	initialized bool
	packets     uint32 // Number of calls to Seq function
	seq         uint32
	syn         uint32 // one or zero, depending on whether SYN was sent.
	sent        uint64 // actual bytes sent, including retransmits
	retransmits uint64 // bytes retransmitted

	ack   uint32 // last observed ack
	acked uint64 // bytes acked

	// sacks keeps track of outstanding SACK blocks
	sacks     []sackBlock
	sackBytes uint64 // keeps track of total bytes reported missing in SACK blocks

	lastDataLength uint16 // Used to compute NextSeq()
}

// NextSeq returns the uint32 value of the expected next sequence number.
func (w *Tracker) NextSeq() uint32 {
	return w.seq + uint32(w.lastDataLength) // wraps at 2^32
}

// ByteCount returns the number of bytes sent so far, not including retransmits
func (w *Tracker) ByteCount() uint64 {
	return w.sent + uint64(w.lastDataLength)
}

func (w *Tracker) delta(clock uint32) int64 {
	delta := int64(clock) - int64(w.seq)
	if delta < -1<<31 {
		delta += 1 << 30
	}
	if delta > 1<<30 || delta < -1<<30 {
		fmt.Print("invalid counter delta")
	}
	return delta
}

// Seq updates the tracker based on an observed packet with sequence number seq and content size length.
// Initializes the tracker if it hasn't been initialized yet.
// Returns true if this is a retransmit
func (w *Tracker) Seq(clock uint32, length uint16, synFin bool) bool {
	w.packets++ // Some of these may be retransmits.

	if !w.initialized {
		w.seq = clock
		w.ack = clock // nothing acked so far
		w.initialized = true
	}
	delta := w.delta(clock)
	if delta < 0 {
		// DO NOT update w.seq or w.lastDataLength, as this is a retransmit
		w.sent += uint64(length)
		w.retransmits += uint64(length)
		return true
	}
	// delta is non-negative (not a retransmit)
	if delta != int64(w.lastDataLength) {
		log.Printf("%d: delta (%d) does not match last data size (%d)", w.packets, delta, w.lastDataLength)
	}

	if synFin {
		// Should this include length?
		w.lastDataLength = 1 + length
	} else {
		w.lastDataLength = length
	}
	//log.Println(w.sent, delta, w.lastDataLength)

	w.sent += uint64(delta)
	w.seq = clock
	return false
}

// Total bytes transmitted, not including retransmits.
func (w *Tracker) Value() uint64 {
	return w.sent - w.retransmits
}

func (w *Tracker) Acked() uint64 {
	return w.acked
}

func (w *Tracker) Ack(clock uint32) {
	if !w.initialized {
		fmt.Print("Ack called before Seq")
	}
}

// Sack updates the counter with sack information (from other direction)
func (w *Tracker) Sack(block sackBlock) {
	if !w.initialized {
		fmt.Print("Sack called before Seq")
	}
	// Auto gen code
	if block.Left > block.Right {
		fmt.Print("Sack block has left > right")
	}
	if block.Left < w.ack {
		fmt.Print("Sack block has left < ack")
	}
	if block.Right > w.NextSeq() {
		fmt.Print("Sack block has right > next seq")
	}
	w.sacks = append(w.sacks, block)
	w.sackBytes += uint64(block.Right - block.Left)
}

type endpoint struct {
	SrcIP net.IP
	TTL   uint8

	SrcPort layers.TCPPort // When this port is SrcPort, we update this stat struct.

	// The following fields are updated when we see a packet with SrcPort == SrcPort.
	// Sent is the total bytes sent, including retransmits.
	Sent                 uint64 // Total bytes sent
	PacketsSent          uint64 // Total packets sent
	Retransmits          uint64 // Total bytes retransmitted
	PacketsRetransmitted uint64 // Total packets retransmitted

	// Seq keeps track of the outgoing Sequence Number.
	// It NOT updated for retransmits.
	Seq     Tracker
	NextSeq uint64 // The next expected sequence number, based on Seq, and the number of bytes sent.

	// The following fields are updated when we see a packet with SrcPort != SrcPort.
	UnAcked uint64 // Total bytes unacked
	Sacked  uint64 // Total bytes sacked
}

type stats struct {
	Window     uint16
	ECECount   uint64
	TTLChanges uint64 // Observed number of TTL values that don't match first IP header.
}

// state keep track of the TCP state of one side of a connection.
// TODO - add histograms for Ack inter-arrival time.
type state struct {
	endpoint // TODO
	stats    // TODO

	maxSeq uint32

	lastHeader *layers.TCP

	SeqTracker         Tracker // This should match the previous value of Sent.
	Sacks              uint64
	LastPacketTimeUsec uint64
}

func (s *state) Update(tcpLength uint16, tcp *layers.TCP, ci gopacket.CaptureInfo) {
	if tcp.SrcPort == s.SrcPort {
		//log.Printf("Port:%20v packet:%d Seq:%10d Length:%5d SYN:%5v ACK:%5v", tcp.SrcPort, s.SeqTracker.packets, tcp.Seq, tcpLength, tcp.SYN, tcp.ACK)
		dataLength := tcpLength - uint16(4*tcp.DataOffset)
		if s.SeqTracker.Seq(tcp.Seq, dataLength, tcp.SYN || tcp.FIN) { // TODO
		}
		s.LastPacketTimeUsec = uint64(ci.Timestamp.UnixNano() / 1000)
		s.Window = tcp.Window
		if tcp.ECE {
			s.ECECount++
		}
	} else {
		// Process ACKs and SACKs from the other direction
		if tcp.ACK {
			s.SeqTracker.Ack(tcp.Ack) // TODO
		}
		// TODO Handle Sacks
		// Handle options
		for i := 0; i < len(tcp.Options); i++ {
			if tcp.Options[i].OptionType == layers.TCPOptionKindSACK {
				data := tcp.Options[i].OptionData
				sacks := make([]sackBlock, len(data)/8)
				binary.Read(bytes.NewReader(data), binary.BigEndian, &sacks)
				for _, block := range sacks {
					s.SeqTracker.Sack(block)
				}
			}
		}

	}
}

func (s state) String() string {
	return fmt.Sprintf("[%v:%5d %d %12d/%10d/%10d %8d win:%5d sacks:%4d retrans:%4d ece:%4d]", s.SrcIP, s.SrcPort, s.TTLChanges, s.Sent, s.SeqTracker.Value(), s.SeqTracker.Acked(), s.LastPacketTimeUsec%10000000, s.Window, s.Sacks, s.Retransmits, s.ECECount)
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

	optionNames := make([]string, 16)
	for i := 0; i < 16; i++ {
		optionNames[i] = layers.TCPOptionKind(i).String()
	}

	data, ci, err := pcap.ReadPacketData()

	for err == nil {
		// Decode a packet
		packet := gopacket.NewPacket(data, layers.LayerTypeEthernet, gopacket.Default)
		if packet.ErrorLayer() != nil {
			log.Printf("Error decoding packet: %v", packet.ErrorLayer().Error())
			continue
		}
		/*
			if packet.Metadata().Truncated {
				if alpha.Packets < 5 {
					log.Printf("Packet %d truncated to %d of %d bytes, from data of length %d",
						alpha.Packets, packet.Metadata().CaptureInfo.CaptureLength,
						packet.Metadata().CaptureInfo.Length, len(data))
				}
				alpha.TruncatedPackets++
			}*/

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
				alpha.OptionCounts[i]++
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

	alpha.FirstECECount = p.LeftState.ECECount
	alpha.SecondECECount = p.RightState.ECECount
	alpha.FirstRetransmits = p.LeftState.Retransmits
	alpha.SecondRetransmits = p.RightState.Retransmits
	alpha.TotalSrcSeq = int64(p.LeftState.SeqTracker.Value())
	alpha.TotalDstSeq = int64(p.RightState.SeqTracker.Value())

	if alpha.FirstECECount > 0 || alpha.SecondECECount > 0 || alpha.FirstRetransmits > 0 || alpha.SecondRetransmits > 0 {
		log.Printf("%d/%d truncated, %v <--> %v", alpha.TruncatedPackets, alpha.Packets, p.LeftState, p.RightState) //, alpha)
	}
	return &alpha, nil
}
