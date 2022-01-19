// Package headers contains code to efficiently decode packet headers
// from a PCAP data stream.
package headers

import (
	"fmt"
	"log"
	"net"
	"os"
	"time"
	"unsafe"

	"github.com/google/gopacket/layers"

	"github.com/m-lab/go/logx"

	be "github.com/m-lab/etl/internal/bigendian"
	nano "github.com/m-lab/etl/internal/nano"
	"github.com/m-lab/etl/metrics"
)

var (
	sparseLogger = log.New(os.Stdout, "sparse: ", log.LstdFlags|log.Lshortfile)
	sparse1      = logx.NewLogEvery(sparseLogger, 1000*time.Millisecond)

	ErrTruncatedPcap = fmt.Errorf("truncated PCAP file")

	ErrNoIPLayer         = fmt.Errorf("no IP layer")
	ErrTruncatedIPHeader = fmt.Errorf("truncated IP header")
)

/******************************************************************************
 * 								IP Header handling
******************************************************************************/

// IP provides the common interface for IPv4 and IPv6 packet headers.
type IP interface {
	Version() uint8
	PayloadLength() int
	SrcIP(net.IP) net.IP
	DstIP(net.IP) net.IP
	NextProtocol() layers.IPProtocol
	HopLimit() uint8
	HeaderLength() int
}

//=============================================================================

// IPv4Header struct for IPv4 header, in wire format
type IPv4Header struct {
	versionIHL    uint8             // Version (4 bits) + Internet header length (4 bits)
	typeOfService uint8             // Type of service
	length        be.BE16           // Total length
	id            be.BE16           // Identification
	flagsFragOff  be.BE16           // Flags (3 bits) + Fragment offset (13 bits)
	hopLimit      uint8             // Time to live
	protocol      layers.IPProtocol // Protocol of next following bytes, after the options
	checksum      be.BE16           // Header checksum
	srcIP         be.BE32           // Source address
	dstIP         be.BE32           // Destination address
}

var IPv4HeaderSize = int(unsafe.Sizeof(IPv4Header{}))

func (h *IPv4Header) Version() uint8 {
	return (h.versionIHL >> 4)
}

func (h *IPv4Header) PayloadLength() int {
	ihl := h.versionIHL & 0x0f
	return int(h.length.Uint16()) - int(ihl*4)
}

// Overwrite the destination IP with the source IP, allocating if needed.
func replace(dst net.IP, src ...byte) net.IP {
	if dst != nil {
		dst = dst[:0]
	}
	return append(dst, src...)
}

// SrcIP returns the source IP address of the packet.
// It uses the provided backing parameter to avoid allocations.
func (h *IPv4Header) SrcIP(backing net.IP) net.IP {
	return replace(backing, h.srcIP[:]...)
}

// DstIP returns the destination IP address of the packet.
// It uses the provided backing parameter to avoid allocations.
func (h *IPv4Header) DstIP(backing net.IP) net.IP {
	return replace(backing, h.dstIP[:]...)
}

// NextProtocol returns the next protocol in the stack.
func (h *IPv4Header) NextProtocol() layers.IPProtocol {
	return h.protocol
}

// HopLimit returns the (remaining?) TTL of the packet.
func (h *IPv4Header) HopLimit() uint8 {
	return h.hopLimit
}

// HeaderLength returns the length of the header in bytes,
// (including extensions for ipv6).
func (h *IPv4Header) HeaderLength() int {
	return int(h.versionIHL&0x0f) << 2
}

// ExtensionHeader is used to parse IPv6 extension headers.
type ExtensionHeader struct {
	NextHeader        layers.IPProtocol
	HeaderLength      uint8
	OptionsAndPadding [6]byte
}

type EHWrapper struct {
	HeaderType layers.IPProtocol // Type of THIS header, not the next header.
	eh         *ExtensionHeader
	data       []byte // All the options and padding, including the first 6 bytes.
}

// IPv6Header struct for IPv6 header
type IPv6Header struct {
	versionTrafficClassFlowLabel be.BE32           // Version (4 bits) + Traffic class (8 bits) + Flow label (20 bits)
	payloadLength                be.BE16           // Original payload length, NOT the payload size of the captured packet.
	nextHeader                   layers.IPProtocol // Protocol of next layer/header
	hopLimit                     uint8             // Hop limit
	srcIP                        [16]byte
	dstIP                        [16]byte
}

var IPv6HeaderSize = int(unsafe.Sizeof(IPv6Header{}))

func OverlayIPv6Header(data []byte) (*IPv6Header, []byte, error) {
	if len(data) < int(unsafe.Sizeof(IPv6Header{})) {
		return nil, nil, ErrTruncatedIPHeader
	}
	h := (*IPv6Header)(unsafe.Pointer(&data[0]))
	if h.Version() != 6 {
		return nil, nil, fmt.Errorf("IPv6 packet with version %d", h.Version())
	}
	return h, data[IPv6HeaderSize:], nil
}

// Overlay reuses this object, using the provided wire data.
// The wire data is NOT copied, but is used to back the object fields.
func (w *IPv6Wrapper) Overlay(wire []byte) (payload []byte, err error) {
	w.IPv6Header, _, err = OverlayIPv6Header(wire)
	if err != nil {
		return nil, err
	}
	err = w.handleExtensionHeaders(wire)
	if err != nil {
		return nil, err
	}
	if len(wire) < w.headerLength {
		return nil, ErrTruncatedIPHeader
	}
	return wire[w.headerLength:], err
}

func (h *IPv6Header) Version() uint8 {
	return (h.versionTrafficClassFlowLabel[0] >> 4)
}

func (h *IPv6Header) PayloadLength() int {
	return int(h.payloadLength.Uint16())
}

func (h *IPv6Header) SrcIP(backing net.IP) net.IP {
	return replace(backing, h.srcIP[:]...)
}

// DstIP returns the destination IP address of the packet.
func (h *IPv6Header) DstIP(backing net.IP) net.IP {
	return replace(backing, h.dstIP[:]...)
}

func (h *IPv6Header) HopLimit() uint8 {
	return h.hopLimit
}

// TODO - This may not be what we want.
func (h *IPv6Header) NextProtocol() layers.IPProtocol {
	return h.nextHeader
}

func (h *IPv6Header) HeaderLength() int {
	// BUG - this is WRONG
	return IPv4HeaderSize
}

type IPv6Wrapper struct {
	*IPv6Header
	ext          []EHWrapper
	headerLength int
}

func (w *IPv6Wrapper) HeaderLength() int {
	return w.headerLength
}

// handleExtensionHeaders reuses the IPv6 header, overlaying it on provided wire data.
// It does not copy or disturb the underlying data.
func (w *IPv6Wrapper) handleExtensionHeaders(rawWire []byte) error {
	if w == nil {
		return fmt.Errorf("nil IPv6Wrapper")
	}
	if w.ext != nil {
		w.ext = make([]EHWrapper, 0)
	}
	w.ext = w.ext[:0]

	if w.nextHeader == layers.IPProtocolNoNextHeader {
		return nil
	}

	np := w.NextProtocol()
	for {
		switch np {
		case layers.IPProtocolNoNextHeader:
			return nil
		case layers.IPProtocolIPv6HopByHop:
		case layers.IPProtocolTCP:
			return nil
		default:
			metrics.WarningCount.WithLabelValues("pcap", "ipv6", "unsupported_extension_type").Inc()
			sparse1.Println("Other IPv6 extension type", np)
		}

		if len(rawWire) < 8 {
			metrics.ErrorCount.WithLabelValues("pcap", "ipv6", "truncated_extension").Inc()
			return ErrTruncatedIPHeader
		}

		eh := (*ExtensionHeader)(unsafe.Pointer(&rawWire[0]))
		if len(rawWire) < int(8+eh.HeaderLength) {
			metrics.ErrorCount.WithLabelValues("pcap", "ipv6", "truncated_extension").Inc()
			return ErrTruncatedIPHeader
		}
		w.ext = append(w.ext, EHWrapper{
			HeaderType: np,
			eh:         eh,
			data:       rawWire[2 : 8+eh.HeaderLength],
		})
		w.headerLength += int(eh.HeaderLength) + 8
		rawWire = rawWire[8+eh.HeaderLength:]
		np = eh.NextHeader
	}
}

// Packet struct contains the packet data and metadata.
// Since it is intended primary to access IP and TCP, those interfaces
// are exposes as embedded fields.
type Packet struct {
	PktTime nano.UnixNano
	eth     *EthernetHeader // Pointer to the Ethernet header, if available.
	IP                      // Access to the IP header, if available.
	v4      *IPv4Header     // DO NOT USE.  Use ip field instead.
	v6      *IPv6Wrapper    // DO NOT USE.  Use ip field instead.

	sharedBacking []byte // The raw packet data, including header.  NOT a copy!
}

// RawForTest provides access to the raw packet data for testing.
func (p *Packet) RawForTest() []byte {
	return p.sharedBacking
}

// Overlay updates THIS Packet object to overlay the underlying packet data,
// passed in wire format.  It avoids copying and allocation as much as possible.
func (p *Packet) Overlay(pTime nano.UnixNano, wire []byte) (err error) {

	if len(wire) < EthernetHeaderSize {
		metrics.ErrorCount.WithLabelValues("pcap", "ethernet", "truncated_header").Inc()
		err = ErrTruncatedEthernetHeader
		return
	}
	p.sharedBacking = wire
	p.PktTime = pTime
	p.eth = (*EthernetHeader)(unsafe.Pointer(&wire[0]))

	switch p.eth.EtherType() {
	case layers.EthernetTypeIPv4:
		if len(wire) < EthernetHeaderSize+IPv4HeaderSize {
			metrics.ErrorCount.WithLabelValues("pcap", "ipv4", "truncated_header").Inc()
			err = ErrTruncatedIPHeader
			return
		}
		p.v4 = (*IPv4Header)(unsafe.Pointer(&wire[EthernetHeaderSize]))
		p.IP = p.v4
	case layers.EthernetTypeIPv6:
		if len(wire) < EthernetHeaderSize+IPv6HeaderSize {
			metrics.ErrorCount.WithLabelValues("pcap", "ipv6", "truncated_header").Inc()
			err = ErrTruncatedIPHeader
			return
		}
		if p.v6 == nil {
			// This allocation should only happen once.
			p.v6 = &IPv6Wrapper{}
		}
		_, err = p.v6.Overlay(wire[EthernetHeaderSize:])
		if err != nil {
			return
		}
		p.IP = p.v6
	default:
		err = ErrUnknownEtherType
		return
	}
	if p.IP != nil {
		switch p.IP.NextProtocol() {
		case layers.IPProtocolTCP:
			// TODO - add TCP layer decoding
		}
	}

	return nil
}

func (p *Packet) PayloadLength() int {
	if p.IP == nil {
		return 0
	}
	return p.IP.PayloadLength()
}
