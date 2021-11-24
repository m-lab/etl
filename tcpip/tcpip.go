// Package tcpip contains code to extract IP and TCP packets from a PCAP file,
// and model the TCP connection state machine.
package tcpip

// The key to safety with unsafe pointers is to gaurantee that the
// pointer is nil before the underlying object goes out of scope.
// The opposite is more likely, if there is a wrapper object containing
// to both the underlying object and the unsafe pointer.  The wrapper
// points to the underlying object, and the unsafe pointer, and when
// it is garbage collected, it will make both the underlying object
// and the unsafe pointer eligible for collection.

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"time"
	"unsafe"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/m-lab/go/logx"
)

var (
	info         = log.New(os.Stdout, "info: ", log.LstdFlags|log.Lshortfile)
	sparseLogger = log.New(os.Stdout, "sparse: ", log.LstdFlags|log.Lshortfile)
	sparse20     = logx.NewLogEvery(sparseLogger, 50*time.Millisecond)

	ErrNotTCP                  = fmt.Errorf("not a TCP packet")
	ErrNoIPLayer               = fmt.Errorf("no IP layer")
	ErrTruncatedEthernetHeader = fmt.Errorf("truncated Ethernet header")
	ErrTruncatedIPHeader       = fmt.Errorf("truncated IP header")
	ErrTruncatedTCPHeader      = fmt.Errorf("truncated TCP header")
	ErrUnknownEtherType        = fmt.Errorf("unknown Ethernet type")
)

/******************************************************************************
	 Ethernet Header
******************************************************************************/
type EthernetHeader struct {
	SrcMAC, DstMAC [6]byte
	etherType      [2]byte // BigEndian
}

func (e *EthernetHeader) EtherType() layers.EthernetType {
	return layers.EthernetType(binary.BigEndian.Uint16(e.etherType[:]))
}

var EthernetHeaderSize = int(unsafe.Sizeof(EthernetHeader{}))

/******************************************************************************
 * IP Header handling
******************************************************************************/

// IP provides the common interface for IPv4 and IPv6 packet headers.
type IP interface {
	Version() uint8
	// HeaderLength() int
	PayloadLength() int
	SrcIP() net.IP
	DstIP() net.IP
	NextProtocol() layers.IPProtocol
	HopLimit() uint8
	HeaderSize() int
}

// IPv4Header struct for IPv4 header
type IPv4Header struct {
	versionIHL    uint8             // Version (4 bits) + Internet header length (4 bits)
	typeOfService uint8             // Type of service
	length        [2]byte           // Total length
	id            [2]byte           // Identification
	flagsFragOff  [2]byte           // Flags (3 bits) + Fragment offset (13 bits)
	hopLimit      uint8             // Time to live
	protocol      layers.IPProtocol // Protocol of next following bytes, after the options
	checksum      [2]byte           // Header checksum
	srcIP         [4]byte           // Source address
	dstIP         [4]byte           // Destination address
}

var IPv4HeaderSize = int(unsafe.Sizeof(IPv4Header{}))

func (h *IPv4Header) Version() uint8 {
	return (h.versionIHL >> 4)
}

func (h *IPv4Header) PayloadLength() int {
	ihl := h.versionIHL & 0x0f
	return int(binary.BigEndian.Uint16(h.length[:]) - uint16(4*ihl))
}

func (h *IPv4Header) HeaderLength() int {
	return int(h.versionIHL&0x0f) << 2
}

func (h *IPv4Header) SrcIP() net.IP {
	ip := make(net.IP, 4)
	copy(ip, h.srcIP[:])
	return ip
}

// DstIP returns the destination IP address of the packet.
func (h *IPv4Header) DstIP() net.IP {
	ip := make(net.IP, 4)
	copy(ip, h.dstIP[:])
	return ip
}

// NextProtocol returns the next protocol in the stack.
func (h *IPv4Header) NextProtocol() layers.IPProtocol {
	return h.protocol
}

func (h *IPv4Header) HopLimit() uint8 {
	return h.hopLimit
}

func (h *IPv4Header) HeaderSize() int {
	return int(h.versionIHL&0x0f) << 2
}

func assertV4isIP(ip *IPv4Header) {
	func(IP) {}(ip)
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
	payload    []byte // Any additional data remaining after the extension header.
}

// Next the next EHWrapper.
// It may return nil if there are no more, or ErrTruncatedIPHeader if the header is truncated.
func (w *EHWrapper) Next() (*EHWrapper, error) {
	if w.eh.NextHeader == layers.IPProtocolNoNextHeader {
		return nil, nil
	}
	if w.eh == nil || len(w.data)%8 != 0 || len(w.payload) < 8 {
		return nil, ErrTruncatedIPHeader
	}
	next := (*ExtensionHeader)(unsafe.Pointer(&w.data[0]))
	return &EHWrapper{
		HeaderType: w.HeaderType,
		eh:         next,
		data:       w.data[2 : 8+next.HeaderLength],
		payload:    w.payload[8+next.HeaderLength:],
	}, nil
}

// IPv6Header struct for IPv6 header
type IPv6Header struct {
	versionTrafficClassFlowLabel [4]byte           // Version (4 bits) + Traffic class (8 bits) + Flow label (20 bits)
	payloadLength                [2]byte           // Original payload length, NOT the payload size of the captured packet.
	nextHeader                   layers.IPProtocol // Protocol of next layer/header
	hopLimit                     uint8             // Hop limit
	srcIP                        [16]byte
	dstIP                        [16]byte
}

var IPv6HeaderSize = int(unsafe.Sizeof(IPv6Header{}))

// NewIPv6Header creates a new IPv6 header and the first associated ExtensionHeader, in an ExtensionHeaderWrapper.
func NewIPv6Header(data []byte) (*IPv6Header, *EHWrapper, error) {
	if len(data) < int(unsafe.Sizeof(IPv6Header{})) {
		return nil, nil, ErrTruncatedIPHeader
	}
	h := (*IPv6Header)(unsafe.Pointer(&data[0]))
	if h.nextHeader == layers.IPProtocolNoNextHeader {
		return h, nil, nil
	}
	eh := (*ExtensionHeader)(unsafe.Pointer(&data[unsafe.Sizeof(IPv6Header{})]))
	minSize := int(unsafe.Sizeof(IPv6Header{})) + int(eh.HeaderLength) + 8
	if len(data) < minSize {
		return h, nil, ErrTruncatedIPHeader
	}
	return h, &EHWrapper{
		HeaderType: h.nextHeader,
		eh:         eh,
		data:       data[minSize-8 : minSize+int(eh.HeaderLength)],
		payload:    data[minSize+int(eh.HeaderLength):],
	}, nil
}

func (h *IPv6Header) Version() uint8 {
	return (h.versionTrafficClassFlowLabel[0] >> 4)
}

func (h *IPv6Header) PayloadLength() int {
	return int(binary.BigEndian.Uint16(h.payloadLength[:]))
}

func (h *IPv6Header) SrcIP() net.IP {
	ip := make(net.IP, 16)
	copy(ip, h.srcIP[:])
	return ip
}

// DstIP returns the destination IP address of the packet.
func (h *IPv6Header) DstIP() net.IP {
	ip := make(net.IP, 16)
	copy(ip, h.dstIP[:])
	return ip
}

func (h *IPv6Header) HopLimit() uint8 {
	return h.hopLimit
}

// TODO - This may not be what we want.
func (h *IPv6Header) NextProtocol() layers.IPProtocol {
	return h.nextHeader
}

func (h *IPv6Header) HeaderSize() int {
	return IPv4HeaderSize
}

func assertV6IP(ip *IPv6Header) {
	func(IP) {}(ip)
}

/******************************************************************************
 * TCP Header and state machine
******************************************************************************/
type TCPOption struct {
	Kind uint8
	Len  uint8
	// This byte array may be shorter than 38 bytes, and cause panics if improperly accessed.
	Data [38]byte // Max length of all TCP options is 40 bytes, so data is limited to 38 bytes.
}

// TCPHeader is autogenerated using Copilot.
type TCPHeader struct {
	srcPort, dstPort [2]byte // Source and destination port
	seqNum           [4]byte // Sequence number
	ackNum           [4]byte // Acknowledgement number
	dataOffsetFlags  uint8   // DataOffset, and Flags
	window           [2]byte // Window
	checksum         [2]byte // Checksum
	urgent           [2]byte // Urgent pointer
}

var TCPHeaderSize = int(unsafe.Sizeof(TCPHeader{}))

func (h *TCPHeader) DataOffset() int {
	return 4 * (int(h.dataOffsetFlags&0xf0) >> 4)
}

func (h *TCPHeader) FIN() bool {
	return (h.dataOffsetFlags & 0x01) != 0
}

func (h *TCPHeader) SYN() bool {
	return (h.dataOffsetFlags & 0x02) != 0
}

func (h *TCPHeader) RST() bool {
	return (h.dataOffsetFlags & 0x04) != 0
}

func (h *TCPHeader) PSH() bool {
	return (h.dataOffsetFlags & 0x08) != 0
}

func (h *TCPHeader) ACK() bool {
	return (h.dataOffsetFlags & 0x10) != 0
}

type TCPHeaderWrapper struct {
	TCP     *TCPHeader
	Options []*TCPOption
}

func WrapTCP(data []byte) (*TCPHeaderWrapper, error) {
	if len(data) < TCPHeaderSize {
		return nil, ErrTruncatedTCPHeader
	}
	tcp := (*TCPHeader)(unsafe.Pointer(&data[0]))
	if tcp.DataOffset() > len(data) {
		return nil, ErrTruncatedTCPHeader
	}
	return &TCPHeaderWrapper{
		TCP:     tcp,
		Options: nil, // parseTCPOptions(data[TCPHeaderSize:]),
	}, nil
}

// Packet struct contains the packet data and metadata.
type Packet struct {
	// If we use a pointer here, for some reason we get zero value timestamps.
	ci   gopacket.CaptureInfo
	data []byte
	eth  *EthernetHeader
	ip   IP
	v4   *IPv4Header       // Nil unless we're parsing IPv4 packets.
	v6   *IPv6Header       // Nil unless we're parsing IPv6 packets.
	ext  *EHWrapper        // Nil unless we're parsing IPv6 packets.
	tcp  *TCPHeaderWrapper // This takes up a small amount of space for the options.
	err  error
}

func Wrap(ci gopacket.CaptureInfo, data []byte) (*Packet, error) {
	if len(data) < EthernetHeaderSize {
		return nil, ErrTruncatedEthernetHeader
	}
	p := Packet{
		ci:   ci,
		data: data,
		eth:  (*EthernetHeader)(unsafe.Pointer(&data[0])),
	}
	switch p.eth.EtherType() {
	case layers.EthernetTypeIPv4:
		if len(data) < EthernetHeaderSize+IPv4HeaderSize {
			return nil, ErrTruncatedIPHeader
		}
		p.v4 = (*IPv4Header)(unsafe.Pointer(&data[EthernetHeaderSize]))
		p.ip = p.v4
	case layers.EthernetTypeIPv6:
		if len(data) < EthernetHeaderSize+IPv6HeaderSize {
			return nil, ErrTruncatedIPHeader
		}
		var err error
		p.v6, p.ext, err = NewIPv6Header(data[EthernetHeaderSize:])
		if err != nil {
			return nil, err
		}
		p.ip = p.v6
	default:
		return nil, ErrUnknownEtherType
	}
	// TODO needs more work
	if p.ip != nil {
		switch p.ip.NextProtocol() {
		case layers.IPProtocolTCP:
			if len(data) < EthernetHeaderSize+p.ip.HeaderSize()+TCPHeaderSize {
				return nil, ErrTruncatedTCPHeader
			}
			p.tcp = &TCPHeaderWrapper{
				TCP: (*TCPHeader)(unsafe.Pointer(&data[EthernetHeaderSize])),
			}
			return &p, nil
		}
	}

	return &p, nil
}

func (p *Packet) GetLayers() error {
	p.eth = (*EthernetHeader)(unsafe.Pointer(&p.data[0]))
	switch p.eth.EtherType() {
	case layers.EthernetTypeIPv4:
		if len(p.data) < int(14+unsafe.Sizeof(IPv4Header{})) {
			return ErrTruncatedIPHeader
		}
		p.v4 = (*IPv4Header)(unsafe.Pointer(&p.data[14]))
	case layers.EthernetTypeIPv6:
		if len(p.data) < int(14+unsafe.Sizeof(IPv6Header{})) {
			return ErrTruncatedIPHeader
		}
		p.v6 = (*IPv6Header)(unsafe.Pointer(&p.data[14]))
	default:
		return ErrNoIPLayer
	}
	return nil
}

func (p *Packet) TCPLength() int {
	if p.v4 != nil {
		return int(p.v4.PayloadLength())
	}
	return int(binary.BigEndian.Uint16(p.v6.payloadLength[:]))
}

// GetTCP constructs or retrieves the TCPHeaderWrapper for this packet.
// This requires correctly parsing the IP header to find the correct offset,
// and then parsing the TCP header and creating the options array.
// The result is cached in the Packet's TCP field.
func GetTCP(data []byte) (*TCPHeaderWrapper, error) {
	return nil, ErrNotTCP
}

// FastExtractIPFields extracts a few IP fields from the packet.
func (p *Packet) FastExtractIPFields() (srcIP, dstIP net.IP, TTL uint8, tcpLength uint16, err error) {
	if p.eth == nil {
		err = p.GetLayers()
		if err != nil {
			return nil, nil, 0, 0, err
		}
	}
	if p.v4 != nil {
		srcIP = make([]byte, 4)
		dstIP = make([]byte, 4)
		copy(srcIP, p.v4.SrcIP())
		copy(dstIP, p.v4.DstIP())
		TTL = p.v4.hopLimit
		tcpLength = uint16(p.v4.PayloadLength())
		if p.v4.protocol != layers.IPProtocolTCP {
			err = ErrNotTCP
		}
	} else if p.v6 != nil {
		srcIP = make([]byte, 16)
		dstIP = make([]byte, 16)
		copy(srcIP, p.v6.SrcIP())
		copy(dstIP, p.v6.DstIP())
		TTL = p.v6.hopLimit
		tcpLength = binary.BigEndian.Uint16(p.v6.payloadLength[:])
		// TODO - needs more work
		if p.ext.HeaderType != layers.IPProtocolTCP {
			err = ErrNotTCP
		}
	} else {
		return nil, nil, 0, 0, ErrNoIPLayer
	}
	return
}
