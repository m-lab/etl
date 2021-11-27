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
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"time"
	"unsafe"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcapgo"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/go/logx"
)

var (
	info         = log.New(os.Stdout, "info: ", log.LstdFlags|log.Lshortfile)
	sparseLogger = log.New(os.Stdout, "sparse: ", log.LstdFlags|log.Lshortfile)
	sparse20     = logx.NewLogEvery(sparseLogger, 50*time.Millisecond)

	ErrTruncatedPcap           = fmt.Errorf("Truncated PCAP file")
	ErrNotTCP                  = fmt.Errorf("not a TCP packet")
	ErrNoIPLayer               = fmt.Errorf("no IP layer")
	ErrNoTCPLayer              = fmt.Errorf("no TCP layer")
	ErrTruncatedEthernetHeader = fmt.Errorf("truncated Ethernet header")
	ErrTruncatedIPHeader       = fmt.Errorf("truncated IP header")
	ErrTruncatedTCPHeader      = fmt.Errorf("truncated TCP header")
	ErrUnknownEtherType        = fmt.Errorf("unknown Ethernet type")
	ErrMalformedTCPOption      = fmt.Errorf("malformed TCP option")
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
	PayloadLength() int
	SrcIP() net.IP
	DstIP() net.IP
	NextProtocol() layers.IPProtocol
	HopLimit() uint8
	HeaderLength() int
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

func (h *IPv4Header) HeaderLength() int {
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

// NewIPv6Header creates a new IPv6 header, and returns the header and remaining bytes.
func NewIPv6Header(data []byte) (*IPv6Wrapper, []byte, error) {
	if len(data) < int(unsafe.Sizeof(IPv6Header{})) {
		return nil, nil, ErrTruncatedIPHeader
	}
	h := (*IPv6Header)(unsafe.Pointer(&data[0]))
	if h.Version() != 6 {
		return nil, nil, fmt.Errorf("IPv6 packet with version %d", h.Version())
	}
	// Wrap the header, compute, the extension headers.
	w, err := h.Wrap(data[IPv6HeaderSize:])
	if err != nil {
		return nil, nil, err
	}
	if len(data) < w.headerLength {
		return nil, nil, ErrTruncatedIPHeader

		// TODO remove this check.
	} else if len(data) == w.headerLength {
		return w, []byte{}, nil
	}
	return w, data[w.headerLength:], err
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

func (h *IPv6Header) HeaderLength() int {
	// BUG - this is WRONG
	return IPv4HeaderSize
}

func assertV6IP(ip *IPv6Header) {
	func(IP) {}(ip)
}

type IPv6Wrapper struct {
	*IPv6Header
	ext          []EHWrapper
	headerLength int
}

func (w *IPv6Wrapper) HeaderLength() int {
	return w.headerLength
}

// func (w *IPv6Wrapper) payload() []byte {
// 	if len(w.ext) == 0 {
// 		return nil
// 	}
// 	return w.ext[len(w.ext)-1].payload
// }

// Wrap creates a wrapper with extension headers.
// data is the remainder of the header data, not including the IPv6 header.
func (ip *IPv6Header) Wrap(data []byte) (*IPv6Wrapper, error) {
	w := IPv6Wrapper{
		IPv6Header:   ip,
		ext:          make([]EHWrapper, 0, 0),
		headerLength: IPv6HeaderSize,
	}
	if w.nextHeader == layers.IPProtocolNoNextHeader {
		return &w, nil
	}

	for np := w.NextProtocol(); np != layers.IPProtocolNoNextHeader; {
		switch np {
		case layers.IPProtocolNoNextHeader:
			return &w, nil
		case layers.IPProtocolIPv6HopByHop:
		case layers.IPProtocolTCP:
			return &w, nil
		default:
			log.Println("IPv6 header type", np)
		}

		if len(data) < 8 {
			return nil, ErrTruncatedIPHeader
		}

		eh := (*ExtensionHeader)(unsafe.Pointer(&data[0]))
		if len(data) < int(8+eh.HeaderLength) {
			return nil, ErrTruncatedIPHeader
		}
		w.ext = append(w.ext, EHWrapper{
			HeaderType: np,
			eh:         eh,
			data:       data[2 : 8+eh.HeaderLength],
			//payload:    data[8+eh.HeaderLength:],
		})
		w.headerLength += int(eh.HeaderLength) + 8
		data = data[8+eh.HeaderLength:]
		np = eh.NextHeader
	}
	return nil, ErrTruncatedIPHeader
}

/******************************************************************************
 * TCP Header and state machine
******************************************************************************/
type TCPOption struct {
	Kind layers.TCPOptionKind
	Len  uint8
	// This byte array may be shorter than 38 bytes, and cause panics if improperly accessed.
	Data []byte // Max length of all TCP options is 40 bytes, so data is limited to 38 bytes.
}

type tcpOption struct {
	kind layers.TCPOptionKind
	len  uint8
	data [38]byte
}

// TCPHeader is autogenerated using Copilot.
type TCPHeader struct {
	srcPort, dstPort [2]byte // Source and destination port
	seqNum           [4]byte // Sequence number
	ackNum           [4]byte // Acknowledgement number
	dataOffset       uint8   //  DataOffset: upper 4 bits
	flags            uint8   // Flags
	window           [2]byte // Window
	checksum         [2]byte // Checksum
	urgent           [2]byte // Urgent pointer
}

var TCPHeaderSize = int(unsafe.Sizeof(TCPHeader{}))

func (h *TCPHeader) SrcPort() layers.TCPPort {
	return layers.TCPPort(binary.BigEndian.Uint16(h.srcPort[:]))
}

func (h *TCPHeader) DstPort() layers.TCPPort {
	return layers.TCPPort(binary.BigEndian.Uint16(h.dstPort[:]))
}

func (h *TCPHeader) DataOffset() int {
	return 4 * int(h.dataOffset>>4)
}

func (h *TCPHeader) FIN() bool {
	return (h.flags & 0x01) != 0
}

func (h *TCPHeader) SYN() bool {
	return (h.flags & 0x02) != 0
}

func (h *TCPHeader) RST() bool {
	return (h.flags & 0x04) != 0
}

func (h *TCPHeader) PSH() bool {
	return (h.flags & 0x08) != 0
}

func (h *TCPHeader) ACK() bool {
	return (h.flags & 0x10) != 0
}

type TCPHeaderWrapper struct {
	TCP     *TCPHeader
	Options []TCPOption
}

func (w *TCPHeaderWrapper) parseTCPOptions(data []byte) error {
	if len(data) == 0 {
		w.Options = make([]TCPOption, 0, 0)
		return nil
	}
	w.Options = make([]TCPOption, 0, 1+len(data)/4)
	// TODO - should we omit the empty option padding?
	for len(data) > 0 {
		overlay := (*tcpOption)(unsafe.Pointer(&data[0]))
		switch overlay.kind {
		case layers.TCPOptionKindEndList:
			return nil
		case layers.TCPOptionKindNop:
			// For NoOperation, just advance the pointer.
			// No need to save the option.
			data = data[1:]
		default:
			if overlay.len > 38 {
				log.Println("Malformed option field", overlay.kind, overlay.len)
				return ErrMalformedTCPOption
			}
			if len(data) < 2 || len(data) < int(overlay.len) {
				log.Println("Truncated option field:", data)
				return ErrTruncatedTCPHeader
			}
			// copy to a persistent Option struct
			opt := TCPOption{
				Kind: overlay.kind,
				Len:  overlay.len,
				Data: make([]byte, overlay.len-2),
			}
			copy(opt.Data, data[2:overlay.len])
			switch opt.Kind {
			case layers.TCPOptionKindMSS:
				fallthrough
			case layers.TCPOptionKindTimestamps:
				fallthrough
			case layers.TCPOptionKindWindowScale:
				fallthrough
			case layers.TCPOptionKindSACKPermitted:
				fallthrough
			case layers.TCPOptionKindSACK:
				fallthrough
			default:
				if len(data) < int(opt.Len) {
					log.Println("Truncated option field:", data)
					return ErrTruncatedTCPHeader
				}
				w.Options = append(w.Options, opt)
				data = data[opt.Len:]
			}
		}
	}
	return nil
}

func WrapTCP(data []byte) (*TCPHeaderWrapper, error) {
	if len(data) < TCPHeaderSize {
		return nil, ErrTruncatedTCPHeader
	}
	tcp := (*TCPHeader)(unsafe.Pointer(&data[0]))
	if tcp.DataOffset() > len(data) {
		return nil, ErrTruncatedTCPHeader
	}
	w := TCPHeaderWrapper{
		TCP:     tcp,
		Options: nil,
	}
	err := w.parseTCPOptions(data[TCPHeaderSize:tcp.DataOffset()])
	return &w, err
}

// Packet struct contains the packet data and metadata.
type Packet struct {
	// If we use a pointer here, for some reason we get zero value timestamps.
	Ci   gopacket.CaptureInfo
	Data []byte
	eth  *EthernetHeader
	IP
	v4  *IPv4Header       // Nil unless we're parsing IPv4 packets.
	v6  *IPv6Wrapper      // Nil unless we're parsing IPv6 packets.
	tcp *TCPHeaderWrapper // This takes up a small amount of space for the options.
	err error
}

func (p *Packet) TCP() *TCPHeader {
	return p.tcp.TCP
}

// Wrap creates a wrapper with partially decoded headers.
// ci is passed by value, since gopacket NoCopy doesn't preserve the values.
func Wrap(ci *gopacket.CaptureInfo, data []byte) (Packet, error) {
	if len(data) < EthernetHeaderSize {
		return Packet{err: ErrTruncatedEthernetHeader}, ErrTruncatedEthernetHeader
	}
	p := Packet{
		Ci:   *ci, // Make a copy, since gopacket NoCopy doesn't preserve the values.
		Data: data,
		eth:  (*EthernetHeader)(unsafe.Pointer(&data[0])),
	}
	switch p.eth.EtherType() {
	case layers.EthernetTypeIPv4:
		if len(data) < EthernetHeaderSize+IPv4HeaderSize {
			return Packet{err: ErrTruncatedIPHeader}, ErrTruncatedIPHeader
		}
		p.v4 = (*IPv4Header)(unsafe.Pointer(&data[EthernetHeaderSize]))
		p.IP = p.v4
	case layers.EthernetTypeIPv6:
		if len(data) < EthernetHeaderSize+IPv6HeaderSize {
			return Packet{err: ErrTruncatedIPHeader}, ErrTruncatedIPHeader
		}
		var err error
		p.v6, _, err = NewIPv6Header(data[EthernetHeaderSize:])
		if err != nil {
			return Packet{}, err
		}
		p.IP = p.v6
	default:
		return Packet{err: ErrUnknownEtherType}, ErrUnknownEtherType
	}
	// TODO needs more work
	if p.IP != nil {
		switch p.IP.NextProtocol() {
		case layers.IPProtocolTCP:
			var err error
			p.tcp, err = WrapTCP(data[EthernetHeaderSize+p.IP.HeaderLength():])
			if err != nil {
				sparse20.Printf("Error parsing TCP: %v for %v", err, p)
				return Packet{}, err
			}
		}
	}

	return p, nil
}

func (p *Packet) TCPLength() int {
	if p.IP == nil {
		return 0
	}
	return p.IP.PayloadLength()
}

// GetGopacketFirstTCP uses gopacket to decode the  TCP layer for the first packet.
// It is a bit slow and does memory allocation.
func (s *Summary) GetGopacketFirstTCP() (*layers.TCP, error) {
	// Decode a packet.
	pkt := gopacket.NewPacket(s.FirstPacket, layers.LayerTypeEthernet, gopacket.DecodeOptions{
		Lazy:                     true,
		NoCopy:                   true,
		SkipDecodeRecovery:       true,
		DecodeStreamsAsDatagrams: false,
	})

	if tcpLayer := pkt.Layer(layers.LayerTypeTCP); tcpLayer != nil {
		tcp, _ := tcpLayer.(*layers.TCP)
		// For IPv4, the TTL length is the ip.Length adjusted for the header length.
		return tcp, nil
	} else {
		return nil, ErrNoTCPLayer
	}
}

type Summary struct {
	Packets      int
	FirstPacket  []byte
	SrcIP        net.IP
	DstIP        net.IP
	SrcPort      layers.TCPPort
	DstPort      layers.TCPPort
	HopLimit     uint8
	PayloadBytes uint64
	StartTime    time.Time
	LastTime     time.Time
	OptionCounts map[layers.TCPOptionKind]int
	Errors       map[int]error
	Details      []string
}

func (s *Summary) Add(p *Packet) {
	if s.Packets == 0 {
		s.FirstPacket = p.Data[:]
	}
	if p.err != nil {
		s.Errors[s.Packets] = p.err
	} else if s.Packets == 0 {
		s.StartTime = p.Ci.Timestamp
		s.SrcIP = p.SrcIP()
		s.DstIP = p.DstIP()
		s.SrcPort = p.TCP().SrcPort()
		s.DstPort = p.TCP().DstPort()
		s.HopLimit = p.IP.HopLimit()
	} else {
		s.LastTime = p.Ci.Timestamp
		s.PayloadBytes += uint64(p.TCPLength())
	}
	s.Packets++
}

// GetIP decodes the IP layers and returns some basic information.
// It is a bit slow and does memory allocation.
func (s *Summary) GetIP() (net.IP, net.IP, uint8) {
	return s.SrcIP, s.DstIP, s.HopLimit
}

func ProcessPackets(archive, fn string, data []byte) (Summary, error) {
	summary := Summary{
		OptionCounts: make(map[layers.TCPOptionKind]int),
		Errors:       make(map[int]error, 1),
	}

	pcap, err := pcapgo.NewReader(bytes.NewReader(data))
	if err != nil {
		log.Print(err)
		return summary, err
	}

	pktSize := int(pcap.Snaplen())
	if pktSize < 1 {
		pktSize = 1
	}
	pcapSize := len(data) // Only if the data is not compressed.
	// Check magic number?
	if len(data) < 4 {
		return summary, ErrTruncatedPcap
	}
	if data[0] != 0xd4 && data[1] != 0xc3 && data[2] != 0xb2 && data[3] != 0xa1 {
		// For compressed data, the 8x factor is based on testing with a few large gzipped files.
		pcapSize *= 8
	}

	// This computed slice sizing alone changes the throughput in sandbox from about 640
	// to about 820 MB/sec per instance.  No crashes after 2 hours.  GIT b46b033.
	// NOTE that previously, we got about 1.09 GB/sec for just indexing.
	summary.Details = make([]string, 0, pcapSize/pktSize)

	for data, ci, err := pcap.ReadPacketData(); err == nil; data, ci, err = pcap.ZeroCopyReadPacketData() {
		// Pass ci by pointer, but Wrap will make a copy, since gopacket NoCopy doesn't preserve the values.
		p, err := Wrap(&ci, data)
		if err != nil {
			log.Println(archive, fn, err, data)
			summary.Errors[summary.Packets] = err
			continue
		}
		summary.Add(&p)
	}

	if err != nil {
		metrics.WarningCount.WithLabelValues("pcap", "ip_layer_failure").Inc()
		metrics.PcapPacketCount.WithLabelValues("IP error").Observe(float64(summary.Packets))
		return summary, err
	} else if summary.Packets > 0 {
		srcIP, _, _ := summary.GetIP()
		// TODO - eventually we should identify key local ports, like 443 and 3001.
		if err != nil {
			metrics.WarningCount.WithLabelValues("pcap", "?", "ip_layer_failure").Inc()
			metrics.PcapPacketCount.WithLabelValues("IP error").Observe(float64(summary.Packets))
		} else {
			duration := summary.LastTime.Sub(summary.StartTime)
			// TODO add TCP layer, so we can label the stats based on local port value.
			if len(srcIP) == 4 {
				metrics.PcapPacketCount.WithLabelValues("ipv4").Observe(float64(summary.Packets))
				metrics.PcapConnectionDuration.WithLabelValues("ipv4").Observe(duration.Seconds())
			} else {
				metrics.PcapPacketCount.WithLabelValues("ipv6").Observe(float64(summary.Packets))
				metrics.PcapConnectionDuration.WithLabelValues("ipv6").Observe(duration.Seconds())
			}
		}
	} else {
		// No packets.
		metrics.PcapPacketCount.WithLabelValues("unknown").Observe(float64(summary.Packets))
	}

	return summary, nil
}
