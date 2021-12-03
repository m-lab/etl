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
	"github.com/m-lab/etl/tcp"
	"github.com/m-lab/go/logx"
)

var (
	info         = log.New(os.Stdout, "info: ", log.LstdFlags|log.Lshortfile)
	sparseLogger = log.New(os.Stdout, "sparse: ", log.LstdFlags|log.Lshortfile)
	sparse10     = logx.NewLogEvery(sparseLogger, 100*time.Millisecond)

	ErrTruncatedPcap           = fmt.Errorf("Truncated PCAP file")
	ErrNoIPLayer               = fmt.Errorf("no IP layer")
	ErrNoTCPLayer              = fmt.Errorf("no TCP layer")
	ErrTruncatedEthernetHeader = fmt.Errorf("truncated Ethernet header")
	ErrTruncatedIPHeader       = fmt.Errorf("truncated IP header")
	ErrUnknownEtherType        = fmt.Errorf("unknown Ethernet type")
)

// These provide byte swapping when running on LittleEndian systems.
// Much much faster than binary.BigEndian.Uint...

type BE16 [2]byte

func (b BE16) Uint16() uint16 {
	swap := [2]byte{b[1], b[0]}
	return *(*uint16)(unsafe.Pointer(&swap))
}

type BE32 [4]byte

func (b BE32) Uint32() uint32 {
	swap := [4]byte{b[3], b[2], b[1], b[0]}
	return *(*uint32)(unsafe.Pointer(&swap))
}

/******************************************************************************
	 Ethernet Header
******************************************************************************/
type EthernetHeader struct {
	SrcMAC, DstMAC [6]byte
	etherType      BE16 // BigEndian
}

func (e *EthernetHeader) EtherType() layers.EthernetType {
	return layers.EthernetType(e.etherType.Uint16())
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
	length        BE16              // Total length
	id            BE16              // Identification
	flagsFragOff  BE16              // Flags (3 bits) + Fragment offset (13 bits)
	hopLimit      uint8             // Time to live
	protocol      layers.IPProtocol // Protocol of next following bytes, after the options
	checksum      BE16              // Header checksum
	srcIP         BE32              // Source address
	dstIP         BE32              // Destination address
}

var IPv4HeaderSize = int(unsafe.Sizeof(IPv4Header{}))

func (h *IPv4Header) Version() uint8 {
	return (h.versionIHL >> 4)
}

func (h *IPv4Header) PayloadLength() int {
	ihl := h.versionIHL & 0x0f
	return int(h.length.Uint16()) - int(ihl*4)
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
	// TODO - this does not need to be stored, but it is convenient for now.
	//	payload []byte // Any additional data remaining after the extension header.
}

// IPv6Header struct for IPv6 header
type IPv6Header struct {
	versionTrafficClassFlowLabel BE32              // Version (4 bits) + Traffic class (8 bits) + Flow label (20 bits)
	payloadLength                BE16              // Original payload length, NOT the payload size of the captured packet.
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
		return w, nil, nil
	}
	return w, data[w.headerLength:], err
}

func (h *IPv6Header) Version() uint8 {
	return (h.versionTrafficClassFlowLabel[0] >> 4)
}

func (h *IPv6Header) PayloadLength() int {
	return int(h.payloadLength.Uint16())
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

// Packet struct contains the packet data and metadata.
type Packet struct {
	// If we use a pointer here, for some reason we get zero value timestamps.
	Ci   gopacket.CaptureInfo
	Data []byte
	eth  *EthernetHeader
	ip   IP
	v4   *IPv4Header           // Nil unless we're parsing IPv4 packets.
	v6   *IPv6Wrapper          // Nil unless we're parsing IPv6 packets.
	tcp  *tcp.TCPHeaderWrapper // This takes up a small amount of space for the options.
	err  error
}

func (p *Packet) TCP() *tcp.TCPHeaderGo {
	return &p.tcp.TCPHeaderGo
}

// Wrap creates a wrapper with partially decoded headers.
// ci is passed by value, since gopacket NoCopy doesn't preserve the values.
func Wrap(ci *gopacket.CaptureInfo, data []byte) (Packet, error) {
	if len(data) < EthernetHeaderSize {
		//return Packet{err: ErrTruncatedEthernetHeader}, nil
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
		p.ip = p.v4
	case layers.EthernetTypeIPv6:
		if len(data) < EthernetHeaderSize+IPv6HeaderSize {
			return Packet{err: ErrTruncatedIPHeader}, ErrTruncatedIPHeader
		}
		var err error
		p.v6, _, err = NewIPv6Header(data[EthernetHeaderSize:])
		if err != nil {
			return Packet{}, err
		}
		p.ip = p.v6
	default:
		return Packet{err: ErrUnknownEtherType}, ErrUnknownEtherType
	}
	// TODO needs more work
	if p.ip != nil {
		switch p.ip.NextProtocol() {
		case layers.IPProtocolTCP:
			p.tcp = &tcp.TCPHeaderWrapper{}
			err := tcp.WrapTCP(data[EthernetHeaderSize+p.ip.HeaderLength():], p.tcp)
			if err != nil {
				//sparse20.Printf("Error parsing TCP: %v for %v", err, p)
				return Packet{}, err
			}
		}
	}

	return p, nil
}

func (p *Packet) PayloadLength() int {
	if p.ip == nil {
		return 0
	}
	return p.ip.PayloadLength()
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

	LeftState  *tcp.State
	RightState *tcp.State
}

func (s *Summary) Add(p *Packet) {
	ip := p.ip
	tcpw := p.tcp
	raw := p.Data
	t := p.Ci.Timestamp

	if s.Packets == 0 {
		s.FirstPacket = raw[:]
		// ESCAPE These are escaping to the heap.
		s.LeftState = tcp.NewState(ip.SrcIP(), tcpw.SrcPort)
		s.RightState = tcp.NewState(ip.DstIP(), tcpw.DstPort)
		s.StartTime = t
		s.SrcIP = ip.SrcIP()
		s.DstIP = ip.DstIP()
		s.SrcPort = tcpw.SrcPort
		s.DstPort = tcpw.DstPort
		s.HopLimit = ip.HopLimit()
	} else {
		s.LastTime = t
	}

	s.PayloadBytes += uint64(p.PayloadLength())
	tcpheader := raw[EthernetHeaderSize+p.ip.HeaderLength():]
	optData := tcpheader[tcp.TCPHeaderSize : 4*int(tcpw.DataOffset>>4)]

	s.LeftState.Update(s.Packets, p.ip.SrcIP(), p.ip.DstIP(), uint16(p.PayloadLength()), p.TCP(), optData, p.Ci)
	s.RightState.Update(s.Packets, p.ip.SrcIP(), p.ip.DstIP(), uint16(p.PayloadLength()), p.TCP(), optData, p.Ci)
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
	//summary.Details = make([]string, 0, pcapSize/pktSize)

	for data, ci, err := pcap.ReadPacketData(); err == nil; {
		// Pass ci by pointer, but Wrap will make a copy, since gopacket NoCopy doesn't preserve the values.
		p, werr := Wrap(&ci, data)
		if werr != nil {
			sparse10.Println(archive, fn, err, data)
			summary.Errors[summary.Packets] = err
			continue // BUG
		}

		if p.err == nil {
			// This now includes some of the TCP state modelling.
			summary.Add(&p)
		}
		data, ci, err = pcap.ReadPacketData()
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
