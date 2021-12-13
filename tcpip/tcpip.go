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
	"fmt"
	"log"
	"net"
	"os"
	"time"
	"unsafe"

	"github.com/google/gopacket/layers"

	"github.com/m-lab/annotation-service/site"
	"github.com/m-lab/etl/headers"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/go/logx"
	"github.com/m-lab/uuid-annotator/annotator"
)

var (
	info         = log.New(os.Stdout, "info: ", log.LstdFlags|log.Lshortfile)
	sparseLogger = log.New(os.Stdout, "sparse: ", log.LstdFlags|log.Lshortfile)
	sparse1      = logx.NewLogEvery(sparseLogger, 1000*time.Millisecond)

	ErrTruncatedPcap           = fmt.Errorf("Truncated PCAP file")
	ErrNoIPLayer               = fmt.Errorf("no IP layer")
	ErrNoTCPLayer              = fmt.Errorf("no TCP layer")
	ErrTruncatedEthernetHeader = fmt.Errorf("truncated Ethernet header")
	ErrTruncatedIPHeader       = fmt.Errorf("truncated IP header")
	ErrUnknownEtherType        = fmt.Errorf("unknown Ethernet type")
)

type UnixNano int64

func (t UnixNano) Sub(other UnixNano) time.Duration {
	return time.Duration(t - other)
}

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
	SrcIP(net.IP) net.IP
	DstIP(net.IP) net.IP
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

func (h *IPv4Header) SrcIP(ip net.IP) net.IP {
	if ip == nil {
		ip = make(net.IP, 4)
	}
	ip = append(ip[:0], h.srcIP[:]...)
	return ip
}

// DstIP returns the destination IP address of the packet.
func (h *IPv4Header) DstIP(ip net.IP) net.IP {
	if ip == nil {
		ip = make(net.IP, 4)
	}
	ip = append(ip[:0], h.dstIP[:]...)
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

func MakeIPv6Header(data []byte) (*IPv6Header, []byte, error) {
	if len(data) < int(unsafe.Sizeof(IPv6Header{})) {
		return nil, nil, ErrTruncatedIPHeader
	}
	h := (*IPv6Header)(unsafe.Pointer(&data[0]))
	if h.Version() != 6 {
		return nil, nil, fmt.Errorf("IPv6 packet with version %d", h.Version())
	}
	return h, data[IPv6HeaderSize:], nil
}

// FromData creates a new IPv6 header, and returns the header and remaining bytes.
func (w *IPv6Wrapper) FromData(data []byte) (rem []byte, err error) {
	var h *IPv6Header
	h, _, err = MakeIPv6Header(data)
	if err != nil {
		return nil, err
	}
	err = w.Populate(h, data)
	if err != nil {
		return nil, err
	}
	if len(data) < w.headerLength {
		return nil, ErrTruncatedIPHeader

		// TODO remove this check.
	} else if len(data) == w.headerLength {
		return nil, nil
	}
	return data[w.headerLength:], err
}

func (h *IPv6Header) Version() uint8 {
	return (h.versionTrafficClassFlowLabel[0] >> 4)
}

func (h *IPv6Header) PayloadLength() int {
	return int(h.payloadLength.Uint16())
}

func (h *IPv6Header) SrcIP(ip net.IP) net.IP {
	if ip == nil {
		ip = make(net.IP, 16)
	}
	ip = append(ip[:0], h.srcIP[:]...)
	return ip
}

// DstIP returns the destination IP address of the packet.
func (h *IPv6Header) DstIP(ip net.IP) net.IP {
	if ip == nil {
		ip = make(net.IP, 16)
	}
	ip = append(ip[:0], h.dstIP[:]...)
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

func (w *IPv6Wrapper) Populate(ip *IPv6Header, data []byte) error {
	if w == nil {
		return fmt.Errorf("nil IPv6Wrapper")
	}
	w.IPv6Header = ip
	if w.ext == nil {
		w.ext = make([]EHWrapper, 0, 0)
	}
	w.ext = w.ext[:0]

	if w.nextHeader == layers.IPProtocolNoNextHeader {
		return nil
	}

	for np := w.NextProtocol(); np != layers.IPProtocolNoNextHeader; {
		switch np {
		case layers.IPProtocolNoNextHeader:
			return nil
		case layers.IPProtocolIPv6HopByHop:
		case layers.IPProtocolTCP:
			return nil
		default:
			log.Println("IPv6 header type", np)
		}

		if len(data) < 8 {
			return ErrTruncatedIPHeader
		}

		eh := (*ExtensionHeader)(unsafe.Pointer(&data[0]))
		if len(data) < int(8+eh.HeaderLength) {
			return ErrTruncatedIPHeader
		}
		w.ext = append(w.ext, EHWrapper{
			HeaderType: np,
			eh:         eh,
			data:       data[2 : 8+eh.HeaderLength],
		})
		w.headerLength += int(eh.HeaderLength) + 8
		data = data[8+eh.HeaderLength:]
		np = eh.NextHeader
	}
	return ErrTruncatedIPHeader
}

// Wrap creates a wrapper with extension headers.
// data is the remainder of the header data, not including the IPv6 header.
func (ip *IPv6Header) xWrap(data []byte) (*IPv6Wrapper, error) {
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
	pTime UnixNano
	Data  []byte
	eth   *EthernetHeader
	ip    IP
	v4    *IPv4Header  // Nil unless we're parsing IPv4 packets.
	v6    *IPv6Wrapper // Nil unless we're parsing IPv6 packets.
	err   error
}

func (p *Packet) From(hp *headers.Packet) error {
	data := hp.Data()
	if len(data) < EthernetHeaderSize {
		p.err = ErrTruncatedEthernetHeader
		return p.err
	}
	p.Data = data
	p.pTime = UnixNano(hp.UnixNano()) // make a copy
	p.eth = (*EthernetHeader)(unsafe.Pointer(&data[0]))

	switch p.eth.EtherType() {
	case layers.EthernetTypeIPv4:
		if len(data) < EthernetHeaderSize+IPv4HeaderSize {
			p.err = ErrTruncatedIPHeader
			return p.err
		}
		p.v4 = (*IPv4Header)(unsafe.Pointer(&data[EthernetHeaderSize]))
		p.ip = p.v4
	case layers.EthernetTypeIPv6:
		if len(data) < EthernetHeaderSize+IPv6HeaderSize {
			p.err = ErrTruncatedIPHeader
			return p.err
		}
		if p.v6 == nil {
			p.v6 = &IPv6Wrapper{}
		}
		_, p.err = p.v6.FromData(data[EthernetHeaderSize:])
		if p.err != nil {
			return p.err
		}
		p.ip = p.v6
	default:
		p.err = ErrUnknownEtherType
		return p.err
	}
	if p.ip != nil {
		switch p.ip.NextProtocol() {
		case layers.IPProtocolTCP:
			// TODO - add TCP layer decoding
		}
	}

	return nil
}

func (p *Packet) PayloadLength() int {
	if p.ip == nil {
		return 0
	}
	return p.ip.PayloadLength()
}

type Stats struct {
	SrcIP   net.IP
	Packets int
	Bytes   int
}

type Summary struct {
	init bool

	HopLimit  uint8
	Packets   int
	StartTime UnixNano
	LastTime  UnixNano

	Left, Right Stats

	// These eventually point to the server and client stats.
	server, client *Stats

	srcIP, dstIP net.IP
}

func (s *Summary) Client() Stats {
	if s.client == nil {
		return Stats{}
	}
	return *s.client
}

func (s *Summary) Server() Stats {
	if s.client == nil {
		return Stats{}
	}
	return *s.server
}

func (s *Summary) Add(p *Packet) {
	ip := p.ip

	s.srcIP = ip.SrcIP(s.srcIP) // ESCAPE - these reduce escapes to the heap
	s.dstIP = ip.DstIP(s.dstIP)
	if !s.init {
		s.StartTime = p.pTime
		s.HopLimit = ip.HopLimit()

		s.Left.SrcIP = append([]byte{}, s.srcIP[:]...)
		s.Right.SrcIP = append([]byte{}, s.dstIP[:]...)

		s.init = true
	} else {
		if p.pTime.Sub(s.LastTime) > time.Second {
			// TODO make this a metric.
			sparse1.Printf("Time jump %08X %v", p.pTime, p.pTime.Sub(s.LastTime))
		}

		if p.pTime.Sub(s.LastTime) < 0 {
			// TODO make this a metric.
			sparse1.Printf("Time travel %08X %v", p.pTime, p.pTime.Sub(s.LastTime))
		}
	}

	s.LastTime = p.pTime

	if s.srcIP.Equal(s.Left.SrcIP) {
		s.Left.Packets++
		s.Left.Bytes += p.PayloadLength()
	} else if s.srcIP.Equal(s.Right.SrcIP) {
		s.Right.Packets++
		s.Right.Bytes += p.PayloadLength()
	} else {
		// TODO
	}
	s.Packets++
}

func (s *Summary) Finish() bool {
	if !s.init {
		return false
	}
	leftAnn := annotator.ServerAnnotations{}
	site.Annotate(s.Left.SrcIP.String(), &leftAnn)
	rightAnn := annotator.ServerAnnotations{}
	site.Annotate(s.Right.SrcIP.String(), &rightAnn)
	if leftAnn.Site != "" {
		s.server = &s.Left
		s.client = &s.Right
		return true
	} else if rightAnn.Site != "" {
		s.server = &s.Right
		s.client = &s.Left
		return true
	}
	return false
}

func ProcessPackets(archive, fn string, data []byte) (Summary, error) {
	// ESCAPE maps are escaping to the heap
	summary := Summary{}

	pr, err := headers.NewPCAPReader(data)
	if err != nil {
		log.Print(err)
		return summary, err
	}

	p := Packet{}
	hp := headers.Packet{}
	for err := pr.Next(&hp); err == nil; err = pr.Next(&hp) {
		// Pass ci by pointer, but Wrap will make a copy, since gopacket NoCopy doesn't preserve the values.
		err := p.From(&hp)
		if err != nil {
			sparse1.Println(archive, fn, err, data)
			continue
		}
		summary.Add(&p)
	}

	if err != nil {
		metrics.WarningCount.WithLabelValues("pcap", "ip_layer_failure").Inc()
		metrics.PcapPacketCount.WithLabelValues("IP error").Observe(float64(summary.Packets))
		return summary, err
	} else if summary.Finish() {
		serverIP := summary.Server().SrcIP
		// TODO - eventually we should identify key local ports, like 443 and 3001.
		duration := summary.LastTime.Sub(summary.StartTime)
		// TODO add TCP layer, so we can label the stats based on local port value.
		if len(serverIP) == 4 {
			metrics.PcapPacketCount.WithLabelValues("ipv4").Observe(float64(summary.Packets))
			metrics.PcapConnectionDuration.WithLabelValues("ipv4").Observe(duration.Seconds())
		} else {
			metrics.PcapPacketCount.WithLabelValues("ipv6").Observe(float64(summary.Packets))
			metrics.PcapConnectionDuration.WithLabelValues("ipv6").Observe(duration.Seconds())
		}
	} else {
		// Server IP not found in the summary.
		metrics.PcapPacketCount.WithLabelValues("unknown").Observe(float64(summary.Packets))
	}

	return summary, nil
}
