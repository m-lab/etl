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
	sparse1      = logx.NewLogEvery(sparseLogger, 1000*time.Millisecond)

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
	pTime tcp.UnixNano
	Data  []byte
	eth   *EthernetHeader
	ip    IP
	v4    *IPv4Header  // Nil unless we're parsing IPv4 packets.
	v6    *IPv6Wrapper // Nil unless we're parsing IPv6 packets.
	tcp   *tcp.TCPHeaderGo
	err   error
}

func (p *Packet) TCP() *tcp.TCPHeaderGo {
	return p.tcp
}

func (p *Packet) From(ci *gopacket.CaptureInfo, data []byte) error {

	if len(data) < EthernetHeaderSize {
		p.err = ErrTruncatedEthernetHeader
		return p.err
	}
	p.Data = data
	p.pTime = tcp.UnixNano(ci.Timestamp.UnixNano()) // make a copy
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
		p.v6, _, p.err = NewIPv6Header(data[EthernetHeaderSize:])
		if p.err != nil {
			return p.err
		}
		p.ip = p.v6
	default:
		p.err = ErrUnknownEtherType
		return p.err
	}
	// TODO needs more work
	if p.ip != nil {
		switch p.ip.NextProtocol() {
		case layers.IPProtocolTCP:
			if p.tcp == nil {
				p.tcp = &tcp.TCPHeaderGo{}
			}
			p.err = p.tcp.From(data[EthernetHeaderSize+p.ip.HeaderLength():])
			if p.err != nil {
				//sparse20.Printf("Error parsing TCP: %v for %v", err, p)
				return p.err
			}
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

type Summary struct {
	Packets             int
	FirstPacket         []byte
	SrcIP               net.IP
	DstIP               net.IP
	SrcPort             layers.TCPPort
	DstPort             layers.TCPPort
	HopLimit            uint8
	PayloadBytes        uint64
	StartTime, LastTime tcp.UnixNano
	OptionCounts        map[layers.TCPOptionKind]int
	Errors              map[int]error
	Details             []string

	LeftState  *tcp.State
	RightState *tcp.State
}

func (s *Summary) Add(p *Packet) {
	ip := p.ip
	tcpw := p.tcp
	raw := p.Data
	var bSrc, bDst [16]byte
	//srcIP := make(net.IP, 16)
	//dstIP := make(net.IP, 16)

	srcIP := ip.SrcIP(bSrc[:]) // ESCAPE - these reduce escapes to the heap
	dstIP := ip.DstIP(bDst[:])
	if s.Packets == 0 {
		s.FirstPacket = raw[:]
		// ESCAPE These are escaping to the heap.
		s.LeftState = tcp.NewState(srcIP, tcpw.SrcPort)
		s.RightState = tcp.NewState(dstIP, tcpw.DstPort)
		s.StartTime = p.pTime
		s.SrcIP = srcIP
		s.DstIP = dstIP
		s.SrcPort = tcpw.SrcPort
		s.DstPort = tcpw.DstPort
		s.HopLimit = ip.HopLimit()
	} else {
		s.LastTime = p.pTime
	}

	payloadLength := p.PayloadLength() // Optimization because p.Payload was using 2.5% of the CPU time.
	s.PayloadBytes += uint64(payloadLength)
	tcpheader := raw[EthernetHeaderSize+p.ip.HeaderLength():]
	optData := tcpheader[tcp.TCPHeaderSize:tcpw.DataOffset]

	s.LeftState.Update(s.Packets, srcIP, dstIP, uint16(payloadLength), p.TCP(), optData, p.pTime)
	s.RightState.Update(s.Packets, srcIP, dstIP, uint16(payloadLength), p.TCP(), optData, p.pTime)
	s.Packets++
}

// GetIP decodes the IP layers and returns some basic information.
// It is a bit slow and does memory allocation.
func (s *Summary) GetIP() (net.IP, net.IP, uint8) {
	return s.SrcIP, s.DstIP, s.HopLimit
}

func isGZip(data []byte) bool {
	return bytes.HasPrefix(data, []byte{0x1F, 0x8B})
}

func isPlainPCAP(data []byte) bool {
	return bytes.HasPrefix(data, []byte{0xd4, 0xc3, 0xb2, 0xa1})
}

func ProcessPackets(archive, fn string, data []byte) (Summary, error) {
	// ESCAPE maps are escaping to the heap
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

	if isGZip(data) {
		// For compressed data, the 8x factor is based on testing with a few large gzipped files.
		pcapSize *= 8
	}

	// This computed slice sizing alone changes the throughput in sandbox from about 640
	// to about 820 MB/sec per instance.  No crashes after 2 hours.  GIT b46b033.
	// NOTE that previously, we got about 1.09 GB/sec for just indexing.
	// summary.Details = make([]string, 0, pcapSize/pktSize)

	p := Packet{}
	for data, ci, err := pcap.ReadPacketData(); err == nil; data, ci, err = pcap.ZeroCopyReadPacketData() {
		// Pass ci by pointer, but Wrap will make a copy, since gopacket NoCopy doesn't preserve the values.
		err := p.From(&ci, data)
		if err != nil {
			log.Println(archive, fn, err, data)
			summary.Errors[summary.Packets] = err
			continue
		}
		// This now includes some of the TCP state modelling.
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

	// Log jitter stats for 1 pcap file per second
	ls := summary.LeftState
	rs := summary.RightState
	l10, l50, l90 := ls.SeqTracker.Stats(true)
	r10, r50, r90 := rs.SeqTracker.Stats(true)
	sparse1.Printf("Large: %b  Packets: %d/%d  Left: jitter %6.4f(%6.4f)    delay %9.4f(%9.4f)  acks: %6.5f %6.5f %6.5f,   Right:  jitter %6.4f(%6.4f)    delay %9.4f(%9.4f)  acks: %6.5f %6.5f %6.5f",
		ls.SeqTracker.Acked() > 1000 || rs.SeqTracker.Acked() > 1000, ls.SeqTracker.Acked(), rs.SeqTracker.Acked(),
		ls.Jitter.LRJitter(), ls.Jitter.Jitter(), ls.Jitter.LRDelay0().Seconds(), ls.Jitter.Delay(),
		l10, l50, l90,
		rs.Jitter.LRJitter(), rs.Jitter.Jitter(), rs.Jitter.LRDelay0().Seconds(), rs.Jitter.Delay(),
		r10, r50, r90)

	return summary, nil
}
