package parser

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unsafe"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcapgo"
	v2as "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/go/logx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//=====================================================================================
//                       PCAP Parser
//=====================================================================================

const pcapSuffix = ".pcap.gz"

var ErrNotTCP = fmt.Errorf("not a TCP packet")

// PCAPParser parses the PCAP datatype from the packet-headers process.
type PCAPParser struct {
	*row.Base
	table  string
	suffix string
}

// NewPCAPParser returns a new parser for PCAP archives.
func NewPCAPParser(sink row.Sink, table, suffix string, ann v2as.Annotator) etl.Parser {
	bufSize := etl.PCAP.BQBufferSize()
	if ann == nil {
		ann = v2as.GetAnnotator(etl.BatchAnnotatorURL)
	}

	return &PCAPParser{
		Base:   row.NewBase(table, sink, bufSize, ann),
		table:  table,
		suffix: suffix,
	}

}

// IsParsable returns the canonical test type and whether to parse data.
func (p *PCAPParser) IsParsable(testName string, data []byte) (string, bool) {
	// Files look like (.*).pcap.gz .
	if strings.HasSuffix(testName, pcapSuffix) {
		return "pcap", true
	}
	return "", false
}

type EthernetHeader struct {
	SrcMAC, DstMAC [6]byte
	etherType      [2]byte // BigEndian
}

func (e *EthernetHeader) EtherType() layers.EthernetType {
	return layers.EthernetType(binary.BigEndian.Uint16(e.etherType[:]))
}

// IPv4Header struct for IPv4 header
type IPv4Header struct {
	VersionIHL   uint8   // Version (4 bits) + Internet header length (4 bits)
	TOS          uint8   // Type of service
	Length       [2]byte // Total length
	Id           [2]byte // Identification
	FlagsFragOff [2]byte // Flags (3 bits) + Fragment offset (13 bits)
	TTL          uint8   // Time to live
	Protocol     uint8   // Protocol of next following bytes
	Checksum     [2]byte // Header checksum
	SrcIP        [4]byte // Source address
	DstIP        [4]byte // Destination address
}

func (h *IPv4Header) PayloadLength() uint16 {
	ihl := h.VersionIHL & 0x0f
	return binary.BigEndian.Uint16(h.Length[:]) - uint16(4*ihl)
}

// IPv6Header struct for IPv6 header
type IPv6Header struct {
	VersionTrafficClassFlowLabel [4]byte // Version (4 bits) + Traffic class (8 bits) + Flow label (20 bits)
	Length                       [2]byte // Payload length
	NextHeader                   uint8   // Protocol of next following bytes
	HopLimit                     uint8   // Hop limit
	SrcIP                        [16]byte
	DstIP                        [16]byte
}

type Packet struct {
	// If we use a pointer here, for some reason we get zero value timestamps.
	Ci   gopacket.CaptureInfo
	Data []byte
	Eth  *EthernetHeader
	IPv4 *IPv4Header // Nil unless we're parsing IPv4 packets.
	IPv6 *IPv6Header // Nil unless we're parsing IPv6 packets.
	Err  error
}

var info = log.New(os.Stdout, "info: ", log.LstdFlags|log.Lshortfile)
var sparseLogger = log.New(os.Stdout, "sparse: ", log.LstdFlags|log.Lshortfile)
var sparse20 = logx.NewLogEvery(sparseLogger, 50*time.Millisecond)

var ErrNoIPLayer = fmt.Errorf("no IP layer")
var ErrTruncatedIPHeader = fmt.Errorf("truncated IP header")

func (p *Packet) GetLayers() error {
	p.Eth = (*EthernetHeader)(unsafe.Pointer(&p.Data[0]))
	switch p.Eth.EtherType() {
	case layers.EthernetTypeIPv4:
		if len(p.Data) < int(14+unsafe.Sizeof(IPv4Header{})) {
			return ErrTruncatedIPHeader
		}
		p.IPv4 = (*IPv4Header)(unsafe.Pointer(&p.Data[14]))
	case layers.EthernetTypeIPv6:
		if len(p.Data) < int(14+unsafe.Sizeof(IPv6Header{})) {
			return ErrTruncatedIPHeader
		}
		p.IPv6 = (*IPv6Header)(unsafe.Pointer(&p.Data[14]))
	default:
		return ErrNoIPLayer
	}
	return nil
}

func (p *Packet) TCPLength() int {
	if p.IPv4 != nil {
		return int(p.IPv4.PayloadLength())
	}
	return int(binary.BigEndian.Uint16(p.IPv6.Length[:]))
}

// FastExtractIPFields extracts a few IP fields from the packet.
func (p *Packet) FastExtractIPFields() (srcIP, dstIP net.IP, TTL uint8, tcpLength uint16, err error) {
	if p.Eth == nil {
		err = p.GetLayers()
		if err != nil {
			return nil, nil, 0, 0, err
		}
	}
	if p.IPv4 != nil {
		srcIP = make([]byte, 4)
		dstIP = make([]byte, 4)
		binary.BigEndian.PutUint32(srcIP, binary.BigEndian.Uint32(p.IPv4.SrcIP[:]))
		binary.BigEndian.PutUint32(dstIP, binary.BigEndian.Uint32(p.IPv4.DstIP[:]))
		TTL = p.IPv4.TTL
		tcpLength = p.IPv4.PayloadLength()
		if p.IPv4.Protocol != uint8(layers.IPProtocolTCP) {
			err = ErrNotTCP
		}
	} else if p.IPv6 != nil {
		srcIP = make([]byte, 16)
		dstIP = make([]byte, 16)
		// TODO - just copy!!
		for i := 0; i < 16; i++ {
			srcIP[i] = p.IPv6.SrcIP[i]
			dstIP[i] = p.IPv6.DstIP[i]
		}
		TTL = p.IPv6.HopLimit
		tcpLength = binary.BigEndian.Uint16(p.IPv6.Length[:])
		if p.IPv6.NextHeader != uint8(layers.IPProtocolTCP) {
			err = ErrNotTCP
		}
	} else {
		return nil, nil, 0, 0, ErrNoIPLayer
	}
	return
}

func (p *Packet) SlowGetIP() (net.IP, net.IP, uint8, uint16, error) {
	// Decode a packet
	pkt := gopacket.NewPacket(p.Data, layers.LayerTypeEthernet, gopacket.DecodeOptions{
		Lazy:                     true,
		NoCopy:                   true,
		SkipDecodeRecovery:       true,
		DecodeStreamsAsDatagrams: false,
	})
	if ipLayer := pkt.Layer(layers.LayerTypeIPv4); ipLayer != nil {
		ip, _ := ipLayer.(*layers.IPv4)
		// For IPv4, the TTL length is the ip.Length adjusted for the header length.
		return ip.SrcIP, ip.DstIP, ip.TTL, ip.Length - uint16(4*ip.IHL), nil
	} else if ipLayer := pkt.Layer(layers.LayerTypeIPv6); ipLayer != nil {
		ip, _ := ipLayer.(*layers.IPv6)
		// In IPv6, the Length field is the payload length.
		return ip.SrcIP, ip.DstIP, ip.HopLimit, ip.Length, nil
	} else {
		return nil, nil, 0, 0, ErrNoIPLayer
	}
}

func GetPackets(data []byte) ([]Packet, error) {
	pcap, err := pcapgo.NewReader(strings.NewReader(string(data)))
	if err != nil {
		log.Print(err)
		return nil, err
	}

	// TODO - should we use MSS instead?
	packets := make([]Packet, 0, len(data)/1500)

	for data, ci, err := pcap.ZeroCopyReadPacketData(); err == nil; data, ci, err = pcap.ReadPacketData() {
		packets = append(packets, Packet{Ci: ci, Data: data, Err: err})
	}

	return packets, nil
}

var PcapPacketCount = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: "etl_pcap_packet_count",
		Help: "Distribution of PCAP packet counts",
		Buckets: []float64{
			1, 2, 3, 5,
			10, 18, 32, 56,
			100, 178, 316, 562,
			1000, 1780, 3160, 5620,
			10000, 17800, 31600, 56200, math.Inf(1),
		},
	},
	[]string{"port"},
)

var PcapConnectionDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: "etl_pcap_connection_duration",
		Help: "Distribution of PCAP connection duration",
		Buckets: []float64{
			.1, .2, .3, .5,
			1, 1.8, 3.2, 5.6,
			10, 18, 32, 56,
			100, 178, 316, 562,
			1000, 1780, 3160, 5620, math.Inf(1),
		},
	},
	[]string{"port"},
)

// ParseAndInsert decodes the PCAP data and inserts it into BQ.
func (p *PCAPParser) ParseAndInsert(fileMetadata map[string]bigquery.Value, testName string, rawContent []byte) error {
	metrics.WorkerState.WithLabelValues(p.TableName(), "pcap").Inc()
	defer metrics.WorkerState.WithLabelValues(p.TableName(), "pcap").Dec()

	row := schema.PCAPRow{
		Parser: schema.ParseInfo{
			Version:    Version(),
			Time:       time.Now(),
			ArchiveURL: fileMetadata["filename"].(string),
			Filename:   testName,
			GitCommit:  GitCommit(),
		},
	}

	// NOTE: Civil is not TZ adjusted. It takes the year, month, and date from
	// the given timestamp, regardless of the timestamp's timezone. Since we
	// run our systems in UTC, all timestamps will be relative to UTC and as
	// will these dates.
	row.Date = fileMetadata["date"].(civil.Date)
	row.ID = p.GetUUID(testName)

	// Parse top level PCAP data.
	packets, err := GetPackets(rawContent)
	if err != nil {
		metrics.WarningCount.WithLabelValues("pcap", "ip_layer_failure").Inc()
		PcapPacketCount.WithLabelValues("IP error").Observe(float64(len(packets)))
	} else if len(packets) > 0 {
		srcIP, _, _, _, err := packets[0].FastExtractIPFields()
		// TODO - eventually we should identify key local ports, like 443 and 3001.
		if err != nil {
			metrics.WarningCount.WithLabelValues("pcap", "ip_layer_failure").Inc()
			PcapPacketCount.WithLabelValues("IP error").Observe(float64(len(packets)))
		} else {
			start := packets[0].Ci.Timestamp
			end := packets[len(packets)-1].Ci.Timestamp
			duration := end.Sub(start)
			// TODO add TCP layer, so we can label the stats based on local port value.
			if len(srcIP) == 4 {
				PcapPacketCount.WithLabelValues("ipv4").Observe(float64(len(packets)))
				PcapConnectionDuration.WithLabelValues("ipv4").Observe(duration.Seconds())
			} else {
				PcapPacketCount.WithLabelValues("ipv6").Observe(float64(len(packets)))
				PcapConnectionDuration.WithLabelValues("ipv6").Observe(duration.Seconds())
			}
			//start := time.Now()
			total := 0
			for i := range packets {
				_, _, _, length, err := packets[i].FastExtractIPFields()
				if err != nil {
					total += int(length)
				}
			}
		}
	} else {
		// No packets.
		PcapPacketCount.WithLabelValues("unknown").Observe(float64(len(packets)))
	}

	// Insert the row.
	if err := p.Put(&row); err != nil {
		return err
	}

	// Count successful inserts.
	metrics.TestCount.WithLabelValues(p.TableName(), "pcap", "ok").Inc()

	return nil
}

// GetUUID extracts the UUID from the filename.
// For example, for filename 2021/07/22/ndt-4c6fb_1625899199_00000000013A4623.pcap.gz,
// it returns ndt-4c6fb_1625899199_00000000013A4623.
func (p *PCAPParser) GetUUID(filename string) string {
	id := filepath.Base(filename)
	return strings.TrimSuffix(id, pcapSuffix)
}

// NB: These functions are also required to complete the etl.Parser interface
// For PCAP, we just forward the calls to the Inserter.

func (p *PCAPParser) Flush() error {
	return p.Base.Flush()
}

func (p *PCAPParser) TableName() string {
	return p.table
}

func (p *PCAPParser) FullTableName() string {
	return p.table + p.suffix
}

// RowsInBuffer returns the count of rows currently in the buffer.
func (p *PCAPParser) RowsInBuffer() int {
	return p.GetStats().Pending
}

// Committed returns the count of rows successfully committed to BQ.
func (p *PCAPParser) Committed() int {
	return p.GetStats().Committed
}

// Accepted returns the count of all rows received through InsertRow(s).
func (p *PCAPParser) Accepted() int {
	return p.GetStats().Total()
}

// Failed returns the count of all rows that could not be committed.
func (p *PCAPParser) Failed() int {
	return p.GetStats().Failed
}
