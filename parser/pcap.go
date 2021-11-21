package parser

import (
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

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

func extractIPFields(packet gopacket.Packet) (srcIP, dstIP net.IP, TTL uint8, tcpLength uint16, err error) {
	if ipLayer := packet.Layer(layers.LayerTypeIPv4); ipLayer != nil {
		ip, _ := ipLayer.(*layers.IPv4)
		// For IPv4, the TTL length is the ip.Length adjusted for the header length.
		return ip.SrcIP, ip.DstIP, ip.TTL, ip.Length - uint16(4*ip.IHL), nil
	} else if ipLayer := packet.Layer(layers.LayerTypeIPv6); ipLayer != nil {
		ip, _ := ipLayer.(*layers.IPv6)
		// In IPv6, the Length field is the payload length.
		return ip.SrcIP, ip.DstIP, ip.HopLimit, ip.Length, nil
	} else {
		return nil, nil, 0, 0, ErrNoIPLayer
	}
}

type Packet struct {
	// If we use a pointer here, for some reason we get zero value timestamps.
	Ci   gopacket.CaptureInfo
	Data []byte
	Err  error
}

var info = log.New(os.Stdout, "info: ", log.LstdFlags|log.Lshortfile)
var sparseLogger = log.New(os.Stdout, "sparse: ", log.LstdFlags|log.Lshortfile)
var sparse20 = logx.NewLogEvery(sparseLogger, 50*time.Millisecond)

var ErrNoIPLayer = fmt.Errorf("no IP layer")

func (p *Packet) GetIP() (net.IP, net.IP, uint8, uint16, error) {
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
		srcIP, _, _, _, err := packets[0].GetIP()
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
