package parser

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcapgo"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/go/logx"
)

var (
	info         = log.New(os.Stdout, "info: ", log.LstdFlags|log.Lshortfile)
	sparseLogger = log.New(os.Stdout, "sparse: ", log.LstdFlags|log.Lshortfile)
	sparse20     = logx.NewLogEvery(sparseLogger, 50*time.Millisecond)

	ErrNoIPLayer = fmt.Errorf("no IP layer")
)

// Packet struct contains the packet data and metadata.
type Packet struct {
	// If we use a pointer here, for some reason we get zero value timestamps.
	Ci   gopacket.CaptureInfo
	Data []byte
	Err  error
}

// GetIP decodes the IP layers and returns some basic information.
// It is a bit slow and does memory allocation.
func (p *Packet) GetIP() (net.IP, net.IP, uint8, uint16, error) {
	// Decode a packet.
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

	// TODO: len(data)/18 provides much better estimate of number of packets.
	// len(data)/18 was determined by looking at bytes/packet in a few pcaps files.
	// The number seems too small, but perhaps the data is still compressed at this point.
	// However, it seems to cause mysterious crashes in sandbox, so
	// reverting to /1500 for now.
	packets := make([]Packet, 0, len(data)/1500)

	for data, ci, err := pcap.ZeroCopyReadPacketData(); err == nil; data, ci, err = pcap.ReadPacketData() {
		packets = append(packets, Packet{Ci: ci, Data: data, Err: err})
	}

	if err != nil {
		metrics.WarningCount.WithLabelValues("pcap", "ip_layer_failure").Inc()
		metrics.PcapPacketCount.WithLabelValues("IP error").Observe(float64(len(packets)))
		return packets, err
	} else if len(packets) > 0 {
		srcIP, _, _, _, err := packets[0].GetIP()
		// TODO - eventually we should identify key local ports, like 443 and 3001.
		if err != nil {
			metrics.WarningCount.WithLabelValues("pcap", "ip_layer_failure").Inc()
			metrics.PcapPacketCount.WithLabelValues("IP error").Observe(float64(len(packets)))
		} else {
			start := packets[0].Ci.Timestamp
			end := packets[len(packets)-1].Ci.Timestamp
			duration := end.Sub(start)
			// TODO add TCP layer, so we can label the stats based on local port value.
			if len(srcIP) == 4 {
				metrics.PcapPacketCount.WithLabelValues("ipv4").Observe(float64(len(packets)))
				metrics.PcapConnectionDuration.WithLabelValues("ipv4").Observe(duration.Seconds())
			} else {
				metrics.PcapPacketCount.WithLabelValues("ipv6").Observe(float64(len(packets)))
				metrics.PcapConnectionDuration.WithLabelValues("ipv6").Observe(duration.Seconds())
			}
		}
	} else {
		// No packets.
		metrics.PcapPacketCount.WithLabelValues("unknown").Observe(float64(len(packets)))
	}

	return packets, nil
}

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
func NewPCAPParser(sink row.Sink, table, suffix string) etl.Parser {
	bufSize := etl.PCAP.BQBufferSize()
	return &PCAPParser{
		Base:   row.NewBase(table, sink, bufSize),
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

// ParseAndInsert decodes the PCAP data and inserts it into BQ.
func (p *PCAPParser) ParseAndInsert(meta etl.Metadata, testName string, rawContent []byte) error {
	metrics.WorkerState.WithLabelValues(p.TableName(), "pcap").Inc()
	defer metrics.WorkerState.WithLabelValues(p.TableName(), "pcap").Dec()

	row := schema.PCAPRow{
		Parser: schema.ParseInfo{
			Version:     meta.Version,
			Time:        time.Now(),
			ArchiveURL:  meta.ArchiveURL,
			Filename:    testName,
			GitCommit:   meta.GitCommit,
			ArchiveSize: meta.ArchiveSize,
			FileSize:    int64(len(rawContent)),
		},
	}

	// NOTE: Civil is not TZ adjusted. It takes the year, month, and date from
	// the given timestamp, regardless of the timestamp's timezone. Since we
	// run our systems in UTC, all timestamps will be relative to UTC and as
	// will these dates.
	row.Date = meta.Date
	row.ID = p.GetUUID(testName)

	// Parse top level PCAP data and update metrics.
	// TODO - add schema fields here.
	_, _ = GetPackets(rawContent)

	// Insert the row.
	if err := p.Put(&row); err != nil {
		return err
	}

	// Count successful inserts.
	metrics.TestTotal.WithLabelValues(p.TableName(), "pcap", "ok").Inc()

	return nil
}

//=====================================================================================
//                       Implementation of the etl.Parser interface
//=====================================================================================

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
