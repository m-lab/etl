package parser

import (
	"fmt"
	"log"
	"net"
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

type wrappingCounter struct {
	initialized bool
	clock       uint32
	value       uint64
}

func (w *wrappingCounter) delta(clock uint32) int64 {
	delta := int64(clock) - int64(w.clock)
	if delta < -1<<31 {
		delta += 1 << 30
	}
	if delta > 1<<30 || delta < -1<<30 {
		log.Fatal("invalid counter delta")
	}
	return delta
}

// Returns true if seq is earlier than the max observed seq.
func (w *wrappingCounter) Update(clock uint32) bool {
	if !w.initialized {
		w.clock = clock
		w.initialized = true
	}
	delta := w.delta(clock)
	if delta < 0 {
		//log.Printf("Retransmit?: %d < %d, (%d)", clock, w.clock, w.value)
		return true
	} else {
		w.value += uint64(delta)
		w.clock = clock
	}
	return false
}

func (w *wrappingCounter) Value() uint64 {
	return w.value
}

// state keep track of the TCP state of one side of a connection.
// TODO - add histograms for Ack inter-arrival time.
type state struct {
	maxSeq uint32

	SrcIP net.IP
	TTL   uint8

	SrcPort            layers.TCPPort  // When this port is SrcPort, we update this stat struct.
	Sent               uint64          // Number of bytes sent in tcp payloads.
	Seq                wrappingCounter // This should match the previous value of Sent.
	Ack                wrappingCounter
	Sacks              uint64
	LastPacketTimeUsec uint64
	Window             uint16
	Retransmits        uint64
	ECECount           uint64
	TTLChanges         uint64 // Observed number of TTL values that don't match first IP header.
}

func (s *state) Update(tcp *layers.TCP, ci gopacket.CaptureInfo, sacks int) {
	if tcp.SrcPort == s.SrcPort {
		if s.Seq.Update(tcp.Seq) {
			s.Retransmits++
		} else {
			// If this is NOT a retransmit, update the Sent value.
			s.Sent += uint64(ci.Length - int(tcp.DataOffset*4))
		}
		s.LastPacketTimeUsec = uint64(ci.Timestamp.UnixNano() / 1000)
		s.Window = tcp.Window
		if tcp.ECE {
			s.ECECount++
		}
	} else {
		if tcp.ACK {
			s.Ack.Update(tcp.Ack)
		}
		s.Sacks += uint64(sacks)
	}
}

func (s state) String() string {
	return fmt.Sprintf("[%v:%5d %d %12d/%10d/%10d %8d win:%5d sacks:%4d retrans:%4d ece:%4d]", s.SrcIP, s.SrcPort, s.TTLChanges, s.Sent, s.Seq.Value(), s.Ack.Value(), s.LastPacketTimeUsec%10000000, s.Window, s.Sacks, s.Retransmits, s.ECECount)
}

// ParseAndInsert decodes the PCAP data and inserts it into BQ.
func (p *PCAPParser) ParseAndInsert(fileMetadata map[string]bigquery.Value, testName string, rawContent []byte) error {
	metrics.WorkerState.WithLabelValues(p.TableName(), "pcap").Inc()
	defer metrics.WorkerState.WithLabelValues(p.TableName(), "pcap").Dec()

	pcap, err := pcapgo.NewReader(strings.NewReader(string(rawContent)))
	if err != nil {
		return err
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

	var first state
	var second state

	for err == nil {
		// Decode a packet
		packet := gopacket.NewPacket(data, layers.LayerTypeEthernet, gopacket.Default)
		if packet.ErrorLayer() != nil {
			log.Printf("Error decoding packet: %v", packet.ErrorLayer().Error())
			continue
		}
		if packet.Metadata().Truncated {
			if alpha.Packets < 20 {
				log.Printf("Packet %d truncated to %d of %d bytes, from data of length %d",
					alpha.Packets, packet.Metadata().CaptureInfo.CaptureLength,
					packet.Metadata().CaptureInfo.Length, len(data))
			}
			alpha.TruncatedPackets++
		}
		if ipLayer := packet.Layer(layers.LayerTypeIPv4); ipLayer != nil {
			ip, _ := ipLayer.(*layers.IPv4)
			switch alpha.Packets {
			case 0:
				first.SrcIP = ip.SrcIP
				first.TTL = ip.TTL
			case 1:
				second.SrcIP = ip.SrcIP
				second.TTL = ip.TTL
			default:
				if first.SrcIP.Equal(ip.SrcIP) {
					if first.TTL != ip.TTL {
						alpha.TTLChanges++
						first.TTLChanges++
					}
				} else if second.SrcIP.Equal(ip.SrcIP) {
					if second.TTL != ip.TTL {
						alpha.TTLChanges++
						second.TTLChanges++
					}
				} else {
					alpha.IPChanges++
				}
			}
		} else if ipLayer := packet.Layer(layers.LayerTypeIPv6); ipLayer != nil {
			ip, _ := ipLayer.(*layers.IPv6)
			switch alpha.Packets {
			case 0:
				first.SrcIP = ip.SrcIP
				first.TTL = ip.HopLimit
			case 1:
				second.SrcIP = ip.SrcIP
				second.TTL = ip.HopLimit
			default:
				if first.SrcIP.Equal(ip.SrcIP) {
					if first.TTL != ip.HopLimit {
						alpha.TTLChanges++
						second.TTLChanges++
					}
				} else if second.SrcIP.Equal(ip.SrcIP) {
					if second.TTL != ip.HopLimit {
						alpha.TTLChanges++
						second.TTLChanges++
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
					log.Println(p.GetUUID(testName))
				}
				first.SrcPort = tcp.SrcPort
				second.SrcPort = tcp.DstPort
			case 1:
				if second.SrcPort != tcp.SrcPort || !tcp.ACK {
					log.Fatal("oops", second, first, tcp.DstPort, tcp.ACK)
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
			first.Update(tcp, ci, sack)
			second.Update(tcp, ci, sack)

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

	alpha.FirstECECount = first.ECECount
	alpha.SecondECECount = second.ECECount
	alpha.FirstRetransmits = first.Retransmits
	alpha.SecondRetransmits = second.Retransmits
	alpha.TotalSrcSeq = int64(first.Seq.Value())
	alpha.TotalDstSeq = int64(second.Seq.Value())

	row := schema.PCAPRow{
		Parser: schema.ParseInfo{
			Version:    Version(),
			Time:       time.Now(),
			ArchiveURL: fileMetadata["filename"].(string),
			Filename:   testName,
			GitCommit:  GitCommit(),
		},

		Alpha: alpha,
	}

	if row.Alpha.FirstECECount > 0 || row.Alpha.SecondECECount > 0 || row.Alpha.FirstRetransmits > 0 || row.Alpha.SecondRetransmits > 0 {
		log.Printf("%d/%d truncated, %v <--> %v", alpha.TruncatedPackets, alpha.Packets, first, second) //, row.Alpha)
	}
	if err := p.Put(&row); err != nil {
		return err
	}

	// NOTE: Civil is not TZ adjusted. It takes the year, month, and date from
	// the given timestamp, regardless of the timestamp's timezone. Since we
	// run our systems in UTC, all timestamps will be relative to UTC and as
	// will these dates.
	row.Date = fileMetadata["date"].(civil.Date)
	row.ID = p.GetUUID(testName)

	//	log.Println(count, "packets", sacks, "sacks", optionCounts, optionNames)
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
