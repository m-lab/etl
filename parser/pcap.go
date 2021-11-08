package parser

import (
	"fmt"
	"log"
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

func (w *wrappingCounter) Update(clock uint32) {
	if !w.initialized {
		w.clock = clock
		w.initialized = true
	}
	w.value = (w.value + uint64(clock-w.clock)) % uint64(1<<32)
	w.clock = clock
}

func (w *wrappingCounter) Value() uint64 {
	return w.value
}

// state keep track of the TCP state of one side of a connection.
// TODO - add histograms for Ack inter-arrival time.
type state struct {
	Port           layers.TCPPort // When this port is SrcPort, we update this stat struct.
	Sent           uint64         // Number of bytes sent in tcp payloads.
	Seq            wrappingCounter
	Ack            wrappingCounter
	LastPacketTime uint64
	Window         uint16
}

func (s *state) Update(tcp *layers.TCP, ci gopacket.CaptureInfo) {
	if tcp.SrcPort == s.Port {
		s.Sent += uint64(ci.Length - int(tcp.DataOffset*4))
		s.Seq.Update(tcp.Seq)
		s.LastPacketTime = uint64(ci.Timestamp.Nanosecond())
		s.Window = tcp.Window
	} else {
		if tcp.ACK {
			s.Ack.Update(tcp.Ack)
		}
	}
}

func (s state) String() string {
	return fmt.Sprintf("[%5d %12d/%10d/%10d %9d %5d]", s.Port, s.Sent, s.Seq.Value(), s.Ack.Value(), s.LastPacketTime, s.Window)
}

// ParseAndInsert decodes the PCAP data and inserts it into BQ.
func (p *PCAPParser) ParseAndInsert(fileMetadata map[string]bigquery.Value, testName string, rawContent []byte) error {
	metrics.WorkerState.WithLabelValues(p.TableName(), "pcap").Inc()
	defer metrics.WorkerState.WithLabelValues(p.TableName(), "pcap").Dec()

	pcap, err := pcapgo.NewReader(strings.NewReader(string(rawContent)))
	if err != nil {
		return err
	}

	var count int64 = 0
	var sacks int64 = 0
	optionCounts := make([]int64, 16)
	optionNames := make([]string, 16)
	for i := 0; i < 16; i++ {
		optionNames[i] = layers.TCPOptionKind(i).String()
	}

	data, ci, err := pcap.ReadPacketData()

	var syn int64 = -1
	var synAck int64 = -1
	var synTime time.Time
	var synAckTime time.Time

	var first state
	var second state

	for err == nil {
		// Decode a packet
		packet := gopacket.NewPacket(data, layers.LayerTypeEthernet, gopacket.Default)
		// Get the TCP layer from this packet
		if tcpLayer := packet.Layer(layers.LayerTypeTCP); tcpLayer != nil {
			//var sack string
			tcp, _ := tcpLayer.(*layers.TCP)
			if !(tcp.SrcPort == 443 || tcp.DstPort == 443) {
				break // only process ndt7 tests
			}
			switch count {
			case 0:
				first.Port = tcp.SrcPort
				second.Port = tcp.DstPort
			case 1:
				if second.Port != tcp.SrcPort || !tcp.ACK {
					log.Fatal("oops", second, first, tcp.DstPort, tcp.ACK)
				}
			default:
			}
			first.Update(tcp, ci)
			second.Update(tcp, ci)

			if tcp.SYN {
				if tcp.ACK {
					synAckTime = ci.Timestamp
					synAck = count
				} else {
					synTime = ci.Timestamp
					syn = count
				}
			}
			for i := 0; i < len(tcp.Options); i++ {
				optionCounts[i]++
				if tcp.Options[i].OptionType == layers.TCPOptionKindSACK {
					sacks += int64(len(tcp.Options[i].OptionData) / 8)
				}
			}
			if count < 100 {
				log.Printf("%2d, %10d, %10d, %s <--> %s", count, tcp.Seq, tcp.Ack, first, second)
			}
		}
		count++
		data, ci, err = pcap.ReadPacketData()
	}

	row := schema.PCAPRow{
		Parser: schema.ParseInfo{
			Version:    Version(),
			Time:       time.Now(),
			ArchiveURL: fileMetadata["filename"].(string),
			Filename:   testName,
			GitCommit:  GitCommit(),
		},

		Alpha: schema.AlphaFields{
			SynAckIntervalNsec: synAckTime.Sub(synTime).Nanoseconds(),
			SynPacket:          syn,
			SynTime:            synTime,
			SynAckPacket:       synAck,
			SynAckTime:         synAckTime,
			OptionCounts:       optionCounts,
			Packets:            count,
			Sacks:              sacks,
			TotalSrcSeq:        int64(first.Seq.Value()),
			TotalDstSeq:        int64(second.Seq.Value()),
		},
	}

	if synAckTime.Sub(synTime) > 100*time.Microsecond {
		log.Println("long synAck interval", synAckTime.Sub(synTime))
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
