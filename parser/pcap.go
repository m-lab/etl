package parser

import (
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

type state struct {
	init           bool
	lastPacketTime time.Time
	Port           layers.TCPPort // When this port is SrcPort, we update this stat struct.
	LastSeq        uint32
	LastAck        uint32 // From the other direction.
	TotalSeq       uint64
	TotalAck       uint64
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
	//startTime := ci.Timestamp

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

			if !first.init {
				first.init = true
				first.Port = tcp.SrcPort
				first.LastSeq = tcp.Seq
				second.Port = tcp.DstPort
			} else if !second.init {
				second.init = true
				if second.Port != tcp.SrcPort {
					log.Fatal("oops", second, first, tcp.DstPort)
				}
				second.LastSeq = tcp.Seq
				if tcp.ACK {
					first.LastAck = tcp.Ack
				}
			}

			if tcp.SrcPort == first.Port {
				first.TotalSeq += uint64(tcp.Seq - first.LastSeq)
				if tcp.Seq < first.LastSeq {
					first.TotalSeq += 1 << 32
				}
				first.LastSeq = tcp.Seq
				if tcp.ACK {
					second.LastAck = tcp.Ack
				}
				first.lastPacketTime = ci.Timestamp
			} else if tcp.SrcPort == second.Port {
				second.TotalSeq += uint64(tcp.Seq - second.LastSeq)
				if tcp.Seq < second.LastSeq {
					second.TotalSeq += 1 << 32
				}
				second.LastSeq = tcp.Seq
				if tcp.ACK {
					first.LastAck = tcp.Ack
				}
				second.lastPacketTime = ci.Timestamp
			}

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
			if count < 20 {
				log.Printf("%2d, %010d, %010d, %v, %v", count, tcp.Seq, tcp.Ack, first, second)
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
			TotalSrcSeq:        int64(first.TotalSeq),
			TotalDstSeq:        int64(second.TotalSeq),
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
