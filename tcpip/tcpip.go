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
	"log"
	"net"
	"os"
	"time"

	"github.com/google/gopacket/pcapgo"

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
)

type Stats struct {
	SrcIP   net.IP
	Packets int
	Bytes   int
}

type Summary struct {
	init bool

	HopLimit  uint8
	Packets   int
	StartTime headers.UnixNano
	LastTime  headers.UnixNano

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

func (s *Summary) Add(p *headers.Packet) {
	ip := p.IP

	s.srcIP = ip.SrcIP(s.srcIP) // ESCAPE - these reduce escapes to the heap
	s.dstIP = ip.DstIP(s.dstIP)
	if !s.init {
		s.StartTime = p.PktTime
		s.HopLimit = ip.HopLimit()

		s.Left.SrcIP = append([]byte{}, s.srcIP[:]...)
		s.Right.SrcIP = append([]byte{}, s.dstIP[:]...)

		s.init = true
	}

	s.LastTime = p.PktTime

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

	pcap, err := pcapgo.NewReader(bytes.NewReader(data))
	if err != nil {
		log.Print(err)
		return summary, err
	}

	p := headers.Packet{}
	for data, ci, err := pcap.ReadPacketData(); err == nil; data, ci, err = pcap.ZeroCopyReadPacketData() {
		// Pass ci by pointer, but Wrap will make a copy, since gopacket NoCopy doesn't preserve the values.
		err := p.Overlay(headers.UnixNano(ci.Timestamp.UnixNano()), data)
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
