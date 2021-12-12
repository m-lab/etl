// Package headers provides basic decoding of pcap file (and maybe network) headers.
package headers

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"unsafe"
)

var (
	ErrCaptureTooLarge = errors.New("capture too large")
)

// These are used to decode the pcap file, and behave differently
// if the file was encoded with BigEndian or LittleEndian byte order.
type file16 uint16
type file32 uint32

func (h file16) Value(be bool) uint16 {
	if be {
		return uint16(h<<8 | h>>8)
	}
	return uint16(h)
}

func (h file32) Value(be bool) uint32 {
	if be {
		return uint32(h<<24 | h>>24 | (h&0x00ff0000)<<8 | (h&0x0000ff00)>>8)
	}
	return uint32(h)
}

func toInt(b []byte, be bool) int {
	if be {
		return int(uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3]))
	} else {
		return int(uint32(b[3])<<24 | uint32(b[2])<<16 | uint32(b[1])<<8 | uint32(b[0]))
	}
}

const (
	magicMicroseconds = 0xA1B23C4D
	magicNanoseconds  = 0x1A2B3C4D
)

//                            1                   2                   3
//        0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//     0 |                          Magic Number                         |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//     4 |          Major Version        |         Minor Version         |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//     8 |                           Reserved1                           |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    12 |                           Reserved2                           |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    16 |                            SnapLen                            |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    20 | FCS |f|                   LinkType                            |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

type PCAP struct {
	MagicNumber  file32
	VersionMajor file16
	VersionMinor file16
	res1, res2   int32
	SnapLen      file32
	FCSfLinkType file32
}

func (p PCAP) IsValid() bool {
	return p.MagicNumber == magicMicroseconds || p.MagicNumber == magicNanoseconds
}

func (p PCAP) IsBE() bool {
	return !(p.MagicNumber == magicMicroseconds || p.MagicNumber == magicNanoseconds)
}

func (p PCAP) isMicroseconds() bool {
	return p.MagicNumber.Value(p.IsBE()) == magicMicroseconds
}

func PCAPHeader(r io.Reader) (PCAP, error) {
	var pcap PCAP
	buf := (*[unsafe.Sizeof(pcap)]byte)(unsafe.Pointer(&pcap))[:unsafe.Sizeof(pcap)]
	n, err := r.Read(buf[:])
	if err != nil {
		return pcap, err
	}
	if n != int(unsafe.Sizeof(pcap)) {
		return pcap, io.ErrUnexpectedEOF
	}
	if !pcap.IsValid() {
		return pcap, errors.New("invalid magic number")
	}
	return pcap, nil
}

type PacketReader struct {
	header  PCAP
	snapLen int
	r       io.Reader
	isGzip  bool
	isBE    bool
}

func (pr *PacketReader) SnapLen() int {
	return pr.snapLen
}

func PCAPReader(data []byte) (*PacketReader, error) {
	if len(data) < 4 {
		return nil, io.ErrUnexpectedEOF
	}
	pr := PacketReader{isGzip: true}
	var err error

	pr.r, err = gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		pr.r = bytes.NewReader(data)
		pr.isGzip = false
		err = nil
	}

	pr.header, err = PCAPHeader(pr.r)
	if err != nil {
		return nil, err
	}
	if !pr.header.IsValid() {
		return nil, errors.New("invalid magic number")
	}
	pr.isBE = pr.header.IsBE()
	pr.snapLen = int(pr.header.SnapLen.Value(pr.isBE))
	return &pr, nil
}

//                           1                   2                   3
//       0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//     0 |                      Timestamp (Seconds)                      |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//     4 |            Timestamp (Microseconds or nanoseconds)            |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//     8 |                    Captured Packet Length                     |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    12 |                    Original Packet Length                     |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    16 /                                                               /
//       /                          Packet Data                          /
//       /                        variable length                        /
//       /                                                               /
//       +---------------------------------------------------------------+

type Packet struct {
	TimestampSeconds  uint32
	TimestampMicrosec uint32
	CapturedLen       uint32
	OriginalLen       uint32
	Data              [200]byte // Backing data is generally not full 200 bytes.
}

func (p Packet) UnixNano() int64 {
	return int64(p.TimestampSeconds)*1e9 + int64(p.TimestampMicrosec)*1e3
}

// NextPacket reads the next packet from the reader into the provided Packet.
// It returns the byte slice containing the packet data, or an error.
// The byte slice is backed by the Data field of the provided Packet.
func (pr *PacketReader) NextPacket(p *Packet) ([]byte, error) {
	if p == nil {
		return nil, errors.New("nil packet")
	}
	if pr.snapLen > 200 {
		return nil, ErrCaptureTooLarge
	}

	pBytes := (*[unsafe.Sizeof(*p)]byte)(unsafe.Pointer(p))[:unsafe.Sizeof(*p)]

	n, err := io.ReadAtLeast(pr.r, pBytes[0:16], 16) // Read the first 16 bytes.
	if err != nil {
		return nil, err
	}

	if pr.isBE {
		p.TimestampSeconds = uint32(toInt(pBytes[0:4], true))
		p.TimestampMicrosec = uint32(toInt(pBytes[4:8], true))
		p.CapturedLen = uint32(toInt(pBytes[8:12], true))
		p.OriginalLen = uint32(toInt(pBytes[12:16], true))
	}

	if int(p.CapturedLen) > pr.snapLen || int(p.CapturedLen) > len(p.Data) {
		return nil, ErrCaptureTooLarge
	}

	n, err = io.ReadAtLeast(pr.r, p.Data[:p.CapturedLen], int(p.CapturedLen))
	if n != int(p.CapturedLen) {
		return nil, io.ErrUnexpectedEOF
	}
	return p.Data[:p.CapturedLen], nil
}
