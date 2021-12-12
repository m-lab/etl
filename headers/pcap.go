// Package headers provides basic decoding of pcap file (and maybe network) headers.
package headers

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
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

func PCAPReader(data []byte) (*PCAP, io.Reader, error) {
	if len(data) < 4 {
		return nil, nil, io.ErrUnexpectedEOF
	}
	var r io.Reader
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		r = bytes.NewReader(data)
		err = nil
	}

	pcap, err := PCAPHeader(r)
	if err != nil {
		return nil, nil, err
	}
	if !pcap.IsValid() {
		return nil, nil, errors.New("invalid magic number")
	}
	return &pcap, r, nil
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

// type packetMicros struct {
// 	TimestampSeconds  file32
// 	TimestampMicrosec file32
// 	CapturedLen       file32
// 	OriginalLen       file32
// 	Data              [200]byte // Backing data is generally not full 200 bytes.
// }

// type packetNanos struct {
// 	TimestampSeconds  file32
// 	TimestampNanosecs file32
// 	CapturedLen       file32 // The number of bytes captured into Data field.
// 	OriginalLen       file32
// 	Data              [200]byte // Backing data is generally not full 200 bytes.
// }

// func (p packetMicros) byteSlice() []byte {
// 	buf := (unsafe.Pointer(&p))
// 	bp := (*[unsafe.Sizeof(p)]byte)(buf)
// 	return bp[:]
// }

// NextPacket reads the next packet from the reader into the dst slice, and
// returns the slice.  Allocates additional space if needed, or if passed nil.
func NextPacket(r io.Reader, dst []byte, be bool) ([]byte, error) {
	if dst == nil {
		dst = make([]byte, 0, 16) // Initially, just need the first 4 words.
	}

	dst = dst[0:16]

	n, err := io.ReadAtLeast(r, dst[0:16], 16) // Read the first 16 bytes.
	if err != nil {
		if n == 0 {
			return nil, io.EOF
		}
		return nil, err
	}
	if n != 16 {
		return nil, io.ErrUnexpectedEOF
	}
	length := toInt(dst[8:12], be) // The captured length.

	if length > 216 {
		return nil, ErrCaptureTooLarge
	}

	if cap(dst) < 16+length { // Need more space.
		tmp := dst
		dst = make([]byte, 16+length)
		n := copy(dst, tmp)
		if n != 16 {
			return nil, fmt.Errorf("unexpected copy length: %d", n)
		}
		if cap(dst) < length+16 {
			return nil, ErrCaptureTooLarge
		}
	}
	dst = dst[0 : 16+length]
	if len(dst) != 16+length {
		return nil, fmt.Errorf("unexpected length: %d", len(dst))
	}
	n, err = io.ReadAtLeast(r, dst[16:16+length], length)
	if err == io.EOF {
		return dst, nil
	}
	if err != nil {
		return nil, err
	}
	return dst, nil
}
