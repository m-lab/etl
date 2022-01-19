package headers

import (
	"fmt"
	"unsafe"

	"github.com/google/gopacket/layers"
	be "github.com/m-lab/etl/internal/bigendian"
)

/*******************************************************************************
	 						Ethernet Header handling
*******************************************************************************/

var (
	ErrUnknownEtherType        = fmt.Errorf("unknown Ethernet type")
	ErrTruncatedEthernetHeader = fmt.Errorf("truncated Ethernet header")
)

// EthernetHeader struct for the Ethernet Header, in wire format.
type EthernetHeader struct {
	SrcMAC, DstMAC [6]byte
	etherType      be.BE16 // BigEndian
}

// EtherType returns the EtherType field of the packet.
func (e *EthernetHeader) EtherType() layers.EthernetType {
	return layers.EthernetType(e.etherType.Uint16())
}

var EthernetHeaderSize = int(unsafe.Sizeof(EthernetHeader{}))
