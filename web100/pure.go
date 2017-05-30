package web100

import (
	"bytes"

	"github.com/m-lab/etl/schema"
)

// Need to:
// 1. Read the header
//   blah blah blah e.g.  "2.5.27 201001301335 net100../spec"
//   name%20offset%20WEB100_TYPE%20length\n (separated by spaces, term by \n)
//   ...
//   ...
//   \n----End-Of-Header----\n
//   log_time
//   group_name
//   connection spec (binary)
//
//   ...
//   \n----Begin-Snap-Data----\n
//   ...
// It appears that the file is expected to end at the end of a snap data
// with no tag to indicate the end - just EOF.
//
//00000000  32 2e 35 2e 32 37 20 32  30 31 30 30 31 33 30 31  |2.5.27 201001301|
//00000010  33 33 35 20 6e 65 74 31  30 30 0a 0a 2f 73 70 65  |335 net100../spe|

//00000c50  4c 69 6d 43 77 6e 64 20  34 30 20 34 20 34 0a 00  |LimCwnd 40 4 4..|
//00000c60  2d 2d 2d 2d 45 6e 64 2d  4f 66 2d 48 65 61 64 65  |----End-Of-Heade|
//00000c70  72 2d 2d 2d 2d 20 2d 31  20 2d 31 0a 8a 57 12 59  |r---- -1 -1..W.Y|
//00000c80  72 65 61 64 00 00 00 00  00 00 00 00 00 00 00 00  |read............|
//00000c90  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
//00000ca0  71 46 00 00 97 37 eb cc  bf d4 00 00 c2 74 55 cb  |qF...7.......tU.|
//00000cb0  2d 2d 2d 2d 42 65 67 69  6e 2d 53 6e 61 70 2d 44  |----Begin-Snap-D|
//00000cc0  61 74 61 2d 2d 2d 2d 0a  00 00 00 00 71 46 71 46  |ata----.....qFqF|

const (
	BEGIN_SNAP_DATA = `----Begin-Snap-Data----` // Plus a newline?
	END_OF_HEADER   = `----End-Of-Header----`   // No newline.
)

type VarType int

const (
	WEB100_TYPE_INTEGER VarType = iota
	WEB100_TYPE_INTEGER32
	WEB100_TYPE_INET_ADDRESS_IPV4
	WEB100_TYPE_COUNTER32
	WEB100_TYPE_GAUGE32
	WEB100_TYPE_UNSIGNED32
	WEB100_TYPE_TIME_TICKS
	WEB100_TYPE_COUNTER64
	WEB100_TYPE_INET_PORT_NUMBER
	WEB100_TYPE_INET_ADDRESS
	WEB100_TYPE_INET_ADDRESS_IPV6
	WEB100_TYPE_STR32
	WEB100_TYPE_OCTET
)
const (
	WEB100_TYPE_IP_ADDRESS = WEB100_TYPE_INET_ADDRESS_IPV4 /* Deprecated */
	WEB100_TYPE_UNSIGNED16 = WEB100_TYPE_INET_PORT_NUMBER  /* Deprecated */
)

// Once consuming the header, we know the names and sizes of all fields, and the
// size of each record, which is len(BEGIN_SNAP_DATA) + 1 + sum(field lengths)
//
// We might want to just find diffs from one record to the next.  If delta is nil
// then nothing interesting has happened.
//
// Or we might just want to inspect a handful of fields to see if they have changes.
//

type Variable struct {
	Name   string // TODO - canonical, or name from header?
	Offset int    // Offset, beyond the BEGIN_SNAP_HEADER and newline.
	Type   VarType
	Length int
}

// The header structure, containing all info from the header.
type Header struct {
	Fields       []Variable
	RecordLength int // Total length of record, including BEGIN_SNAP_DATA
}

// Find returns the variable of a given name, or nil.
func (h *Header) Find(name string) *Variable {
	return nil
}

type SnapLog struct {
	raw    []byte // The entire raw contents of the file.  Possibly very large.
	header Header
	buf    *bytes.Buffer
}

// Wraps a byte array in a SnapLog.  Returns error if there are problems.
func NewSnapLog(raw []byte) (SnapLog, error) {
	log := SnapLog{raw, Header{}, bytes.NewBuffer(raw)}

	// TODO Parse the header

	return log, nil
}

type Snapshot struct {
	raw []byte // The raw data, NOT including the BEGIN_SNAP_HEADER
}

// Returns the snapshot at index n, or possibly error if n is not a valid index.
func (log *SnapLog) Snapshot(n int) (Snapshot, error) {
	return Snapshot{}, nil
}

// Convert to map suitable for writing to bigquery.
// TODO - may drop the canonical param, and incorporate that into the Header.
func (log *Snapshot) ToMap(canonical map[string]string) (schema.Web100ValueMap, error) {
	return nil, nil
}
