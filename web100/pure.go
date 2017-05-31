package web100

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// TODO - resolve use of Record, Snapshot, slog, snap, etc.

// Need to:
// 1. Read the header
//   blah blah blah e.g.  "2.5.27 201001301335 net100\n"
//   "\n"
//   /spec\n
//   name%20offset%20WEB100_TYPE%20length\n (separated by spaces, term by \n)
//   ...
//   ...
//   \n\n
//   /read\n
//   \n----End-Of-Header---- -1 -1\n
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
	BEGIN_SNAP_DATA   = "----Begin-Snap-Data----"           // Plus a newline?
	END_OF_HEADER     = "\x00----End-Of-Header---- -1 -1\n" // No newline.
	GROUPNAME_LEN_MAX = 32
	VARNAME_LEN_MAX   = 32
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

func NewVariable(s *string) (*Variable, error) {
	// TODO - use regular expression ??
	var name string
	var length, typ, offset int
	n, err := fmt.Sscanln(*s, &name, &offset, &typ, &length)

	if err != nil {
		fmt.Printf("%v, %d: %s\n", err, n, *s)
		return nil, err
	}
	if VarType(typ) > WEB100_TYPE_OCTET || VarType(typ) < WEB100_TYPE_INTEGER {
		return nil, errors.New(fmt.Sprintf("Invalid type field: %d\n", typ))
	}
	vt := VarType(typ)
	if length < 1 || length > 17 {
		return nil, errors.New(fmt.Sprintf("Invalid length field: %d\n", length))
	}

	// TODO - validate length to type consistency.
	// TODO - validate offset and sum of lengths
	return &Variable{name, offset, vt, length}, nil
}

// The header structure, containing all info from the header.
type FieldSet struct {
	Fields       []Variable
	FieldMap     map[string]int // Map from field name to index in Fields.
	RecordLength int            // Total length of record, including preamble, e.g. BEGIN_SNAP_DATA
}

// Find returns the variable of a given name, or nil.
func (fs *FieldSet) Find(name string) *Variable {
	index, ok := fs.FieldMap[name]
	if !ok {
		return nil
	}
	return &fs.Fields[index]
}

type ConnectionSpec struct {
	DestPort uint16
	SrcPort  uint16
	DestAddr []byte
	SrcAddr  []byte
}

type SnapLog struct {
	// The entire raw contents of the file.  Generally 1.5 MB, but may be
	// much larger
	raw []byte

	LogTime   uint32
	GroupName string

	ConnSpecOffset int // Offset in bytes of the ConnSpec
	BodyOffset     int // Offset in bytes of the first snapshot
	Spec           FieldSet
	Body           FieldSet
	Tune           FieldSet

	ConnSpec ConnectionSpec
}

func parseFields(buf *bytes.Buffer, preamble string, terminator string) (FieldSet, error) {
	fields := FieldSet{FieldMap: make(map[string]int)}
	pre, err := buf.ReadString('\n')
	if err != nil {
		return fields, err
	}
	if pre != preamble {
		return fields, errors.New("Expected preamble: " +
			preamble[:len(preamble)-2] + " != " + pre[:len(pre)-2])
	}

	for {
		line, err := buf.ReadString('\n')
		// TODO - choose a better line length limit?
		if err != nil || len(line) > 200 {
			if err == io.EOF {
				return fields, errors.New("Encountered EOF")
			} else {
				return fields, errors.New("Corrupted header")
			}
		}
		if line == terminator {
			return fields, nil
		}
		v, err := NewVariable(&line)
		if err != nil {
			return fields, err
		}
		if fields.RecordLength != v.Offset {
			return fields, errors.New("Bad offset at " + line[:len(line)-2])
		}
		fields.FieldMap[v.Name] = len(fields.Fields)
		fields.Fields = append(fields.Fields, *v)
		fields.RecordLength += v.Length
	}
}

func parseConnectionSpec(buf *bytes.Buffer) (ConnectionSpec, error) {
	// The web100 snaplog only correctly represents ipv4 addresses.
	// But try to read it anyway.
	raw := make([]byte, 16)
	n, err := buf.Read(raw)
	if err != nil || n < 16 {
		return ConnectionSpec{}, errors.New("Too few bytes for connection spec")
	}
	dstPort := binary.LittleEndian.Uint16(raw[0:2])
	// WARNING - the web100 code seemingly depends on a 32 bit architecture.
	// There is no "packed" directive for the web100_connection_spec, and the
	// fields all seem to be 32 bit aligned.
	dstAddr := raw[4:8]
	srcPort := binary.LittleEndian.Uint16(raw[8:10])
	srcAddr := raw[12:16]

	return ConnectionSpec{DestPort: dstPort, SrcPort: srcPort,
		DestAddr: dstAddr, SrcAddr: srcAddr}, nil
}

/*struct web100_connection_spec {
    u_int16_t dst_port;
    u_int32_t dst_addr;
    u_int16_t src_port;
    u_int32_t src_addr;
};*/

// Wraps a byte array in a SnapLog.  Returns error if there are problems.
func NewSnapLog(raw []byte) (*SnapLog, error) {
	buf := bytes.NewBuffer(raw)

	// First, the version, etc.
	_, err := buf.ReadString('\n')
	if err != nil {
		return nil, err
	}
	// Empty line
	empty, err := buf.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if empty != "\n" {
		fmt.Printf("%v\n", []byte(empty))
		return nil, errors.New("Expected empty string")
	}

	spec, err := parseFields(buf, "/spec\n", "\n")
	if err != nil {
		return nil, err
	}
	spec.RecordLength += 0

	body, err := parseFields(buf, "/read\n", "\n")
	if err != nil {
		return nil, err
	}
	// There seems to be a null character at the end of each record, so
	// add one to the length.
	body.RecordLength += len(BEGIN_SNAP_DATA) + 1

	tune, err := parseFields(buf, "/tune\n", END_OF_HEADER)
	if err != nil {
		return nil, err
	}

	// Read the timestamp and groupname
	t := make([]byte, 4)
	n, err := buf.Read(t)
	if err != nil || n < 4 {
		return nil, errors.New("Too few bytes for logTime")
	}
	logTime := binary.LittleEndian.Uint32(t)
	fmt.Println(logTime)

	gn := make([]byte, GROUPNAME_LEN_MAX)
	n, err = buf.Read(gn)
	if err != nil || n != GROUPNAME_LEN_MAX {
		return nil, errors.New("Too few bytes for groupName")
	}
	groupName := string(gn)
	fmt.Println(groupName)

	connSpecOffset := len(raw) - buf.Len()
	connSpec, err := parseConnectionSpec(buf)
	if err != nil {
		return nil, err
	}

	bodyOffset := len(raw) - buf.Len()

	slog := SnapLog{raw: raw, LogTime: logTime, GroupName: groupName,
		ConnSpecOffset: connSpecOffset, BodyOffset: bodyOffset,
		Spec: spec, Body: body, Tune: tune, ConnSpec: connSpec}

	return &slog, nil
}

func (sl *SnapLog) Validate() error {
	// Verify that body starts with BEGIN
	first := string(sl.raw[sl.BodyOffset : sl.BodyOffset+len(BEGIN_SNAP_DATA)])
	if first != BEGIN_SNAP_DATA {
		return errors.New("Missing first BeginSnapData")
	}

	// Verify that body size is integer multiple of body record length.
	total := len(sl.raw) - sl.BodyOffset
	if total%sl.Body.RecordLength != 0 {
		return errors.New("Body is not multiple of Body.RecordLength")
	}

	// Verify that last record is good quality

	// Verify that last record is in a TCP end state?
	return nil
}

type Snapshot struct {
	raw []byte // The raw data, NOT including the BEGIN_SNAP_HEADER
}

// Returns the snapshot at index n, or error if n is not a valid index, or data is corrupted.
func (slog *SnapLog) Snapshot(n int) (Snapshot, error) {
	// TODO
	return Snapshot{}, nil
}
