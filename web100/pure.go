package web100

import (
	"C"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
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
	WEB100_NUM_TYPES
)
const (
	WEB100_TYPE_IP_ADDRESS = WEB100_TYPE_INET_ADDRESS_IPV4 /* Deprecated */
	WEB100_TYPE_UNSIGNED16 = WEB100_TYPE_INET_PORT_NUMBER  /* Deprecated */
)

var Web100Sizes = [WEB100_NUM_TYPES + 1]byte{
	4 /*INTEGER*/, 4 /*INTEGER32*/, 4 /*IPV4*/, 4 /*COUNTER32*/, 4, /*GAUGE32*/
	4 /*UNSIGNED32*/, 4, /*TIME_TICKS*/
	8 /*COUNTER64*/, 2 /*PORT_NUM*/, 17, 17, 32 /*STR32*/, 1 /*OCTET*/, 0}

// Once consuming the header, we know the names and sizes of all fields, and the
// size of each record, which is len(BEGIN_SNAP_DATA) + 1 + sum(field lengths)
//
// We might want to just find diffs from one record to the next.  If delta is nil
// then nothing interesting has happened.
//
// Or we might just want to inspect a handful of fields to see if they have changes.
//

var legacyNames map[string]string

func init() {
	data, err := Asset("tcp-kis.txt")
	if err != nil {
		panic("tcp-kis.txt not found")
	}
	b := bytes.NewBuffer(data)

	legacyNames, err = ParseWeb100Definitions(b)
	if err != nil {
		panic("error parsing tcp-kis.txt")
	}
	fmt.Printf("LegacyNames has %d entries.\n", len(legacyNames))
}

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

	// Use with caution.  Generally should use connection spec from .meta file or
	// from snapshot instead.
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

func (sl *SnapLog) SnapCount() int {
	total := len(sl.raw) - sl.BodyOffset
	return total / sl.Body.RecordLength
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
		return errors.New("Body length is not multiple of Body.RecordLength")
	}

	// Verify that last record is good quality
	numSnapshots := sl.SnapCount()
	lastOffset := sl.BodyOffset + (numSnapshots-1)*sl.Body.RecordLength
	lastBegin := string(sl.raw[lastOffset : lastOffset+len(BEGIN_SNAP_DATA)])
	if lastBegin != BEGIN_SNAP_DATA {
		return errors.New("Missing last BeginSnapData")
	}

	// lastSnap := slog.Snapshot(numSnapshots - 1)
	// Verify that last record is in a TCP end state?
	return nil
}

type Snapshot struct {
	// Just the raw data, without BEGIN_SNAP_DATA.
	raw    []byte    // The raw data, NOT including the BEGIN_SNAP_HEADER
	fields *FieldSet // The fieldset describing the raw contents.
}

// Returns the snapshot at index n, or error if n is not a valid index, or data is corrupted.
func (sl *SnapLog) Snapshot(n int) (Snapshot, error) {
	offset := sl.BodyOffset + n*sl.Body.RecordLength
	if string(sl.raw[offset:offset+len(BEGIN_SNAP_DATA)]) != BEGIN_SNAP_DATA {
		return Snapshot{}, errors.New("Missing BeginSnapData")
	}

	return Snapshot{raw: sl.raw[offset+len(BEGIN_SNAP_DATA) : offset+sl.Body.RecordLength],
		fields: &sl.Body}, nil
}

// TODO URGENT - unit tests for this!!
func Save(field *Variable, data []byte, snapValues Saver) error {
	canonicalName := field.Name
	if legacy, ok := legacyNames[canonicalName]; ok {
		canonicalName = legacy
	}
	switch field.Type {
	case WEB100_TYPE_INTEGER:
		fallthrough
	case WEB100_TYPE_INTEGER32:
		val := binary.LittleEndian.Uint32(data)
		if val >= 1<<31 {
			snapValues.SetInt64(canonicalName, int64(val)-(int64(1)<<32))
		} else {
			snapValues.SetInt64(canonicalName, int64(val))
		}
	case WEB100_TYPE_INET_ADDRESS_IPV4:
		// TODO 4 unsigned char
		panic("Unimplemented")
	case WEB100_TYPE_COUNTER32:
		fallthrough
	case WEB100_TYPE_GAUGE32:
		fallthrough
	case WEB100_TYPE_UNSIGNED32:
		fallthrough
	case WEB100_TYPE_TIME_TICKS:
		snapValues.SetInt64(canonicalName, int64(binary.LittleEndian.Uint32(data)))
	case WEB100_TYPE_COUNTER64:
		// This conversion to signed may cause overflow panic!
		snapValues.SetInt64(canonicalName, int64(binary.LittleEndian.Uint64(data)))
	case WEB100_TYPE_INET_PORT_NUMBER:
		snapValues.SetInt64(canonicalName, int64(binary.LittleEndian.Uint16(data)))
	case WEB100_TYPE_INET_ADDRESS:
		fallthrough
	case WEB100_TYPE_INET_ADDRESS_IPV6:
		panic("Unimplemented")
	case WEB100_TYPE_STR32:
		// TODO - is there a better way?
		snapValues.SetString(canonicalName, strings.SplitN(string(data), "\000", 2)[0])
	case WEB100_TYPE_OCTET:
		// TODO - use byte array?
		snapValues.SetInt64(canonicalName, int64(data[0]))
	default:
		return errors.New("Invalid field type")
	}
	return nil
}

// SnapshotValues saves all values from the most recent C.web100_snapshot read by
// Next. Next must be called at least once before calling SnapshotValues.
func (snap *Snapshot) SnapshotValues(snapValues Saver) error {
	// Parses variables from most recent web100_snapshot data.

	// The web100 group is a set of web100 variables from a specific agent.
	// M-Lab snaplogs only ever have a single agent ("local") and group
	// (whatever the static set of web100 variables read from
	// /proc/web100/header).  The group is typically "read", but the header
	// typically also includes "spec" and "tune".
	//
	// To extract each web100 variables corresponding to all the variables
	// in the group, we iterate through the FieldSet
	var field Variable
	for _, field = range snap.fields.Fields {
		// Extract the web100 variable name and type. This will
		// correspond to one of the variables defined in tcp-kis.txt.
		// TODO handle canonical names
		Save(&field, snap.raw[field.Offset:field.Offset+field.Length], snapValues)
	}
	/*

			errno := C.web100_snap_read(v, snap, w.data)
			if errno != C.WEB100_ERR_SUCCESS {
				return fmt.Errorf(C.GoString(C.web100_strerror(errno)))
			}

			// Convert raw w.data into a string based on var_type.
			// TODO(prod): reimplement web100_value_to_textn to operate on Go types.
			C.web100_value_to_textn((*C.char)(w.text), C.WEB100_VALUE_LEN_MAX,
				(C.WEB100_TYPE)(var_type), w.data)

			// Use the canonical variable name. The variable name known to
			// the web100 kernel at run time lagged behind the official
			// web100 spec. So, some variable names need to be translated
			// from their legacy form (read from the kernel and written to
			// the snaplog) to the canonical form (as defined in
			// tcp-kis.txt).
			canonicalName := C.GoString(name)
			if _, ok := w.legacyNames[canonicalName]; ok {
				canonicalName = w.legacyNames[canonicalName]
			}

			// TODO(dev): are there any cases where we need unsigned int64?
			// Attempt to convert the current variable to an int64.
			value, err := strconv.ParseInt(C.GoString((*C.char)(w.text)), 10, 64)
			if err != nil {
				e := err.(*strconv.NumError)
				if e.Err == strconv.ErrSyntax {
					// If it cannot be converted, leave the variable as a string.
					snapValues.SetString(canonicalName, C.GoString((*C.char)(w.text)))
				} else if e.Err == strconv.ErrRange {
					log.Println("Range error: " + e.Num)
					// On a range error, ParseInt returns the best legal value,
					// i.e., MaxInt64, or MinInt64, so we just use that value.
					snapValues.SetInt64(canonicalName, value)
				}
			} else {
				snapValues.SetInt64(canonicalName, value)
			}
		}
	*/
	return nil
}
