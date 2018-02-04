// web100 provides tools for reading web100 snapshot logs, and parsing snapshots.
package web100

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
)

// NOTES:
//  This implementation relies on some apparent invariants that may or may not
//  be true across all NDT snaplogs.  The invariants are checked, and if they are
//  not actually true, we should see errors in processing our archives.
//
// TODO
//  With the new parser, it is now (probably) easy to identify where a log is
//  corrupted, and possibly make use of all the snapshots up to that point.
//  We probably should take advantage of this somehow.
//
// Terminology:
//   Snapshot: a single Web100 snapshot.
//   snap or snapshot - variable name for a single snapshot.
//   SnapLog: a full log containing typically 2000 or so Snapshots.
//   slog or snaplog - variable name for a SnapLog.

// SnapLog file overview.
//   The text portion of snaplog headers appear to be identical to the contents of
//   /proc/web100/header (on web100 patched kernels).
//
// In summary, the files look like:
//   "2.5.27 201001301335 net100\n"  // version string
//   \n
//   /spec\n
//   name%20offset%20WEB100_TYPE%20length\n (separated by spaces, term by \n)
//   ...
//   ...
//   \n  // blank line
//   /read\n
//   ...
//   /tune\n
//   ...
//   \n----End-Of-Header---- -1 -1\n
//   log_time
//   group_name (currently "read")
//   connection spec (binary)
//   ----Begin-Snap-Data----\n
//   ...
//   ----Begin-Snap-Data----\n
//   ...
//   ----Begin-Snap-Data----\n
//   ...
//   EOF
// It appears that the file is expected to end at the end of a snap data
// with no tag to indicate the end - just EOF.
//
//00000000  32 2e 35 2e 32 37 20 32  30 31 30 30 31 33 30 31  |2.5.27 201001301|
//00000010  33 33 35 20 6e 65 74 31  30 30 0a 0a 2f 73 70 65  |335 net100../spe|
//...
//00000c50  4c 69 6d 43 77 6e 64 20  34 30 20 34 20 34 0a 00  |LimCwnd 40 4 4..|
//00000c60  2d 2d 2d 2d 45 6e 64 2d  4f 66 2d 48 65 61 64 65  |----End-Of-Heade|
//00000c70  72 2d 2d 2d 2d 20 2d 31  20 2d 31 0a 8a 57 12 59  |r---- -1 -1..W.Y|
//00000c80  72 65 61 64 00 00 00 00  00 00 00 00 00 00 00 00  |read............|
//00000c90  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
//00000ca0  71 46 00 00 97 37 eb cc  bf d4 00 00 c2 74 55 cb  |qF...7.......tU.|
//00000cb0  2d 2d 2d 2d 42 65 67 69  6e 2d 53 6e 61 70 2d 44  |----Begin-Snap-D|
//00000cc0  61 74 61 2d 2d 2d 2d 0a  00 00 00 00 71 46 71 46  |ata----.....qFqF|
//
// Performance
//   This code is roughly 10 times faster than the web100 library based code.  It
//   avoids conversion of integers to text and back, file io, nasty inner fgetc loops.
//   If we use this in ndt.go to parse ALL snapshot records, we end up spending about
//   25% of the time doing ReadFrom (including decompression), 66% doing getAndInsertValues,
//   including 23% mapassign, and 12% mapassign2_faststr.

// The Saver interface decouples reading data from the web100 log files and
// saving those values.
type Saver interface {
	SetInt64(name string, value int64)
	SetString(name string, value string)
	SetBool(name string, value bool)
}

//=================================================================================

// CanonicalNames provides the mapping from old names (in snaplog files) to new
// canonical names.
// This is exported so that SideStream parser can use it easily.
var CanonicalNames map[string]string

func init() {
	data, err := Asset("tcp-kis.txt")
	if err != nil {
		panic("tcp-kis.txt not found")
	}
	b := bytes.NewBuffer(data)

	CanonicalNames, err = ParseWeb100Definitions(b)
	if err != nil {
		panic("error parsing tcp-kis.txt")
	}
}

//=================================================================================
const (
	BEGIN_SNAP_DATA   = "----Begin-Snap-Data----\n"
	END_OF_HEADER     = "\x00----End-Of-Header---- -1 -1\n"
	GROUPNAME_LEN_MAX = 32
	VARNAME_LEN_MAX   = 32
)

type varType int

const (
	// The ordering here is important, as it reflects the type values
	// defined by the web100 libraries.  Do not change ordering.
	WEB100_TYPE_INTEGER varType = iota
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

type addrType int

const (
	// The ordering here is important, as it reflects the type values
	// defined by the web100 libraries.  Do not change ordering.
	WEB100_ADDRTYPE_UNKNOWN addrType = iota
	WEB100_ADDRTYPE_IPV4
	WEB100_ADDRTYPE_IPV6
	WEB100_ADDRTYPE_DNS = 16
)

var web100Sizes = [WEB100_NUM_TYPES + 1]int{
	4,  /*INTEGER*/
	4,  /*INTEGER32*/
	4,  /*IPV4*/
	4,  /*COUNTER32*/
	4,  /*GAUGE32*/
	4,  /*UNSIGNED32*/
	4,  /*TIME_TICKS*/
	8,  /*COUNTER64*/
	2,  /*PORT_NUM*/
	17, /*INET_ADDRESS*/
	17, /*INET_ADDRESS_IPV6*/
	32, /*STR32*/
	1,  /*OCTET*/
	0,
}

//=================================================================================

// Variable is a representation of a Web100 field specifications, as they appear
// in snaplog headers.
type Variable struct {
	Name   string  // Encoded field name (before conversion to canonicalName)
	Offset int     // Offset, beyond the BEGIN_SNAP_HEADER
	Type   varType // Web100 type of the field
	Size   int     // Size, in bytes, of the raw data field.
}

// NewVariable creates a new variable based on web100 definition string
func NewVariable(s string) (*Variable, error) {
	var name string
	var length, typ, offset int
	n, err := fmt.Sscanln(s, &name, &offset, &typ, &length)

	if err != nil {
		fmt.Printf("NewVariable Error %v, %d: %s\n", err, n, s)
		return nil, err
	}
	vt := varType(typ)
	if vt > WEB100_TYPE_OCTET || vt < WEB100_TYPE_INTEGER {
		return nil, fmt.Errorf("invalid type field: %d", typ)
	}
	if length != web100Sizes[vt] {
		return nil, fmt.Errorf("invalid length for %s field: %d",
			name, length)
	}

	return &Variable{name, offset, vt, length}, nil
}

// IPFromBytes handles the 17 byte web100 IP address fields.
func IPFromBytes(data []byte) (net.IP, error) {
	if len(data) != 17 {
		return net.IP{}, errors.New("Wrong number of bytes")
	}
	switch addrType(data[16]) {
	case WEB100_ADDRTYPE_IPV4:
		return net.IPv4(data[0], data[1], data[2], data[3]), nil
	case WEB100_ADDRTYPE_IPV6:
		return net.IP(data[:16]), nil
	case WEB100_ADDRTYPE_UNKNOWN:
		fallthrough
	default:
		return nil, errors.New("Invalid IP encoding")
	}
}

// Save interprets data according to the receiver type, and saves the result to snapValues.
// Most of the types are unused, but included here for completeness.
// This does a single alloc per int64 save???
// TODO URGENT - unit tests for this!!
func (v *Variable) Save(data []byte, snapValues Saver) error {
	// Ignore deprecated fields.
	if v.Name[0] == '_' {
		return nil
	}
	// Use the canonical variable name. The variable name known to the web100
	// kernel at run time lagged behind the official web100 spec. So, some
	// variable names need to be translated from their legacy form (read from
	// the kernel and written to the snaplog) to the canonical form (as defined
	// in tcp-kis.txt).
	canonicalName := v.Name
	if legacy, ok := CanonicalNames[canonicalName]; ok {
		canonicalName = legacy
	}
	switch v.Type {
	case WEB100_TYPE_INTEGER:
		fallthrough
	case WEB100_TYPE_INTEGER32:
		val := binary.LittleEndian.Uint32(data)
		if val >= 0x7FFFFFFF {
			snapValues.SetInt64(canonicalName, int64(val)-0x100000000)
		} else {
			snapValues.SetInt64(canonicalName, int64(val))
		}
	case WEB100_TYPE_INET_ADDRESS_IPV4:
		snapValues.SetString(canonicalName,
			fmt.Sprintf("%d.%d.%d.%d",
				data[0], data[1], data[2], data[3]))
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
		ip, err := IPFromBytes(data)
		if err != nil {
			return err
		}
		snapValues.SetString(canonicalName, ip.String())
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

//=================================================================================
// fieldSet provides the ordered list of Web100 variable specifications.
type fieldSet struct {
	Fields   []Variable
	FieldMap map[string]int // Map from field name to index in Fields.
	// Total length of each record, in bytes, including preamble, e.g. BEGIN_SNAP_DATA
	// For example, for the standard "/read" snapshot record, the length is 669 bytes.
	Length int
}

// find returns the variable spec of a given name, or nil.
func (fs *fieldSet) find(name string) *Variable {
	index, ok := fs.FieldMap[name]
	if !ok {
		return nil
	}
	return &fs.Fields[index]
}

//=================================================================================

// connectionSpec holds the 4-tuple info from the header, and may be used to
// populate the connection_spec field of the web100_log_entry.  It does not support
// ipv6, so it is of limited use.
type connectionSpec struct {
	DestPort uint16
	SrcPort  uint16
	DestAddr []byte // 4 byte IP address.  0.0.0.0 for ipv6
	SrcAddr  []byte
}

//=================================================================================

// SnapLog encapsulates the raw data and all elements of the header.
type SnapLog struct {
	// The entire raw contents of the file.  Generally 1.5MB, but may be much larger
	raw []byte

	Version   string
	LogTime   uint32
	GroupName string

	connSpecOffset int // Offset in bytes of the ConnSpec
	bodyOffset     int // Offset in bytes of the first snapshot
	spec           fieldSet
	// The primary field set used by snapshots
	// The name "read" is ugly, but that is the name of the web100 header section.
	read fieldSet
	tune fieldSet

	// Use with caution.  Generally should use connection spec from .meta file or
	// from snapshot instead.
	connSpec connectionSpec
}

func (sl *SnapLog) ConnectionSpecValues(saver Saver) {
	saver.SetInt64("local_af", int64(0))
	src := sl.connSpec.SrcAddr
	saver.SetString("local_ip", net.IPv4(src[0], src[1], src[2], src[3]).String())
	saver.SetInt64("local_port", int64(sl.connSpec.SrcPort))
	dst := sl.connSpec.DestAddr
	saver.SetString("remote_ip", net.IPv4(dst[0], dst[1], dst[2], dst[3]).String())
	saver.SetInt64("remote_port", int64(sl.connSpec.DestPort))
}

// SnapshotNumBytes returns the length of snapshot records, including preamble.
// Used only for testing.
func (sl *SnapLog) SnapshotNumBytes() int {
	return sl.read.Length
}

// SnapshotNumFields returns the total number of snapshot fields.
// Used only for testing.
func (sl *SnapLog) SnapshotNumFields() int {
	return len(sl.read.Fields)
}

// parseFields parses the newline separated web100 variable types from the header.
func parseFields(buf *bytes.Buffer, preamble string, terminator string) (*fieldSet, error) {
	fields := new(fieldSet)
	fields.FieldMap = make(map[string]int)

	pre, err := buf.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if pre != preamble {
		return nil, errors.New("Expected preamble: " +
			// Strip terminal \n from each string for readability.
			preamble[:len(preamble)-1] + " != " + pre[:len(pre)-1])
	}

	for {
		line, err := buf.ReadString('\n')
		// line length is max var name size, plus 20 bytes for the 3 numeric fields.
		if err != nil || len(line) > VARNAME_LEN_MAX+20 {
			if err == io.EOF {
				return nil, errors.New("Encountered EOF")
			}
			return nil, errors.New("Corrupted header")
		}
		if line == terminator {
			return fields, nil
		}
		v, err := NewVariable(line)
		if err != nil {
			return nil, err
		}
		if fields.Length != v.Offset {
			return nil, errors.New("Bad offset at " + line[:len(line)-2])
		}
		fields.FieldMap[v.Name] = len(fields.Fields)
		fields.Fields = append(fields.Fields, *v)
		fields.Length += v.Size
	}
}

// parseConnectionSpec parses the 16 byte binary connection spec field from the header.
func parseConnectionSpec(buf *bytes.Buffer) (connectionSpec, error) {
	// The web100 snaplog only correctly represents ipv4 addresses.
	// If the later parts of the log are corrupt, this may be all we get,
	// so for now, read it anyway.
	raw := make([]byte, 16)
	n, err := buf.Read(raw)
	if err != nil || n < 16 {
		return connectionSpec{}, errors.New("Too few bytes for connection spec")
	}
	// WARNING - the web100 code seemingly depends on a 32 bit architecture.
	// There is no "packed" directive for the web100_connection_spec, and the
	// fields all seem to be 32 bit aligned.
	dstPort := binary.LittleEndian.Uint16(raw[0:2])
	dstAddr := raw[4:8]
	srcPort := binary.LittleEndian.Uint16(raw[8:10])
	srcAddr := raw[12:16]

	return connectionSpec{DestPort: dstPort, SrcPort: srcPort,
		DestAddr: dstAddr, SrcAddr: srcAddr}, nil
}

// NewSnapLog creates a SnapLog from a byte array.  Returns error if there are problems.
func NewSnapLog(raw []byte) (*SnapLog, error) {
	buf := bytes.NewBuffer(raw)

	// First, the version, etc.
	version, err := buf.ReadString('\n')
	if err != nil {
		return nil, err
	}
	version = strings.Split(version, "\n")[0]

	// Empty line
	empty, err := buf.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if empty != "\n" {
		fmt.Printf("%v\n", []byte(empty))
		return nil, errors.New("Expected empty string")
	}

	// TODO - do these header elements always come in this order.
	spec, err := parseFields(buf, "/spec\n", "\n")
	if err != nil {
		return nil, err
	}

	read, err := parseFields(buf, "/read\n", "\n")
	if err != nil {
		return nil, err
	}
	read.Length += len(BEGIN_SNAP_DATA)

	// The terminator here does NOT start with \n.  8-(
	tune, err := parseFields(buf, "/tune\n", END_OF_HEADER)
	if err != nil {
		return nil, err
	}

	// Read the timestamp.
	t := make([]byte, 4)
	n, err := buf.Read(t)
	if err != nil || n < 4 {
		return nil, errors.New("Too few bytes for logTime")
	}
	logTime := binary.LittleEndian.Uint32(t)

	// Read the group name.
	// The web100 group is a set of web100 variables from a specific agent.
	// M-Lab snaplogs only ever have a single agent ("local") and group.
	// The group is typically "read", but the header typically also includes
	// "spec" and "tune".
	gn := make([]byte, GROUPNAME_LEN_MAX)
	n, err = buf.Read(gn)
	if err != nil || n != GROUPNAME_LEN_MAX {
		return nil, errors.New("Too few bytes for groupName")
	}
	// The groupname is a C char*, terminated with a null character.
	groupName := strings.SplitN(string(gn), "\000", 2)[0]
	if groupName != "read" {
		fmt.Println(groupName)
		return nil, errors.New("Only 'read' group is supported")
	}

	connSpecOffset := len(raw) - buf.Len()
	connSpec, err := parseConnectionSpec(buf)
	if err != nil {
		return nil, err
	}

	bodyOffset := len(raw) - buf.Len()

	slog := SnapLog{raw: raw, Version: version, LogTime: logTime, GroupName: groupName,
		connSpecOffset: connSpecOffset, bodyOffset: bodyOffset,
		spec: *spec, read: *read, tune: *tune, connSpec: connSpec}

	return &slog, nil
}

// SnapCount returns the number of valid snapshots.
func (sl *SnapLog) SnapCount() int {
	total := len(sl.raw) - sl.bodyOffset
	return total / sl.read.Length
}

// ValidateSnapshots checks whether the first and last snapshots are valid and complete.
func (sl *SnapLog) ValidateSnapshots() error {
	// Valid first snapshot?
	_, err := sl.Snapshot(0)
	if err != nil {
		return err
	}
	// Valid last snapshot?
	_, err = sl.Snapshot(sl.SnapCount() - 1)
	if err != nil {
		return err
	}
	// Verify that body size is integer multiple of body record length.
	total := len(sl.raw) - sl.bodyOffset
	if total%sl.read.Length != 0 {
		return errors.New("last snapshot truncated")
	}
	return nil
}

//=================================================================================

// Snapshot represents a complete snapshot from a snapshot log.
type Snapshot struct {
	// Just the raw data, without BEGIN_SNAP_DATA.
	raw    []byte    // The raw data, NOT including the BEGIN_SNAP_HEADER
	fields *fieldSet // The fieldset describing the raw contents.
}

// Snapshot returns the snapshot at index n, or error if n is not a valid index, or data is corrupted.
func (sl *SnapLog) Snapshot(n int) (Snapshot, error) {
	if n > sl.SnapCount()-1 {
		return Snapshot{}, fmt.Errorf("invalid snapshot index %d", n)
	}
	offset := sl.bodyOffset + n*sl.read.Length
	begin := string(sl.raw[offset : offset+len(BEGIN_SNAP_DATA)])
	if begin != BEGIN_SNAP_DATA {
		return Snapshot{}, errors.New("missing BeginSnapData")
	}

	// We use the "/read" field group, as that is what is always used for NDT snapshots.
	// This may be incorrect for use in other settings.
	return Snapshot{raw: sl.raw[offset+len(BEGIN_SNAP_DATA) : offset+sl.read.Length],
		fields: &sl.read}, nil
}

func (snap *Snapshot) reset(data []byte, fields *fieldSet) {
	snap.fields = fields
	snap.raw = data
}

// SnapshotValues writes all values into the provided Saver.
func (snap *Snapshot) SnapshotValues(snapValues Saver) error {
	if snap.raw == nil {
		return errors.New("Empty/Invalid Snaplog")
	}
	var field Variable
	for _, field = range snap.fields.Fields {
		// Interpret and save the web100 field value.
		field.Save(snap.raw[field.Offset:field.Offset+field.Size], snapValues)
	}
	return nil
}

// SnapshotDeltas writes changed values into the provided Saver.
func (snap *Snapshot) SnapshotDeltas(other *Snapshot, snapValues Saver) error {
	if snap.raw == nil {
		return errors.New("Empty/Invalid Snaplog")
	}
	if other.raw == nil {
		// If other is empty, return full snapshot
		return snap.SnapshotValues(snapValues)
	}
	var field Variable
	for _, field = range snap.fields.Fields {
		a := other.raw[field.Offset : field.Offset+field.Size]
		b := snap.raw[field.Offset : field.Offset+field.Size]
		if bytes.Compare(a, b) != 0 {
			// Interpret and save the web100 field value.
			field.Save(b, snapValues)
		}
	}
	return nil
}

// Check whether a single field differs between two snapshots.
// For example, CongSignal
// TODO - optimize by passing in a?
func (snap *Snapshot) diff(other *Snapshot, field Variable) bool {
	a := other.raw[field.Offset : field.Offset+field.Size]
	b := snap.raw[field.Offset : field.Offset+field.Size]
	return bytes.Compare(a, b) != 0
}

func (snap *Snapshot) extract(field Variable, values Saver) {
	data := snap.raw[field.Offset : field.Offset+field.Size]
	field.Save(data, values)
}

// About 100 usec (for CongestionSignals)
func (snaplog *SnapLog) ChangeIndices(fieldName string) ([]int, error) {
	result := make([]int, 0, 100)
	field := snaplog.read.find(fieldName)
	if field == nil {
		return nil, errors.New("Field not found")
	}
	last := make([]byte, field.Size)
	for i := 0; i < snaplog.SnapCount(); i++ {
		s, err := snaplog.Snapshot(i)
		if err != nil {
			return result, err
		}
		data := s.raw[field.Offset : field.Offset+field.Size]
		if bytes.Compare(data, last) != 0 {
			result = append(result, i)
		}
		last = data
	}
	return result, nil
}

type ArraySaver struct {
	Integers []int64
	Strings  []string
	Bools    []bool
}

func NewArraySaver(n int) ArraySaver {
	return ArraySaver{make([]int64, 0, n),
		make([]string, 0, n), make([]bool, 0, n)}
}

func (s ArraySaver) SetInt64(name string, val int64) {
	s.Integers = append(s.Integers, val)
}
func (s ArraySaver) SetBool(name string, val bool) {
	s.Bools = append(s.Bools, val)
}
func (s ArraySaver) SetString(name string, val string) {
	s.Strings = append(s.Strings, val)
}

type NullSaver struct {
	Integers []int64
}

func (s NullSaver) SetString(name string, val string) {}
func (s NullSaver) SetInt64(name string, val int64)   {}
func (s NullSaver) SetBool(name string, val bool)     {}

// about 100 nsec per field.
func (sl *SnapLog) SliceIntField(fieldName string, at []int) []int64 {
	var s Snapshot
	field := sl.read.find(fieldName)
	//result := NullSaver{}
	//data := []byte{1, 2, 3, 4, 5, 6}
	result := NewArraySaver(len(at))
	for i := 0; i < len(at); i++ {
		offset := sl.bodyOffset + at[i]*sl.read.Length
		//begin := string(sl.raw[offset : offset+len(BEGIN_SNAP_DATA)])
		//if begin != BEGIN_SNAP_DATA {
		//	return nil
		//}

		// We use the "/read" field group, as that is what is always used for NDT snapshots.
		// This may be incorrect for use in other settings.
		// This saves about 70 usec.
		s.reset(sl.raw[offset+len(BEGIN_SNAP_DATA):offset+sl.read.Length], &sl.read)
		//s, err := sl.Snapshot(at[i]) // This is doing an alloc
		//if err != nil {
		//	return nil
		//}
		// Alloc seems to be here, even using NullSaver
		// also even using simple data.
		field.Save(s.raw[field.Offset:field.Offset+field.Size], result)
		//field.Save(data, result)
	}
	return result.Integers
}

// For each increment in CongSignal, we want to add values of snapCount, SRTT
// the corresponding repeated fields.
