// web100 provides Go bindings to some functions in the web100 library.
package web100

// Cgo directives must immediately preceed 'import "C"' below.
// For more information see:
//  - https://blog.golang.org/c-go-cgo
//  - https://golang.org/cmd/cgo

/*
#include <stdio.h>
#include <stdlib.h>
#include <web100.h>
#include <web100-int.h>

#include <arpa/inet.h>
*/
import "C"

import (
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"unsafe"
)

var (
	// TODO(prod): eliminate this lock (along with tmpfs).
	web100Lock sync.Mutex
)

// Discoveries:
//  - Not all C macros exist in the "C" namespace.
//  - 'NULL' is usually equivalent to 'nil'

// The Saver interface decouples reading data from the web100 log files and
// saving those values.
type Saver interface {
	SetInt64(name string, value int64)
	SetString(name string, value string)
}

// Web100 maintains state associated with a web100 log file.
type Web100 struct {
	// legacyNames maps legacy web100 variable names to their canonical names.
	legacyNames map[string]string

	// NOTE: we define all C-allocated types as unsafe.Pointers here. This is a
	// design choice to emphasize that these values should not be used outside
	// of this package and even within this package, they should be used
	// carefully.

	// snaplog is the primary *C.web100_log object encapsulating a snaplog file.
	snaplog unsafe.Pointer

	// snap is an individual *C.web100_snapshot record read from a snaplog.
	snap unsafe.Pointer

	// Pointers to C buffers for use in calls to web100 functions.
	text unsafe.Pointer
	data unsafe.Pointer
}

// Open prepares a web100 snaplog file for reading. The caller must call Close on
// the returned Web100 instance to free memory and close open file descriptors.
func Open(filename string, legacyNames map[string]string) (*Web100, error) {
	c_filename := C.CString(filename)
	defer C.free(unsafe.Pointer(c_filename))

	// TODO(prod): do not require reading from a file. Accept a byte array.
	// We need to lock calls to web100_log_open_read because of "log_header".
	var w_errno C.int = C.WEB100_ERR_SUCCESS
	web100Lock.Lock()
	snaplog := C.web100_log_open_read(c_filename, &w_errno)
	web100Lock.Unlock()
	if w_errno != C.WEB100_ERR_SUCCESS {
		fmt.Printf("%v\n", snaplog)
	}

	// Verify that the snaplog is valid before continuing.
	if snaplog == nil {
		return nil, fmt.Errorf(C.GoString(C.web100_strerror(w_errno)))
	}
	if w_errno != C.WEB100_ERR_SUCCESS {
		C.web100_log_close_read(snaplog)
		return nil, fmt.Errorf(C.GoString(C.web100_strerror(w_errno)))
	}

	// Pre-allocate a snapshot record.
	snap := C.web100_snapshot_alloc_from_log(snaplog, &w_errno)
	if snap == nil {
		log.Printf("%s\n", C.GoString(C.web100_strerror(w_errno)))
		C.web100_log_close_read(snaplog)
		return nil, fmt.Errorf(C.GoString(C.web100_strerror(w_errno)))
	}
	if w_errno != C.WEB100_ERR_SUCCESS {
		C.web100_snapshot_free(snap)
		C.web100_log_close_read(snaplog)
		return nil, fmt.Errorf(C.GoString(C.web100_strerror(w_errno)))
	}

	w := &Web100{
		legacyNames: legacyNames,
		snaplog:     unsafe.Pointer(snaplog),
		snap:        unsafe.Pointer(snap),
		// Pre-allocate space for converting snapshot values.
		text: C.calloc(2*C.WEB100_VALUE_LEN_MAX, 1),
		data: C.calloc(C.WEB100_VALUE_LEN_MAX, 1),
	}
	return w, nil
}

// Next reads the next C.web100_snapshot record from the web100 snaplog and
// saves it in an internal buffer. Use SnapshotValues to read the all values from
// the most recently read snapshot.  If Next reaches EOF or another error, the
// last snapshot is in an undefined state.
func (w *Web100) Next() error {
	snaplog := (*C.web100_log)(w.snaplog)
	snap := (*C.web100_snapshot)(w.snap)
	if snap == nil {
		log.Printf("Snapshot is nil\n")
		return fmt.Errorf("Snapshot is nil")
	}

	// Read the next web100_snaplog data from underlying file.
	err := C.web100_snap_from_log(snap, snaplog)
	if err == C.EOF {
		return io.EOF
	}
	if err != C.WEB100_ERR_SUCCESS {
		// WEB100_ERR_FILE_TRUNCATED_SNAP_DATA or
		// WEB100_ERR__MISSING_SNAP_MAGIC
		return fmt.Errorf(C.GoString(C.web100_strerror(err)))
	}
	return nil
}

// LogVersion returns the snaplog version.
func (w *Web100) LogVersion() string {
	snaplog := (*C.web100_log)(w.snaplog)
	return C.GoString(C.web100_get_agent_version(C.web100_get_log_agent(snaplog)))
}

// LogTime returns the timestamp of when the snaplog was opened for writing.
func (w *Web100) LogTime() int64 {
	snaplog := (*C.web100_log)(w.snaplog)
	return int64(C.web100_get_log_time(snaplog))
}

// ConnectionSpec populates the connSpec Saver with values from C.web100_connection_spec.
// TODO(dev): define the field names saved in connSpec since they are not the
// same ones defined in C.web100_connection_spec.
func (w *Web100) ConnectionSpec(connSpec Saver) error {
	snaplog := (*C.web100_log)(w.snaplog)

	// NOTE: web100_connection_spec_v6 is never set by the web100 library.
	// NOTE: conn->addrtype is always WEB100_ADDRTYPE_UNKNOWN.
	// So, we expect it to be IPv4 and fix local_ip and remote_ip later if
	// snapshots have IPv6 addresses.
	var spec C.struct_web100_connection_spec

	// Get reference to the snaplog connection.
	conn := C.web100_get_log_connection(snaplog)
	// Copy the connection spec from the snaplog connection information.
	C.web100_get_connection_spec(conn, &spec)

	// NOTE: web100_connection_spec only contains IPv4 addresses (4 byte values).
	// If the connection was IPv6, the IPv4 addresses here will be 0.0.0.0.
	srcIp := net.IP(C.GoBytes(unsafe.Pointer(&spec.src_addr), 4))
	dstIp := net.IP(C.GoBytes(unsafe.Pointer(&spec.dst_addr), 4))

	connSpec.SetString("local_ip", srcIp.String())
	connSpec.SetInt64("local_port", int64(spec.src_port))
	connSpec.SetString("remote_ip", dstIp.String())
	connSpec.SetInt64("remote_port", int64(spec.dst_port))
	// NOTE: legacy values of local_af are: IPv4 = 0, IPv6 = 1.
	connSpec.SetInt64("local_af", 0)

	return nil
}

// SnapshotValues saves all values from the most recent C.web100_snapshot read by
// Next. Next must be called at least once before calling SnapshotValues.
func (w *Web100) SnapshotValues(snapValues Saver) error {
	snaplog := (*C.web100_log)(w.snaplog)
	snap := (*C.web100_snapshot)(w.snap)

	// Parses variables from most recent web100_snapshot data.
	var w_errno C.int = C.WEB100_ERR_SUCCESS
	group := C.web100_get_log_group(snaplog)

	// The web100 group is a set of web100 variables from a specific agent.
	// M-Lab snaplogs only ever have a single agent ("local") and group
	// (whatever the static set of web100 variables read from
	// /proc/web100/header).
	//
	// To extract each web100 variables corresponding to all the variables
	// in the group, we iterate through each one.
	for v := C.web100_var_head(group, &w_errno); v != nil; v = C.web100_var_next(v, &w_errno) {
		if w_errno != C.WEB100_ERR_SUCCESS {
			return fmt.Errorf(C.GoString(C.web100_strerror(w_errno)))
		}

		// Extract the web100 variable name and type. This will
		// correspond to one of the variables defined in tcp-kis.txt.
		name := C.web100_get_var_name(v)
		var_type := C.web100_get_var_type(v)

		// Read the raw bytes for the variable from the snapshot.
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
			// If it cannot be converted, leave the variable as a string.
			snapValues.SetString(canonicalName, C.GoString((*C.char)(w.text)))
		} else {
			snapValues.SetInt64(canonicalName, value)
		}
	}
	return nil
}

// Close releases resources created by Open.
func (w *Web100) Close() error {
	snap := (*C.web100_snapshot)(w.snap)
	C.web100_snapshot_free(snap)

	snaplog := (*C.web100_log)(w.snaplog)
	err := C.web100_log_close_read(snaplog)
	if err != C.WEB100_ERR_SUCCESS {
		return fmt.Errorf(C.GoString(C.web100_strerror(err)))
	}
	if w.text != nil {
		C.free(w.text)
		w.text = nil
	}
	if w.data != nil {
		C.free(w.data)
		w.data = nil
	}

	// Clear pointer after free.
	w.snaplog = nil
	w.snap = nil
	return nil
}

func LookupError(errnum int) string {
	return C.GoString(C.web100_strerror(C.int(errnum)))
}
