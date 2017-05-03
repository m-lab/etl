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
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"unsafe"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/schema"
)

var (
	// TODO(prod): eliminate this lock (along with tmpfs).
	web100Lock sync.Mutex
)

// Discoveries:
//  - Not all C macros exist in the "C" namespace.
//  - 'NULL' is usually equivalent to 'nil'

// Web100 maintains state associated with a web100 log file.
type Web100 struct {
	// legacyNames maps legacy web100 variable names to their canonical names.
	legacyNames map[string]string

	// Do not export unsafe pointers.
	snaplog unsafe.Pointer
	snap    unsafe.Pointer
	// temp space for converting web100 variables to string.
	text unsafe.Pointer
	data unsafe.Pointer

	// The original filename created by the NDT server.
	TestId string
	// The time associated with that file.
	LogTime int64
}

// Open prepares a web100 log file for reading. The caller must call Close on
// the returned Web100 instance to release resources.
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

// Next iterates through the web100 log file reading the next snapshot record
// until EOF or an error occurs.
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
		return fmt.Errorf(C.GoString(C.web100_strerror(err)))
	}
	return nil
}

func (w *Web100) Values() (map[string]bigquery.Value, error) {
	results := schema.NewRecord()
	v, err := w.logValues(schema.Map(results["web100_log_entry"]))
	if err != nil {
		return nil, err
	}
	err = w.snapValues(v)
	if err != nil {
		return nil, err
	}
	// TODO(dev): is NPAD also affected in the same way?
	err = fixValues(results["web100_log_entry"])
	if err != nil {
		return nil, err
	}
	return results, nil
}

// fixValues updates web100 log values that need post-processing fix-ups.
// TODO(dev): does this only apply to NDT or is NPAD also affected?
func fixValues(r bigquery.Value) error {
	record := schema.Map(r)
	if record == nil {
		return fmt.Errorf("Can only fix types of map[string]bigquery.Value")
	}
	// TODO(dev): fix these values.
	// Fix StartTimeStamp:
	//  - web100_log_entry.snap.StartTimeStamp: (1000000 * StartTimeStamp + StartTimeUsec)
	// Fix IPv6 addresses in connection_spec:
	//  - web100_log_entry.connection_spec.local_ip
	//  - web100_log_entry.connection_spec.remote_ip
	// Fix local_af:
	//  - web100_log_entry.connection_spec.local_af: IPv4 = 0, IPv6 = 1.
	return nil
}

// logValues returns a map of values from the web100 log. IPv6 address
// connection information is not available and must be set based on a snapshot.
func (w *Web100) logValues(web100LogEntry map[string]bigquery.Value) (map[string]bigquery.Value, error) {
	snaplog := (*C.web100_log)(w.snaplog)
	agent := C.web100_get_log_agent(snaplog)

	web100LogEntry["version"] = C.GoString(C.web100_get_agent_version(agent))
	web100LogEntry["log_time"] = int64(C.web100_get_log_time(snaplog))

	conn := C.web100_get_log_connection(snaplog)

	// NOTE: web100_connection_spec_v6 is never set by the web100 library.
	// NOTE: conn->addrtype is always WEB100_ADDRTYPE_UNKNOWN.
	// So, we expect it to be IPv4 and fix local_ip and remote_ip later if
	// snapshots have IPv6 addresses.
	var spec C.struct_web100_connection_spec
	C.web100_get_connection_spec(conn, &spec)

	// NOTE: web100_connection_spec only contains IPv4 addresses (4 byte values).
	// If the connection was IPv6, the IPv4 addresses here will be 0.0.0.0.
	srcIp := net.IP(C.GoBytes(unsafe.Pointer(&spec.src_addr), 4))
	dstIp := net.IP(C.GoBytes(unsafe.Pointer(&spec.dst_addr), 4))
	connectionSpec := schema.Map(web100LogEntry["connection_spec"])
	connectionSpec["local_ip"] = srcIp.String()
	connectionSpec["local_port"] = int64(spec.src_port)
	connectionSpec["remote_ip"] = dstIp.String()
	connectionSpec["remote_port"] = int64(spec.dst_port)

	// NOTE: legacy values of local_af are: IPv4 = 0, IPv6 = 1.
	connectionSpec["local_af"] = int64(0)

	return web100LogEntry, nil
}

// snapValues converts all variables in the latest snap record into a results map.
func (w *Web100) snapValues(logValues map[string]bigquery.Value) error {
	snaplog := (*C.web100_log)(w.snaplog)
	snap := (*C.web100_snapshot)(w.snap)

	web100snap := schema.Map(logValues["snap"])

	// Parses variables from most recent web100_snapshot data.
	var w_errno C.int = C.WEB100_ERR_SUCCESS
	group := C.web100_get_log_group(snaplog)
	for v := C.web100_var_head(group, &w_errno); v != nil; v = C.web100_var_next(v, &w_errno) {
		if w_errno != C.WEB100_ERR_SUCCESS {
			return nil, fmt.Errorf(C.GoString(C.web100_strerror(w_errno)))
		}

		name := C.web100_get_var_name(v)
		var_type := C.web100_get_var_type(v)

		// Read the raw variable data from the snapshot data.
		errno := C.web100_snap_read(v, snap, w.data)
		if errno != C.WEB100_ERR_SUCCESS {
			return fmt.Errorf(C.GoString(C.web100_strerror(errno)))
		}

		// Convert raw w.data into a string based on var_type.
		// TODO(prod): reimplement web100_value_to_textn to operate on Go types.
		C.web100_value_to_textn((*C.char)(w.text), C.WEB100_VALUE_LEN_MAX,
			(C.WEB100_TYPE)(var_type), w.data)

		// Use the canonical variable name.
		canonicalName := C.GoString(name)
		if _, ok := w.legacyNames[canonicalName]; ok {
			canonicalName = w.legacyNames[canonicalName]
		}

		// TODO(dev): are there any cases where we need unsigned int64?
		// Attempt to convert the current variable to an int64.
		value, err := strconv.ParseInt(C.GoString((*C.char)(w.text)), 10, 64)
		if err != nil {
			// Leave variable as a string.
			web100snap[canonicalName] = C.GoString((*C.char)(w.text))
		} else {
			web100snap[canonicalName] = value
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

func PrettyPrint(results map[string]string) {
	b, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Print(string(b))
}
