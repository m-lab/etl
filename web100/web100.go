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

#cgo CFLAGS: -Og
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"sync"
	"unsafe"

	"cloud.google.com/go/bigquery"
)

var (
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
	log  unsafe.Pointer
	snap unsafe.Pointer
}

// Open prepares a web100 log file for reading. The caller must call Close on
// the returned Web100 instance to release resources.
func Open(filename string, legacyNames map[string]string) (*Web100, error) {
	web100Lock.Lock()

	c_filename := C.CString(filename)
	defer C.free(unsafe.Pointer(c_filename))

	// TODO(prod): do not require reading from a file. Accept a byte array.
	log := C.web100_log_open_read(c_filename)
	if log == nil {
		return nil, fmt.Errorf(C.GoString(C.web100_strerror(C.web100_errno)))
	}

	// Pre-allocate a snapshot record.
	snap := C.web100_snapshot_alloc_from_log(log)

	w := &Web100{
		legacyNames: legacyNames,
		log:         unsafe.Pointer(log),
		snap:        unsafe.Pointer(snap),
	}
	return w, nil
}

// Next iterates through the web100 log file reading the next snapshot record
// until EOF or an error occurs.
func (w *Web100) Next() error {
	log := (*C.web100_log)(w.log)
	snap := (*C.web100_snapshot)(w.snap)

	// Read the next web100_snaplog data from underlying file.
	err := C.web100_snap_from_log(snap, log)
	if err == C.EOF {
		return io.EOF
	}
	if err != C.WEB100_ERR_SUCCESS {
		return fmt.Errorf(C.GoString(C.web100_strerror(err)))
	}
	return nil
}

func (w *Web100) Values() (map[string]bigquery.Value, error) {
	v, err := w.logValues()
	if err != nil {
		return nil, err
	}
	v, err = w.snapValues(v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// logValues returns a map of values from the web100 log. IPv6 address
// connection information is not available and must be set based on a snapshot.
func (w *Web100) logValues() (map[string]bigquery.Value, error) {
	log := (*C.web100_log)(w.log)

	agent := C.web100_get_log_agent(log)

	results := make(map[string]bigquery.Value)
	results["web100_log_entry_version"] = C.GoString(C.web100_get_agent_version(agent))

	time := C.web100_get_log_time(log)
	results["web100_log_entry_log_time"] = int64(time)

	conn := C.web100_get_log_connection(log)
	// NOTE: web100_connection_spec_v6 is not filled in by the web100 library.
	// NOTE: addrtype is always WEB100_ADDRTYPE_UNKNOWN.
	// NOTE: legacy values for local_af are: IPv4 = 0, IPv6 = 1.
	results["web100_log_entry_connection_spec_local_af"] = int64(0)

	var spec C.struct_web100_connection_spec
	C.web100_get_connection_spec(conn, &spec)

	// TODO(prod): do not use inet_ntoa because it depends on a static internal buffer.
	addr := C.struct_in_addr{C.in_addr_t(spec.src_addr)}
	results["web100_log_entry_connection_spec_local_ip"] = C.GoString(C.inet_ntoa(addr))
	results["web100_log_entry_connection_spec_local_port"] = int64(spec.src_port)

	addr = C.struct_in_addr{C.in_addr_t(spec.dst_addr)}
	results["web100_log_entry_connection_spec_remote_ip"] = C.GoString(C.inet_ntoa(addr))
	results["web100_log_entry_connection_spec_remote_port"] = int64(spec.dst_port)

	return results, nil
}

// snapValues converts all variables in the latest snap record into a results map.
func (w *Web100) snapValues(logValues map[string]bigquery.Value) (map[string]bigquery.Value, error) {
	log := (*C.web100_log)(w.log)
	snap := (*C.web100_snapshot)(w.snap)

	// TODO(dev): do not re-allocate these buffers on every call.
	var_text := C.calloc(2*C.WEB100_VALUE_LEN_MAX, 1) // Use a better size.
	defer C.free(var_text)

	var_data := C.calloc(C.WEB100_VALUE_LEN_MAX, 1)
	defer C.free(var_data)

	// Parses variables from most recent web100_snapshot data.
	group := C.web100_get_log_group(log)
	for v := C.web100_var_head(group); v != nil; v = C.web100_var_next(v) {

		name := C.web100_get_var_name(v)
		var_type := C.web100_get_var_type(v)

		// Read the raw variable data from the snapshot data.
		errno := C.web100_snap_read(v, snap, var_data)
		if errno != C.WEB100_ERR_SUCCESS {
			return nil, fmt.Errorf(C.GoString(C.web100_strerror(errno)))
		}

		// Convert raw var_data into a string based on var_type.
		// TODO(prod): reimplement web100_value_to_textn to operate on Go types.
		C.web100_value_to_textn((*C.char)(var_text), C.WEB100_VALUE_LEN_MAX, (C.WEB100_TYPE)(var_type), var_data)

		// Use the canonical variable name.
		var canonicalName string
		if _, ok := w.legacyNames[canonicalName]; ok {
			canonicalName = w.legacyNames[canonicalName]
		} else {
			canonicalName = C.GoString(name)
		}

		// TODO(dev): are there any cases where we need unsigned int64?
		// Attempt to convert the current variable to an int64.
		value, err := strconv.ParseInt(C.GoString((*C.char)(var_text)), 10, 64)
		if err != nil {
			// Leave variable as a string.
			logValues[fmt.Sprintf("web100_log_entry_snap_%s", canonicalName)] = C.GoString((*C.char)(var_text))
		} else {
			logValues[fmt.Sprintf("web100_log_entry_snap_%s", canonicalName)] = value
		}
	}
	return logValues, nil
}

// Close releases resources created by Open.
func (w *Web100) Close() error {
	web100Lock.Unlock()

	snap := (*C.web100_snapshot)(w.snap)
	C.web100_snapshot_free(snap)

	log := (*C.web100_log)(w.log)
	err := C.web100_log_close_read(log)
	if err != C.WEB100_ERR_SUCCESS {
		return fmt.Errorf(C.GoString(C.web100_strerror(err)))
	}

	// Clear pointer after free.
	w.log = nil
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
