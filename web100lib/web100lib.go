// web100lib provides Go bindings to some functions in the web100 library.
package web100lib

// Cgo directives must immediately preceed 'import "C"' below.

/*
#include <stdio.h>
#include <stdlib.h>
#include <web100.h>
#include <web100-int.h>

#include <arpa/inet.h>


void print_bytes(size_t var_size, void *var_data) {
	unsigned char *data = (unsigned char *)var_data;
	int i = 0;
	fflush(stdout);
	for (i = 0; i < var_size; i++ ) {
		fprintf(stdout, "%02x ", data[i]);
	}
	fflush(stdout);
}
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"io"
	"unsafe"
)

// Necessary web100 functions:
//  + web100_log_open_read(filename)
//  + web100_log_close_read(log_)
//  + snap_ = web100_snapshot_alloc_from_log(log_);
//  + web100_snap_from_log(snap_, log_)
//
//  + for (web100_var *var = web100_var_head(group_);
//  +      var != NULL;
//  +      var = web100_var_next(var)) {
//
//   web100_get_log_agent(log_)
//   web100_get_log_time(log_);
//   + web100_get_log_group(log_);
//
//   connection_ = web100_get_log_connection(log_);

// Notes:
//  - See: https://golang.org/cmd/cgo/#hdr-Go_references_to_C
//
// Discoveries:
//  - Not all C macros exist in the "C" namespace.
//  - 'NULL' is usually equivalent to 'nil'

// Web100 maintains state associated with a web100 log file.
type Web100 struct {
	// Do not export unsafe pointers.
	log  unsafe.Pointer
	snap unsafe.Pointer
}

// Open prepares a web100 log file for reading. The caller must call Close on
// the returned Web100 instance to release resources.
func Open(filename string) (*Web100, error) {
	c_filename := C.CString(filename)
	defer C.free(unsafe.Pointer(c_filename))

	log := C.web100_log_open_read(c_filename)
	if log == nil {
		return nil, fmt.Errorf(C.GoString(C.web100_strerror(C.web100_errno)))
	}

	// Pre-allocate a snapshot record.
	snap := C.web100_snapshot_alloc_from_log(log)

	w := &Web100{
		log:  unsafe.Pointer(log),
		snap: unsafe.Pointer(snap),
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

// LogValues returns a map of values from the web100 log. IPv6 address
// connection information is not available.
func (w *Web100) LogValues() (map[string]string, error) {
	log := (*C.web100_log)(w.log)

	agent := C.web100_get_log_agent(log)

	results := make(map[string]string)
	results["web100_log_entry.version"] = C.GoString(C.web100_get_agent_version(agent))

	time := C.web100_get_log_time(log)
	results["web100_log_entry.log_time"] = fmt.Sprintf("%d", int64(time))

	conn := C.web100_get_log_connection(log)
	// NOTE: web100_connection_spec_v6 is not filled in by the web100 library.
	// NOTE: addrtype is always WEB100_ADDRTYPE_UNKNOWN.
	results["web100_log_entry.connection_spec.local_af"] = ""
	var spec C.struct_web100_connection_spec
	C.web100_get_connection_spec(conn, &spec)

	addr := C.struct_in_addr{C.in_addr_t(spec.src_addr)}
	results["web100_log_entry.connection_spec.local_ip"] = C.GoString(C.inet_ntoa(addr))
	results["web100_log_entry.connection_spec.local_port"] = fmt.Sprintf("%d", spec.src_port)

	addr = C.struct_in_addr{C.in_addr_t(spec.dst_addr)}
	results["web100_log_entry.connection_spec.remote_ip"] = C.GoString(C.inet_ntoa(addr))
	results["web100_log_entry.connection_spec.remote_port"] = fmt.Sprintf("%d", spec.dst_port)

	return results, nil
}

// SnapValues converts all variables in the latest snap record into a results
// map.
func (w *Web100) SnapValues(legacyNames map[string]string) (map[string]string, error) {
	log := (*C.web100_log)(w.log)
	snap := (*C.web100_snapshot)(w.snap)

	results := make(map[string]string)

	var_text := C.calloc(2*C.WEB100_VALUE_LEN_MAX, 1) // Use a better size.
	defer C.free(var_text)

	var_data := C.calloc(C.WEB100_VALUE_LEN_MAX, 1)
	defer C.free(var_data)

	// Parses variables from most recent web100_snapshot data.
	group := C.web100_get_log_group(log)
	for v := C.web100_var_head(group); v != nil; v = C.web100_var_next(v) {

		name := C.web100_get_var_name(v)
		var_size := C.web100_get_var_size(v)
		var_type := C.web100_get_var_type(v)

		// Read the raw variable data from the snapshot data.
		err := C.web100_snap_read(v, snap, var_data)
		if err != C.WEB100_ERR_SUCCESS {
			return nil, fmt.Errorf(C.GoString(C.web100_strerror(err)))
		}

		// Convert raw var_data into a string based on var_type.
		C.web100_value_to_textn((*C.char)(var_text), C.WEB100_VALUE_LEN_MAX, (C.WEB100_TYPE)(var_type), var_data)
		newName := C.GoString(name)
		if _, ok := legacyNames[newName]; ok {
			newName = legacyNames[newName]
		}
		results[fmt.Sprintf("web100_log_entry.snap.%s", newName)] = C.GoString((*C.char)(var_text))

		fmt.Printf("name: %-20s type: %d %d size %d: %-30s ", C.GoString(name), C.WEB100_TYPE_INTEGER32, var_type, var_size,
			C.GoString((*C.char)(var_text)))
		C.print_bytes(var_size, var_data)
		fmt.Printf("\n")
	}

	return results, nil
}

// Close releases resources created by Open.
func (w *Web100) Close() error {
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
