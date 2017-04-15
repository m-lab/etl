package main

// The Cgo directives must immediately preceed 'import "C"' below.
// Example:
//   $ wget http://.../web100_userland-1.8.tar.gz
//   $ tar -xvf web100_userland-1.8.tar.gz
//   $ pushd web100_userland
//   $ ./configure --prefix=$PWD/build
//   $ make && make install
//   $ popd
//   $ go build

/*
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <web100.h>
#include <web100-int.h>

web100_log *get_null_log() {
	return NULL;
}
*/
import "C"

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"unsafe"
	//"github.com/kr/pretty"
)

var (
	filename = flag.String("filename", "", "Trace filename.")
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

// Next iterates through the web100 log file and returns the next snapshot
// record in the form of a map.
func (w *Web100) Next() (map[string]string, error) {
	results := make(map[string]string)

	log := (*C.web100_log)(w.log)
	snap := (*C.web100_snapshot)(w.snap)

	// Read the next web100_snaplog data from underlying file.
	err := C.web100_snap_from_log(snap, log)
	if err == C.EOF {
		return nil, io.EOF
	}
	if err != C.WEB100_ERR_SUCCESS {
		return nil, fmt.Errorf(C.GoString(C.web100_strerror(err)))
	}

	// Parses variables from most recent web100_snapshot data.
	group := C.web100_get_log_group(log)
	for v := C.web100_var_head(group); v != nil; v = C.web100_var_next(v) {

		name := C.web100_get_var_name(v)
		var_size := C.web100_get_var_size(v)
		var_type := C.web100_get_var_type(v)

		var_data := C.malloc(var_size)
		defer C.free(var_data)

		var_text := C.malloc(2 * C.WEB100_VALUE_LEN_MAX) // Use a better size.
		defer C.free(var_text)

		// Read the raw variable data from the snapshot data.
		err := C.web100_snap_read(v, snap, var_data)
		if err != C.WEB100_ERR_SUCCESS {
			return nil, fmt.Errorf(C.GoString(C.web100_strerror(err)))
		}

		// Convert raw var_data into a string based on var_type.
		C.web100_value_to_textn((*C.char)(var_text), var_size, (C.WEB100_TYPE)(var_type), var_data)
		results[C.GoString(name)] = C.GoString((*C.char)(var_text))
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

func Pprint(results map[string]string) {
	b, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Print(string(b))
}

func main() {
	flag.Parse()

	fmt.Println(LookupError(0))
	w, err := Open(*filename)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%#v\n", w)

	// Find and print the last web100 snapshot record.
	var results map[string]string
	var current map[string]string
	for {
		current, err = w.Next()
		if err != nil {
			break
		}
		results = current
	}
	if err != io.EOF {
		panic(err)
	}
	Pprint(results)
	w.Close()
	fmt.Printf("%#v\n", w)
}
