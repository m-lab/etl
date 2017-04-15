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
	"flag"
	"fmt"
	"unsafe"
)

var (
	filename = flag.String("filename", "", "Trace filename.")
)

// Necessary web100 functions:
//  + web100_log_open_read(filename)
//  + web100_log_close_read(log_)
//
//   ConvertWeb100VarToNameValue(snap_, var, &var_name, &var_value)
//   for (web100_var *var = web100_var_head(group_);
//        var != NULL;
//        var = web100_var_next(var)) {
//
//   web100_get_log_agent(log_)
//   web100_get_log_time(log_);
//   web100_get_log_group(log_);
//
//   connection_ = web100_get_log_connection(log_);
//   snap_ = web100_snapshot_alloc_from_log(log_);
//   web100_snap_from_log(snap_, log_)
//

// Go structs cannot embed fields with C types.
// https://golang.org/cmd/cgo/#hdr-Go_references_to_C
// Discovered:
//  - 'NULL' is usually equivalent to 'nil'

type Web100 struct {
	log unsafe.Pointer
}

func Open(filename string) (*Web100, error) {
	w := &Web100{}
	var log *C.web100_log

	c_filename := C.CString(filename)
	defer C.free(unsafe.Pointer(c_filename))

	log = C.web100_log_open_read(c_filename)
	fmt.Println("errno", C.web100_errno)
	if log == nil {
		return nil, fmt.Errorf(C.GoString(C.web100_strerror(C.web100_errno)))
	}
	w.log = unsafe.Pointer(log)
	fmt.Println(log)
	snap := C.web100_snapshot_alloc_from_log(log)
	defer C.web100_snapshot_free(snap)
	return w, nil
}

func (w *Web100) Read() error {
	var log *C.web100_log
	var snap *C.web100_snapshot

	log = (*C.web100_log)(w.log)
	fmt.Println("test")
	fmt.Println(log)
	snap = C.web100_snapshot_alloc_from_log(log)
	defer C.web100_snapshot_free(snap)

	err := C.web100_snap_from_log(snap, log)
	if err != C.WEB100_ERR_SUCCESS {
		return fmt.Errorf(C.GoString(C.web100_strerror(C.int(err))))
	}

	group := C.web100_get_log_group(log)
	for v := C.web100_var_head(group); v != nil; v = C.web100_var_next(v) {
		//name := C.web100_get_var_name(v)
		var_size := C.web100_get_var_size(v)
		var_type := C.web100_get_var_type(v)

		var_value := C.malloc(var_size)
		var_text := C.malloc(2 * C.WEB100_VALUE_LEN_MAX) // Use a better size.

		err := C.web100_snap_read(v, snap, var_value)
		if err != C.WEB100_ERR_SUCCESS {
			return fmt.Errorf(C.GoString(C.web100_strerror(C.int(err))))
		}
		C.web100_value_to_textn((*C.char)(var_text), var_size, (C.WEB100_TYPE)(var_type), var_value)
		//fmt.Println(
		//	C.GoString(name),
		//	var_size,
		//	var_type,
		//	C.GoString((*C.char)(var_value)),
		//	C.GoString((*C.char)(var_text)))
	}

	return nil
}

func (w *Web100) Close() error {
	var log *C.web100_log

	log = (*C.web100_log)(w.log)
	err := C.web100_log_close_read(log)
	if err != C.WEB100_ERR_SUCCESS {
		return fmt.Errorf(C.GoString(C.web100_strerror(C.int(err))))
	}

	// Clear pointer after free.
	w.log = nil
	return nil
}

func LookupError(errnum int) string {
	return C.GoString(C.web100_strerror(C.int(errnum)))
}

func main() {
	flag.Parse()

	fmt.Println(LookupError(0))
	// fmt.Println(LookupError(1))
	// fmt.Println(LookupError(2))
	// fmt.Println(LookupError(3))
	// w, err := Open("logs/20170413T01:05:24.133980000Z_c-68-80-50-142.hsd1.pa.comcast.net:53301.s2c_snaplog")
	w, err := Open(*filename)
	if err != nil {
		panic(err)
	}
	err = w.Read()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%#v\n", w)
	w.Close()
	fmt.Printf("%#v\n", w)
}
