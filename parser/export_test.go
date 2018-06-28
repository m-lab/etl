package parser

// This file contains any whitebox tests (with access to package internals), and wrappers
// to enable blackbox tests to set up environment.

// InitParserVersionForTest allows tests to rerun initParserVersion after initializing
// environment variables.
// See https://groups.google.com/forum/#!topic/golang-nuts/v1TXLIRZjv4 and
// https://golang.org/src/net/http/export_test.go
var InitParserVersionForTest = initParserVersion
