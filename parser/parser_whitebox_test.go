package parser

// This file contains any whitebox tests (with access to package internals), and wrappers
// to enable blackbox tests to set up environment.

// InitParserVersionForTest allows tests to rerun initParserVersion after initializing
// environment variables.
func InitParserVersionForTest() {
	initParserVersion()
}
