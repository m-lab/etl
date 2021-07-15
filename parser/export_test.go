package parser

// This file contains any whitebox tests (with access to package internals), and wrappers
// to enable blackbox tests to set up environment.
// See https://golang.org/src/net/http/export_test.go.

// InitParserVersionForTest allows tests to rerun initParserVersion after initializing
// environment variables.
var InitParserVersionForTest = initParserVersion

// InitParserGitCommitForTest allows test to rerun initParseGitCommit after initializing
// environement variables.
var InitParserGitCommitForTest = initParserGitCommit
