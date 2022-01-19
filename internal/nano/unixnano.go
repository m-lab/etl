package nano

import "time"

//=============================================================================

// UnixNano is a Unix timestamp in nanoseconds.
// It provided more efficient basic time operations.
type UnixNano int64

// Sub returns the difference between two unix times.
func (t UnixNano) Sub(other UnixNano) time.Duration {
	return time.Duration(t - other)
}
