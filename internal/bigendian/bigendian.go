package bigendian

import "unsafe"

//=============================================================================

// These provide byte swapping from BigEndian to LittleEndian.
// Much much faster than binary.BigEndian.UintNN.
// NOTE: If this code is used on a BigEndian machine, it should cause unit tests to fail.

// BE16 is a 16-bit big-endian value.
type BE16 [2]byte

// Uint16 returns the 16-bit value in LitteEndian.
func (b BE16) Uint16() uint16 {
	swap := [2]byte{b[1], b[0]}
	return *(*uint16)(unsafe.Pointer(&swap))
}

// BE32 is a 32-bit big-endian value.
type BE32 [4]byte

// Uint32 returns the 32-bit value in LitteEndian.
func (b BE32) Uint32() uint32 {
	swap := [4]byte{b[3], b[2], b[1], b[0]}
	return *(*uint32)(unsafe.Pointer(&swap))
}
