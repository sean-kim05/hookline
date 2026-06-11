// Package id generates unique identifiers for Hookline events and messages.
package id

import (
	"crypto/rand"
	"encoding/hex"
)

// New returns a random 128-bit identifier as a 32-character hex string.
func New() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand.Read does not fail on supported platforms; if it ever
		// does, the process is in an unrecoverable state and should not
		// continue handing out non-unique IDs.
		panic("id: cannot read random bytes: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
