package ecql

import (
	"time"

	"github.com/gocql/gocql"
)

// MustUUID parses a 32 digit hexadecimal number (that might contain hypens)
// representing an UUID. If panics if the UUID is invalid.
func MustUUID(input string) gocql.UUID {
	uuid, err := gocql.ParseUUID(input)
	if err != nil {
		panic(err)
	}
	return uuid
}

// Now returns the current local time rounded to milliseconds.
func Now() time.Time {
	return time.Now().Round(time.Millisecond)
}
