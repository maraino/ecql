package ecql

import (
	"github.com/gocql/gocql"
)

func MustUUID(input string) gocql.UUID {
	uuid, err := gocql.ParseUUID(input)
	if err != nil {
		panic(err)
	}
	return uuid
}
