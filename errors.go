package ecql

import "errors"

var (
	ErrInvalidQueryType = errors.New("invalid query type")
	ErrInvalidCommand   = errors.New("invalid cql command")
)
