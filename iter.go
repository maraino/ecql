package ecql

import (
	"github.com/gocql/gocql"
)

type Iter struct {
	statement *Statement
	query     *gocql.Query
	iter      *gocql.Iter
	err       error
}

func (it *Iter) TypeScan(i interface{}) bool {
	m := Map(i)
	if it.iter == nil {
		if query, err := it.statement.query(); err != nil {
			it.err = err
			return false
		} else {
			it.iter = query.Iter()
		}
	}
	return it.iter.MapScan(m)
}

func (it *Iter) Close() error {
	if it.err != nil {
		return it.err
	}
	return it.iter.Close()
}
