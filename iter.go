package ecql

import (
	"github.com/gocql/gocql"
)

type Iter interface {
	TypeScan(i interface{}) bool
	Close() error
}

type IterImpl struct {
	iter      *gocql.Iter
	statement *StatementImpl
	query     *gocql.Query
	err       error
}

func (it *IterImpl) TypeScan(i interface{}) bool {
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

func (it *IterImpl) Close() error {
	if it.err != nil {
		return it.err
	}
	return it.iter.Close()
}
