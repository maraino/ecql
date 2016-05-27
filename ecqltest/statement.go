package ecqltest

import (
	"github.com/maraino/ecql"
	"github.com/maraino/go-mock"
)

type Statement struct {
	mock.Mock
}

func NewStatement() ecql.Statement {
	return &Statement{}
}

func (m *Statement) TypeScan() error {
	var result = m.Called()
	return result.Error(0)
}

func (m *Statement) Scan(i ...interface{}) error {
	var result = m.Called(i...)
	return result.Error(0)
}

func (m *Statement) Exec() error {
	var result = m.Called()
	return result.Error(0)
}

func (m *Statement) Iter() ecql.Iter {
	var result = m.Called()
	return result.Get(0).(ecql.Iter)
}

func (m *Statement) Do(cmd ecql.Command) ecql.Statement {
	var result = m.Called(cmd)
	return result.Get(0).(ecql.Statement)
}

func (m *Statement) From(table string) ecql.Statement {
	var result = m.Called(table)
	return result.Get(0).(ecql.Statement)
}

func (m *Statement) FromType(i interface{}) ecql.Statement {
	var result = m.Called(i)
	return result.Get(0).(ecql.Statement)
}

func (m *Statement) Columns(columns ...string) ecql.Statement {
	slice := make([]interface{}, len(columns))
	for i, v := range columns {
		slice[i] = v
	}
	var result = m.Called(slice...)
	return result.Get(0).(ecql.Statement)
}

func (m *Statement) Set(column string, value interface{}) ecql.Statement {
	var result = m.Called(column, value)
	return result.Get(0).(ecql.Statement)
}

func (m *Statement) Where(cond ...ecql.Condition) ecql.Statement {
	slice := make([]interface{}, len(cond))
	for i, v := range cond {
		slice[i] = v
	}

	var result = m.Called(slice...)
	return result.Get(0).(ecql.Statement)
}

func (m *Statement) OrderBy(order ...ecql.OrderBy) ecql.Statement {
	slice := make([]interface{}, len(order))
	for i, v := range order {
		slice[i] = v
	}
	var result = m.Called(slice...)
	return result.Get(0).(ecql.Statement)
}

func (m *Statement) Bind(i interface{}) ecql.Statement {
	var result = m.Called(i)
	return result.Get(0).(ecql.Statement)
}

func (m *Statement) Map(i interface{}) ecql.Statement {
	var result = m.Called(i)
	return result.Get(0).(ecql.Statement)
}

func (m *Statement) Limit(n int) ecql.Statement {
	var result = m.Called(n)
	return result.Get(0).(ecql.Statement)
}

func (m *Statement) TTL(seconds int) ecql.Statement {
	var result = m.Called(seconds)
	return result.Get(0).(ecql.Statement)
}
