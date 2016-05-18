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
	var result = m.Called(i)
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

func (m *Statement) Where(cond ...ecql.Condition) ecql.Statement {
	var result = m.Called(cond)
	return result.Get(0).(ecql.Statement)
}

func (m *Statement) OrderBy(order ...ecql.OrderBy) ecql.Statement {
	var result = m.Called(order)
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
