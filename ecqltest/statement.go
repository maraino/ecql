package ecqltest

import (
	"github.com/maraino/ecql"
	"github.com/maraino/go-mock"
)

type MockStatement struct {
	mock.Mock
}

func NewMockStatement() ecql.Statement {
	return &MockStatement{}
}

func (m *MockStatement) TypeScan() error {
	var result = m.Called()
	return result.Error(0)
}

func (m *MockStatement) Scan(i ...interface{}) error {
	var result = m.Called(i)
	return result.Error(0)
}

func (m *MockStatement) Exec() error {
	var result = m.Called()
	return result.Error(0)
}

func (m *MockStatement) Iter() ecql.Iter {
	var result = m.Called()
	return result.Get(0).(ecql.Iter)
}

func (m *MockStatement) Do(cmd ecql.Command) ecql.Statement {
	var result = m.Called(cmd)
	return result.Get(0).(ecql.Statement)
}

func (m *MockStatement) From(table string) ecql.Statement {
	var result = m.Called(table)
	return result.Get(0).(ecql.Statement)
}

func (m *MockStatement) FromType(i interface{}) ecql.Statement {
	var result = m.Called(i)
	return result.Get(0).(ecql.Statement)
}

func (m *MockStatement) Where(cond ...ecql.Condition) ecql.Statement {
	var result = m.Called(cond)
	return result.Get(0).(ecql.Statement)
}

func (m *MockStatement) OrderBy(order ...ecql.OrderBy) ecql.Statement {
	var result = m.Called(order)
	return result.Get(0).(ecql.Statement)
}

func (m *MockStatement) Bind(i interface{}) ecql.Statement {
	var result = m.Called(i)
	return result.Get(0).(ecql.Statement)
}

func (m *MockStatement) Map(i interface{}) ecql.Statement {
	var result = m.Called(i)
	return result.Get(0).(ecql.Statement)
}

func (m *MockStatement) Limit(n int) ecql.Statement {
	var result = m.Called(n)
	return result.Get(0).(ecql.Statement)
}

func (m *MockStatement) TTL(seconds int) ecql.Statement {
	var result = m.Called(seconds)
	return result.Get(0).(ecql.Statement)
}
