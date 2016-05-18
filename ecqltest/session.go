package ecqltest

import (
	"github.com/betable/mock"
	"github.com/maraino/ecql"
)

type MockSession struct {
	mock.Mock
}

func NewMockSession() ecql.Session {
	return &MockSession{}
}

func (m MockSession) Get(i interface{}, keys ...interface{}) error {
	result := m.Called(i, keys)
	return result.Error(0)
}

func (m MockSession) Set(i interface{}) error {
	result := m.Called(i)
	return result.Error(0)
}

func (m MockSession) Del(i interface{}) error {
	result := m.Called(i)
	return result.Error(0)
}

func (m MockSession) Select(i interface{}) ecql.Statement {
	result := m.Called(i)
	return result.Get(0).(ecql.Statement)
}

func (m MockSession) Insert(i interface{}) ecql.Statement {
	result := m.Called(i)
	return result.Get(0).(ecql.Statement)
}

func (m MockSession) Delete(i interface{}) ecql.Statement {
	result := m.Called(i)
	return result.Get(0).(ecql.Statement)
}

func (m MockSession) Count(i interface{}) ecql.Statement {
	result := m.Called(i)
	return result.Get(0).(ecql.Statement)
}
