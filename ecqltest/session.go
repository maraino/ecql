package ecqltest

import (
	"github.com/maraino/ecql"
	"github.com/maraino/go-mock"
)

type Session struct {
	mock.Mock
}

func NewSession() ecql.Session {
	return &Session{}
}

func (m *Session) Get(i interface{}, keys ...interface{}) error {
	slice := make([]interface{}, 1)
	slice[0] = i
	slice = append(slice, keys...)
	result := m.Called(slice...)
	return result.Error(0)
}

func (m *Session) Set(i interface{}) error {
	result := m.Called(i)
	return result.Error(0)
}

func (m *Session) Del(i interface{}) error {
	result := m.Called(i)
	return result.Error(0)
}

func (m *Session) Select(i interface{}) ecql.Statement {
	result := m.Called(i)
	return result.Get(0).(ecql.Statement)
}

func (m *Session) Insert(i interface{}) ecql.Statement {
	result := m.Called(i)
	return result.Get(0).(ecql.Statement)
}

func (m *Session) Delete(i interface{}) ecql.Statement {
	result := m.Called(i)
	return result.Get(0).(ecql.Statement)
}

func (m *Session) Update(i interface{}) ecql.Statement {
	result := m.Called(i)
	return result.Get(0).(ecql.Statement)
}

func (m *Session) Count(i interface{}) ecql.Statement {
	result := m.Called(i)
	return result.Get(0).(ecql.Statement)
}
