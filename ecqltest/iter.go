package ecqltest

import (
	"github.com/betable/mock"
	"github.com/maraino/ecql"
)

type MockIter struct {
	mock.Mock
}

func NewMockIter() ecql.Iter {
	return &MockIter{}
}

func (m *MockIter) TypeScan(i interface{}) bool {
	result := m.Called(i)
	return result.Bool(0)
}

func (m *MockIter) Close() error {
	result := m.Called()
	return result.Error(0)
}
