package ecqltest

import (
	"github.com/maraino/ecql"
	"github.com/maraino/go-mock"
)

type Iter struct {
	mock.Mock
}

func NewIter() ecql.Iter {
	return &Iter{}
}

func (m *Iter) TypeScan(i interface{}) bool {
	result := m.Called(i)
	return result.Bool(0)
}

func (m *Iter) Close() error {
	result := m.Called()
	return result.Error(0)
}
