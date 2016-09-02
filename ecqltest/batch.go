package ecqltest

import (
	"github.com/betable/mock"
	"github.com/maraino/ecql"
)

// Batch is a mock implementation of the Batch interface
type Batch struct {
	mock.Mock
}

// Add is mocks a call to this method.
func (m *Batch) Add(s ...ecql.Statement) ecql.Batch {
	ret := m.Called(s)
	ret0, _ := ret.Get(0).(ecql.Batch)
	return ret0
}

// Apply is mocks a call to this method.
func (m *Batch) Apply() error {
	ret := m.Called()
	ret0, _ := ret.Get(0).(error)
	return ret0
}

// ApplyCAS is mocks a call to this method.
func (m *Batch) ApplyCAS() (bool, error) {
	ret := m.Called()
	ret0, _ := ret.Get(0).(bool)
	ret1, _ := ret.Get(1).(error)
	return ret0, ret1
}
