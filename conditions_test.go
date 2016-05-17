package ecql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var mockCond0 = Condition{CQLFragment: "mock cql 0"}
var mockCond1 = Condition{CQLFragment: "mock cql 1", Values: []interface{}{"one"}}
var mockCond2 = Condition{CQLFragment: "mock cql 2", Values: []interface{}{1, 2}}
var mockCond3 = Condition{CQLFragment: "mock cql 3", Values: []interface{}{1, 2, 3}}
var mockCond4 = Condition{CQLFragment: "mock cql 4", Values: []interface{}{1, 2, 3, 4}}

var mockOpData = map[string]interface{}{
	"name":     "fred",
	"brooklyn": 99,
	"index":    333,
	"key":      "val",
	"coins":    -90210,
}

type MockModel struct {
	MockKey1 string `cql:"key1" cqlkey:"key1,key2"`
	MockKey2 string `cql:"key2"`
	Mockval  string `cql:"data"`
}

func TestTrue(t *testing.T) {
	expected := Condition{CQLFragment: "true"}
	result := True()
	assert.Equal(t, expected, result)
}

func TestAndPassthrough(t *testing.T) {
	expected := mockCond3
	result := And(mockCond3)
	assert.Equal(t, expected, result)
}

func TestAndWith2Elements(t *testing.T) {
	expected := Condition{CQLFragment: "mock cql 2 AND mock cql 3", Values: []interface{}{1, 2, 1, 2, 3}}
	result := And(mockCond2, mockCond3)
	assert.Equal(t, expected, result)
}

func TestAndWith3Elements(t *testing.T) {
	expected := Condition{CQLFragment: "mock cql 1 AND mock cql 0 AND mock cql 4", Values: []interface{}{"one", 1, 2, 3, 4}}
	result := And(mockCond1, mockCond0, mockCond4)
	assert.Equal(t, expected, result)
}

func TestEq(t *testing.T) {
	for col, val := range mockOpData {
		expected := Condition{CQLFragment: col + " = ?", Values: []interface{}{val}}
		result := Eq(col, val)
		assert.Equal(t, expected, result)
	}
}

func TestGt(t *testing.T) {
	for col, val := range mockOpData {
		expected := Condition{CQLFragment: col + " > ?", Values: []interface{}{val}}
		result := Gt(col, val)
		assert.Equal(t, expected, result)
	}
}

func TestGe(t *testing.T) {
	for col, val := range mockOpData {
		expected := Condition{CQLFragment: col + " >= ?", Values: []interface{}{val}}
		result := Ge(col, val)
		assert.Equal(t, expected, result)
	}
}

func TestLt(t *testing.T) {
	for col, val := range mockOpData {
		expected := Condition{CQLFragment: col + " < ?", Values: []interface{}{val}}
		result := Lt(col, val)
		assert.Equal(t, expected, result)
	}
}

func TestLe(t *testing.T) {
	for col, val := range mockOpData {
		expected := Condition{CQLFragment: col + " <= ?", Values: []interface{}{val}}
		result := Le(col, val)
		assert.Equal(t, expected, result)
	}
}

func TestEqInt(t *testing.T) {
	mockInt := MockModel{MockKey2: "second part", MockKey1: "first part", Mockval: "ignore this"}
	expected := Condition{CQLFragment: "key1 = ? AND key2 = ?", Values: []interface{}{"first part", "second part"}}
	result := EqInt(mockInt)
	assert.Equal(t, expected, result)

}
