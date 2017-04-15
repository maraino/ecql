package ecql

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testStruct struct {
	F1 string `cql:"f1" cqltable:"mytable" cqlkey:"f1"`
	F2 int    `cql:"f22"`
	F3 map[string]string
	F4 *string `cql:""`
	F5 string  `cql:"-"`
}

var testStructNames = []string{"f1", "f22", "f3", "f4"}

func TestRegister(t *testing.T) {
	var tests = []struct {
		I interface{}
	}{
		{testStruct{}},
		{&testStruct{}},
	}

	typ := reflect.TypeOf(testStruct{})
	for _, tc := range tests {
		DeleteRegistry()
		Register(tc.I)
		table, ok := registry.get(typ)
		assert.True(t, ok)
		assert.Equal(t, "mytable", table.Name)
		assert.Equal(t, []string{"f1"}, table.KeyColumns)
		assert.Len(t, table.Columns, 4)
		for i := range testStructNames {
			assert.Equal(t, testStructNames[i], table.Columns[i].Name)
			assert.Equal(t, []int{i}, table.Columns[i].Position)
		}
	}
}

func TestMap(t *testing.T) {
	DeleteRegistry()

	s1 := "string-1"
	s2 := "string-2"
	ts := testStruct{
		F1: "foo",
		F2: 123,
		F3: map[string]string{"foo": "bar"},
		F4: &s1,
		F5: "zar",
	}
	exp := testStruct{
		F1: "foobar",
		F2: 321,
		F3: map[string]string{"foobar": "zar"},
		F4: &s2,
		F5: "zar",
	}

	var tests = []struct {
		name string
		ok   bool
		v    interface{}
		vv   interface{}
	}{
		{"f1", true, ts.F1, exp.F1},
		{"f22", true, ts.F2, exp.F2},
		{"f3", true, ts.F3, exp.F3},
		{"f4", true, ts.F4, exp.F4},
		{"f5", false, nil, exp.F5},
	}

	// With registry and passing as a value
	Register(testStruct{})
	m := Map(ts)
	for _, tc := range tests {
		v, ok := m[tc.name]
		assert.Equal(t, tc.ok, ok)
		assert.Equal(t, tc.v, v)
		m[tc.name] = tc.vv
	}
	assert.NotEqual(t, exp, ts)

	// Without registry and passing as a reference
	DeleteRegistry()
	m = Map(&ts)
	for _, tc := range tests {
		v, ok := m[tc.name]
		vv := reflect.ValueOf(v)
		assert.Equal(t, tc.ok, ok)
		if tc.name != "f5" {
			assert.Equal(t, tc.v, vv.Elem().Interface())
			vv.Elem().Set(reflect.ValueOf(tc.vv))
		}
	}
	assert.Equal(t, exp, ts)
}

func TestBind(t *testing.T) {
	DeleteRegistry()

	s1 := "string-1"
	ts := testStruct{
		F1: "foo",
		F2: 123,
		F3: map[string]string{"foo": "bar"},
		F4: &s1,
		F5: "zar",
	}

	s2 := "string-1"
	exp := []interface{}{"foo", 123, map[string]string{"foo": "bar"}, &s2}

	// With registry and passing as a value
	Register(testStruct{})
	m := Bind(ts)
	assert.Equal(t, exp, m)

	// Without registry and passing as a reference
	DeleteRegistry()
	m = Bind(&ts)
	assert.Equal(t, exp, m)
}

func TestGetTable(t *testing.T) {
	DeleteRegistry()
	// With registry and passing as a value
	Register(testStruct{})
	table := GetTable(testStruct{})
	assert.Equal(t, "mytable", table.Name)

	// Without registry and passing as a reference
	DeleteRegistry()
	table = GetTable(testStruct{})
	assert.Equal(t, "mytable", table.Name)
}

func TestStructOfPanic1(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Panic not executed")
		}
	}()
	Register("string")
}

func TestStructOfPanic2(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Panic not executed")
		}
	}()
	s := "string"
	Register(&s)
}
