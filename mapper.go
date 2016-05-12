package ecql

import (
	"reflect"
	"strings"
)

var (
	// TAG_COLUMNS is the tag used in the structs to set the column name for a field.
	// If a name is not set, the name would be the lowercase version of the field.
	// If you want to skip a field you can use `cql:"-"`
	TAG_COLUMN = "cql"

	// TAG_TABLE is the tag used in the structs to define the table for a type.
	// If the table is not set it defaults to the type name in lowercase.
	TAG_TABLE = "cqltable"

	// TAG_KEY defines the primary key for the table.
	// If the table uses a composite key you just need to define multiple columns
	// separated by a comma: `cqlkey:"id"` or `cqlkey:"partkey,id"`
	TAG_KEY = "cqlkey"
)

var registry = make(map[reflect.Type]Table)

// Delete registry cleans the registry.
// This would be mainly used in unit testing.
func DeleteRegistry() {
	registry = make(map[reflect.Type]Table)
}

// Register adds the passed struct to the registry to be able to use gocql
// MapScan methods with struct types.
//
// It maps the columns using the struct tag 'cql' or the lowercase of the
// field name. You can skip the mapping of one field using the tag `cql:"-"`
func Register(i interface{}) {
	register(i)
}

// Map creates a new map[string]interface{} where each member in the map
// is a reference to a field in the struct. This allows to assign values
// to a struct using gocql MapScan.
//
// Given a gocql session, the following code will populate the struct 't'
// with the values in the datastore.
// 	var t MyStruct
// 	query := session.Query("select * from mytable where id = ?", "my-id")
// 	m := cql.Map(&t)
// 	err := query.MapScan(m)
func Map(i interface{}) map[string]interface{} {
	columns, _ := MapTable(i)
	return columns
}

// MapTable creates a new map[string]interface{} where each member in the map
// is a reference to a field in the struct. This allows to assign values
// to a struct using gocql MapScan. MapTable also returns the Table with the
// information about the type.
//
// Given a gocql session, the following code will populate the struct 't'
// with the values in the datastore.
// 	var t MyStruct
// 	query := session.Query("select * from mytable where id = ?", "my-id")
// 	m, _ := cql.MapTable(&t)
// 	err := query.MapScan(m)
func MapTable(i interface{}) (map[string]interface{}, Table) {
	v := structOf(i)
	t := v.Type()

	// Get the table or register on the fly if necessary
	table, ok := registry[t]
	if !ok {
		// table = register(i)
		table = register(i)
	}

	columns := make(map[string]interface{})
	for _, col := range table.Columns {
		field := v.Field(col.Position)
		if field.CanAddr() {
			columns[col.Name] = field.Addr().Interface()
		} else {
			columns[col.Name] = field.Interface()
		}
	}
	return columns, table
}

func structOf(i interface{}) reflect.Value {
	v := reflect.ValueOf(i)
	switch v.Kind() {
	case reflect.Struct:
		return v
	case reflect.Ptr, reflect.Interface:
		elem := v.Elem()
		if elem.Kind() == reflect.Struct {
			return elem
		}
	}

	panic("register type is not struct")
}

func register(i interface{}) Table {
	v := structOf(i)
	t := v.Type()

	// Table name defaults to the type name.
	var table Table
	table.Name = t.Name()

	for i, n := 0, t.NumField(); i < n; i++ {
		field := t.Field(i)
		// Get table if available
		name := field.Tag.Get(TAG_TABLE)
		if name != "" {
			table.Name = name
		}

		// Get the key columns
		// FIXME: add a default behavior, ie: first field
		name = field.Tag.Get(TAG_KEY)
		if name != "" {
			table.KeyColumn = name
		}

		// Get columns or field name
		name = field.Tag.Get(TAG_COLUMN)
		if name == "" {
			name = strings.ToLower(field.Name)
		}
		if name != "-" {
			table.Columns = append(table.Columns, Column{name, i})
		}
	}
	registry[t] = table
	return table
}
