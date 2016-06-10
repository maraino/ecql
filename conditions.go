package ecql

import (
	"fmt"
)

type OrderType string

const (
	AscOrder  OrderType = "ASC"
	DescOrder           = "DESC"
)

type OrderBy struct {
	Column string
	OrderType
}

func Asc(col string) OrderBy {
	return OrderBy{col, AscOrder}
}

func Desc(col string) OrderBy {
	return OrderBy{col, DescOrder}
}

type PredicateType int

type Condition struct {
	CQLFragment string
	Values      []interface{}
}

func And(lhs Condition, list ...Condition) Condition {
	cqlfragment := lhs.CQLFragment
	values := lhs.Values
	for _, rhs := range list {
		cqlfragment += " AND " + rhs.CQLFragment
		values = append(values, rhs.Values...)
	}
	return Condition{CQLFragment: cqlfragment, Values: values}
}

func Eq(col string, v interface{}) Condition {
	return Condition{CQLFragment: fmt.Sprintf("%s = ?", col),
		Values: []interface{}{v}}
}

func Gt(col string, v interface{}) Condition {
	return Condition{CQLFragment: fmt.Sprintf("%s > ?", col),
		Values: []interface{}{v}}
}

func Ge(col string, v interface{}) Condition {
	return Condition{CQLFragment: fmt.Sprintf("%s >= ?", col),
		Values: []interface{}{v}}
}

func Lt(col string, v interface{}) Condition {
	return Condition{CQLFragment: fmt.Sprintf("%s < ?", col),
		Values: []interface{}{v}}
}

func Le(col string, v interface{}) Condition {
	return Condition{CQLFragment: fmt.Sprintf("%s <= ?", col),
		Values: []interface{}{v}}
}

func In(col string, v ...interface{}) Condition {
	return Condition{CQLFragment: fmt.Sprintf("%s IN (%s)", qms(len(v)), col),
		Values: v}
}

// EqInt takes is interested in the CQL indexes of the provided struct as a condition
// For convenience, that struct is assumed to follow the same rules as other mappings
func EqInt(i interface{}) Condition {
	values, table := MapTable(i)
	first := true
	condition := True()
	for _, column := range table.KeyColumns {
		keyCondition := Eq(column, values[column])
		if first {
			condition = keyCondition
			first = false
		} else {
			condition = And(condition, keyCondition)
		}

	}
	return condition
}

func True() Condition {
	return Condition{CQLFragment: "true"}
}

// Contains creates the condition 'col CONTAINS value' used to filter elements
// in a collection set, list, or map. Supported on CQL versions >= 3.2.0.
func Contains(col string, v interface{}) Condition {
	return Condition{
		CQLFragment: fmt.Sprintf("%s CONTAINS ?", col),
		Values:      []interface{}{v},
	}
}

// Contains creates the condition 'col CONTAINS KEY value' used to filter elements
// by key in a map. Supported on CQL versions >= 3.2.0.
func ContainsKey(col string, v interface{}) Condition {
	return Condition{
		CQLFragment: fmt.Sprintf("%s CONTAINS KEY ?", col),
		Values:      []interface{}{v},
	}
}
