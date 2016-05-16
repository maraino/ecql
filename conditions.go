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
	cqlfragment := "(" + lhs.CQLFragment
	values := lhs.Values
	for _, rhs := range list {
		cqlfragment += " AND " + rhs.CQLFragment
		values = append(values, rhs.Values)
	}
	cqlfragment += ")"
	return Condition{CQLFragment: cqlfragment, Values: values}
}

func Or(lhs Condition, list ...Condition) Condition {
	cqlfragment := "(" + lhs.CQLFragment
	values := lhs.Values
	for _, rhs := range list {
		cqlfragment += " OR " + rhs.CQLFragment
		values = append(values, rhs.Values)
	}
	cqlfragment += ")"
	return Condition{CQLFragment: cqlfragment, Values: values}
}

func NEq(col string, v interface{}) Condition {
	return Condition{CQLFragment: fmt.Sprintf("%s != ?", col),
		Values: []interface{}{v}}
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
