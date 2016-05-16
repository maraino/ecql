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

func And(lhs Condition, rhs Condition) Condition {
	return Condition{CQLFragment: fmt.Sprintf("(%s AND %s)", lhs.CQLFragment, rhs.CQLFragment),
		Values: append(lhs.Values, rhs.Values)}
}

func Or(lhs Condition, rhs Condition) Condition {
	return Condition{CQLFragment: fmt.Sprintf("(%s OR %s)", lhs.CQLFragment, rhs.CQLFragment),
		Values: append(lhs.Values, rhs.Values)}
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
