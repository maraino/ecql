package ecql

import (
	"strings"

	"fmt"
)

type queryType int

const (
	selectQuery = iota
	insertQuery
	deleteQuery
	updateQuery
)

const (
	CQL_SELECT = "SELECT * FROM %s WHERE %s = ?"
	CQL_INSERT = "INSERT INTO %s (%s) VALUES (%s)"
	CQL_DELETE = "DELETE FROM %s WHERE %s = ?"
	CQL_UPDATE = "UPDATE %s WHERE %s = ?"
	CQL_AND    = "AND %s = ?"
)

// Table contains the information of a table in cassandra.
type Table struct {
	Name      string
	KeyColumn string
	Columns   []Column
}

// Column contains the information of a column in a table required
// to create a map for it.
type Column struct {
	Name     string
	Position int
}

func (t *Table) BuildQuery(qt queryType) (string, error) {
	var cql string
	switch qt {
	case selectQuery:
		cql = fmt.Sprintf(CQL_SELECT, t.Name, t.KeyColumn)
	case insertQuery:
		cql = fmt.Sprintf(CQL_INSERT, t.Name, t.getCols(), t.getQms())
	case deleteQuery:
		cql = fmt.Sprintf(CQL_DELETE, t.Name, t.KeyColumn)
	case updateQuery:
		cql = CQL_UPDATE
		return "", ErrInvalidQueryType
	default:
		return "", ErrInvalidQueryType
	}

	return cql, nil
}

func (t *Table) getCols() string {
	names := make([]string, len(t.Columns))
	for i := range t.Columns {
		names[i] = t.Columns[i].Name
	}
	return strings.Join(names, ",")
}

func (t *Table) getQms() string {
	return qms(len(t.Columns))
}
