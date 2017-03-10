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
	countQuery
)

// Table contains the information of a table in cassandra.
type Table struct {
	Name       string
	KeyColumns []string
	Columns    []Column
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
		cql = fmt.Sprintf("SELECT %s FROM %s WHERE %s", t.getCols(), t.Name, appendCols(t.KeyColumns))
	case insertQuery:
		cql = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", t.Name, t.getCols(), t.getQms())
	case deleteQuery:
		cql = fmt.Sprintf("DELETE FROM %s WHERE %s", t.Name, appendCols(t.KeyColumns))
	case updateQuery:
		// cql = "UPDATE %s WHERE %s = ?"
		return "", ErrInvalidQueryType
	case countQuery:
		cql = fmt.Sprintf("SELECT COUNT(1) FROM %s WHERE %s", t.Name, appendCols(t.KeyColumns))
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

func appendCols(cols []string) string {
	parts := make([]string, len(cols))
	for i := range cols {
		parts[i] = fmt.Sprintf("%s = ?", cols[i])
	}
	return strings.Join(parts, " AND ")
}
