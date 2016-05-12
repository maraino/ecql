package ecql

import "fmt"

type queryType int

const (
	selectQuery = iota
	insertQuery
	deleteQuery
	updateQuery
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
		cql = CQL_INSERT
		return "", ErrInvalidQueryType
	case deleteQuery:
		cql = CQL_DELETE
		return "", ErrInvalidQueryType
	case updateQuery:
		cql = CQL_UPDATE
		return "", ErrInvalidQueryType
	default:
		return "", ErrInvalidQueryType
	}

	return cql, nil
}
