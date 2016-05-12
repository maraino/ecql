package ecql

import "github.com/gocql/gocql"

const (
	CQL_SELECT = "SELECT * FROM %s WHERE %s = ?"
	CQL_INSERT = "INSERT INTO %s (%s) VALUES (%s)"
	CQL_DELETE = "DELETE FROM %s WHERE %s = ?"
	CQL_UPDATE = "UPDATE %s WHERE %s = ?"
	CQL_AND    = "AND %s = ?"
)

// Session is the interfaced used by users to interact with the database.
type Session struct {
	*gocql.Session
}

// NewSession initializes a new ecql.Session with gocql.ConsterConfig.
func NewSession(cfg gocql.ClusterConfig) (*Session, error) {
	s, err := gocql.NewSession(cfg)
	if err != nil {
		return nil, err
	}

	return &Session{
		Session: s,
	}, nil
}

// Select executes a select statements on the table defined in i and sets the
// fields on i with the information present in the database.
func (s *Session) Select(i interface{}, key interface{}) error {
	m, table := MapTable(i)
	if cql, err := table.BuildQuery(selectQuery); err != nil {
		return err
	} else {
		query := s.Query(cql, key)
		return query.MapScan(m)
	}
}
