package ecql

import "github.com/gocql/gocql"

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
		return s.Query(cql, key).MapScan(m)
	}
}

// Insert executes an insert statement on the the table defined in i and
// saves the information of i in the dtabase.
func (s *Session) Insert(i interface{}) error {
	v, table := BindTable(i)
	if cql, err := table.BuildQuery(insertQuery); err != nil {
		return err
	} else {
		return s.Query(cql, v...).Exec()
	}
}

// Delete extecutes a delete statement on the table defined in i to
// remove the object i from the database.
func (s *Session) Delete(i interface{}) error {
	m, table := MapTable(i)
	if cql, err := table.BuildQuery(deleteQuery); err != nil {
		return err
	} else {
		key := m[table.KeyColumn]
		return s.Query(cql, key).Exec()
	}
}
