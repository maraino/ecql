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

// Get executes a SELECT statements on the table defined in i and sets the
// fields on i with the information present in the database.
func (s *Session) Get(i interface{}, key interface{}) error {
	m, table := MapTable(i)
	if cql, err := table.BuildQuery(selectQuery); err != nil {
		return err
	} else {
		return s.Query(cql, key).MapScan(m)
	}
}

// Set executes an INSERT statement on the the table defined in i and
// saves the information of i in the dtabase.
func (s *Session) Set(i interface{}) error {
	v, table := BindTable(i)
	if cql, err := table.BuildQuery(insertQuery); err != nil {
		return err
	} else {
		return s.Query(cql, v...).Exec()
	}
}

// Del extecutes a delete statement on the table defined in i to
// remove the object i from the database.
func (s *Session) Del(i interface{}) error {
	m, table := MapTable(i)
	if cql, err := table.BuildQuery(deleteQuery); err != nil {
		return err
	} else {
		key := m[table.KeyColumn]
		return s.Query(cql, key).Exec()
	}
}

// Select initializes a SELECT statement.
func (s *Session) Select(i interface{}) *Statement {
	return NewStatement(s).Do(SelectCmd).Map(i)
}

// Select initializes an Insert statement.
func (s *Session) Insert(i interface{}) *Statement {
	return NewStatement(s).Do(InsertCmd).Bind(i)
}

// Select initializes an Insert statement.
func (s *Session) Delete(i interface{}) *Statement {
	return NewStatement(s).Do(DeleteCmd).FromType(i)
}

// Count initializes a SELECT COUNT(1) statement from the table defined by i.
func (s *Session) Count(i interface{}) *Statement {
	return NewStatement(s).Do(CountCmd).FromType(i)
}
