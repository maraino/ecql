package ecql

import "github.com/gocql/gocql"

// Session is the interface used by users to interact with the database.
type Session interface {
	Get(i interface{}, keys ...interface{}) error
	Set(i interface{}) error
	Del(i interface{}) error
	Select(i interface{}) Statement
	Insert(i interface{}) Statement
	Delete(i interface{}) Statement
	Update(i interface{}) Statement
	Count(i interface{}) Statement
}

type SessionImpl struct {
	*gocql.Session
}

// NewSession initializes a new ecql.Session with gocql.ConsterConfig.
func NewSession(cfg gocql.ClusterConfig) (Session, error) {
	s, err := gocql.NewSession(cfg)
	if err != nil {
		return nil, err
	}

	session := SessionImpl{
		Session: s,
	}

	return &session, nil
}

// Get executes a SELECT statements on the table defined in i and sets the
// fields on i with the information present in the database.
func (s *SessionImpl) Get(i interface{}, keys ...interface{}) error {
	m, table := MapTable(i)
	if cql, err := table.BuildQuery(selectQuery); err != nil {
		return err
	} else {
		return s.Query(cql, keys...).MapScan(m)
	}
}

// Set executes an INSERT statement on the the table defined in i and
// saves the information of i in the dtabase.
func (s *SessionImpl) Set(i interface{}) error {
	v, table := BindTable(i)
	if cql, err := table.BuildQuery(insertQuery); err != nil {
		return err
	} else {
		return s.Query(cql, v...).Exec()
	}
}

// Del extecutes a delete statement on the table defined in i to
// remove the object i from the database.
func (s *SessionImpl) Del(i interface{}) error {
	m, table := MapTable(i)
	if cql, err := table.BuildQuery(deleteQuery); err != nil {
		return err
	} else {
		keys := make([]interface{}, len(table.KeyColumns))
		for i, name := range table.KeyColumns {
			keys[i] = m[name]
		}
		return s.Query(cql, keys...).Exec()
	}
}

// Select initializes a SELECT statement.
func (s *SessionImpl) Select(i interface{}) Statement {
	return NewStatement(s).Do(SelectCmd).Map(i)
}

// Select initializes an INSERT statement.
func (s *SessionImpl) Insert(i interface{}) Statement {
	return NewStatement(s).Do(InsertCmd).Bind(i)
}

// Select initializes an DELETE statement.
func (s *SessionImpl) Delete(i interface{}) Statement {
	return NewStatement(s).Do(DeleteCmd).FromType(i)
}

// Update initializes an UPDATE statement.
func (s *SessionImpl) Update(i interface{}) Statement {
	return NewStatement(s).Do(UpdateCmd).FromType(i)
}

// Count initializes a SELECT COUNT(1) statement from the table defined by i.
func (s *SessionImpl) Count(i interface{}) Statement {
	return NewStatement(s).Do(CountCmd).FromType(i)
}
