package ecql

import (
	"fmt"
	"strings"

	"github.com/gocql/gocql"
)

type Command int

const (
	SelectCmd Command = iota
	InsertCmd
	DeleteCmd
	UpdateCmd
	CountCmd
)

type Statement struct {
	session    *Session
	Command    Command
	Table      string
	Conditions []Condition
	Values     []interface{}
	LimitValue int
	TTLValue   int
	table      Table
}

func NewStatement(sess *Session) *Statement {
	return &Statement{session: sess}
}

func (s *Statement) TypeScan(i interface{}) error {
	m, table := MapTable(i)
	s.Table = table.Name
	if query, err := s.query(); err != nil {
		return err
	} else {
		return query.MapScan(m)
	}
}

func (s *Statement) Scan(i ...interface{}) error {
	if query, err := s.query(); err != nil {
		return err
	} else {
		return query.Scan(i...)
	}
}

func (s *Statement) Exec() error {
	if query, err := s.query(); err != nil {
		return err
	} else {
		return query.Exec()
	}
}

func (s *Statement) query() (*gocql.Query, error) {
	var cql []string
	switch s.Command {
	case SelectCmd:
		cql = append(cql, fmt.Sprintf("SELECT * FROM %s", s.Table))
	case InsertCmd:
		cql = append(cql, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", s.Table, s.table.getCols(), s.table.getQms()))
	case DeleteCmd:
		cql = append(cql, fmt.Sprintf("DELETE FROM %s", s.Table))
	// case UpdateCmd:
	// 	cql = append(cql, fmt.Sprintf("UPDATE %s", s.Table))
	case CountCmd:
		cql = append(cql, fmt.Sprintf("SELECT COUNT(1) FROM %s", s.Table))
	default:
		return nil, ErrInvalidCommand
	}

	var args []interface{}

	if len(s.Conditions) > 0 {
		cql = append(cql, "WHERE")
		for _, cond := range s.Conditions {
			args = append(args, cond.Value)
			switch cond.Predicate {
			case EqPredicate:
				cql = append(cql, fmt.Sprintf("%s = ?", cond.Column))
			case GtPredicate:
				cql = append(cql, fmt.Sprintf("%s > ?", cond.Column))
			case GePredicate:
				cql = append(cql, fmt.Sprintf("%s >= ?", cond.Column))
			case LtPredicate:
				cql = append(cql, fmt.Sprintf("%s < ?", cond.Column))
			case LePredicate:
				cql = append(cql, fmt.Sprintf("%s <= ?", cond.Column))
			// FIXME
			case InPredicate:
				cql = append(cql, fmt.Sprintf("%s IN (%s)", qms(len(cond.Values)), cond.Column))
			}
		}
	}

	if len(s.Values) > 0 {
		for i := range s.Values {
			args = append(args, s.Values[i])
		}
	}

	fmt.Println(strings.Join(cql, " "), args, s.Values)
	return s.session.Query(strings.Join(cql, " "), args...), nil
}

func (s *Statement) Do(cmd Command) *Statement {
	s.Command = cmd
	return s
}

func (s *Statement) From(table string) *Statement {
	s.Table = table
	return s
}

func (s *Statement) FromType(i interface{}) *Statement {
	table := GetTable(i)
	return s.From(table.Name)
}

func (s *Statement) Where(cond ...Condition) *Statement {
	s.Conditions = cond
	return s
}

func (s *Statement) Bind(i interface{}) *Statement {
	v, table := BindTable(i)
	s.Values = v
	s.table = table
	s.Table = table.Name
	return s
}

func (s *Statement) Limit(n int) *Statement {
	s.LimitValue = n
	return s
}

func (s *Statement) TTL(seconds int) *Statement {
	s.TTLValue = seconds
	return s
}

func qms(l int) string {
	switch l {
	case 0:
		return ""
	case 1:
		return "?"
	default:
		return strings.Repeat("?,", l-1) + "?"
	}
}
