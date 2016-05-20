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

type Statement interface {
	TypeScan() error
	Scan(i ...interface{}) error
	Exec() error
	Iter() Iter
	Do(cmd Command) Statement
	From(table string) Statement
	FromType(i interface{}) Statement
	Set(column string, value interface{}) Statement
	Where(cond ...Condition) Statement
	OrderBy(order ...OrderBy) Statement
	Bind(i interface{}) Statement
	Map(i interface{}) Statement
	Limit(n int) Statement
	TTL(seconds int) Statement
}

type StatementImpl struct {
	session     *SessionImpl
	Command     Command
	Table       Table
	Conditions  *Condition
	Orders      []OrderBy
	Assignments map[string]interface{}
	LimitValue  int
	TTLValue    int
	mapping     map[string]interface{}
	values      []interface{}
}

func NewStatement(sess *SessionImpl) Statement {
	return &StatementImpl{session: sess}
}

func (s *StatementImpl) TypeScan() error {
	if query, err := s.query(); err != nil {
		return err
	} else {
		return query.MapScan(s.mapping)
	}
}

func (s *StatementImpl) Scan(i ...interface{}) error {
	if query, err := s.query(); err != nil {
		return err
	} else {
		return query.Scan(i...)
	}
}

func (s *StatementImpl) Exec() error {
	if query, err := s.query(); err != nil {
		return err
	} else {
		return query.Exec()
	}
}

func (s *StatementImpl) Iter() Iter {
	return &IterImpl{
		statement: s,
	}
}

func (s *StatementImpl) query() (*gocql.Query, error) {
	var cql []string

	switch s.Command {
	case SelectCmd:
		cql = append(cql, fmt.Sprintf("SELECT * FROM %s", s.Table.Name))
	case InsertCmd:
		cql = append(cql, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", s.Table.Name, s.Table.getCols(), s.Table.getQms()))
	case DeleteCmd:
		cql = append(cql, fmt.Sprintf("DELETE FROM %s", s.Table.Name))
	case UpdateCmd:
		cql = append(cql, fmt.Sprintf("UPDATE %s", s.Table.Name))
		if s.TTLValue > 0 {
			cql = append(cql, fmt.Sprintf("USING TTL %d", s.TTLValue))
		}
	case CountCmd:
		cql = append(cql, fmt.Sprintf("SELECT COUNT(1) FROM %s", s.Table.Name))
	default:
		return nil, ErrInvalidCommand
	}

	var args []interface{}

	// SET col = ?
	if s.Command == UpdateCmd {
		i := 0
		assignments := make([]string, len(s.Assignments))
		for col, v := range s.Assignments {
			assignments[i] = fmt.Sprintf("%s = ?", col)
			args = append(args, v)
			i++
		}
		if i > 0 {
			cql = append(cql, "SET", strings.Join(assignments, ", "))
		}
	}

	if s.Conditions != nil {
		cql = append(cql, "WHERE", s.Conditions.CQLFragment)
		args = append(args, s.Conditions.Values...)
	}

	if len(s.values) > 0 {
		for i := range s.values {
			args = append(args, s.values[i])
		}
	}

	if s.Command == SelectCmd {
		if len(s.Orders) > 0 {
			cql = append(cql, "ORDER BY")
			orders := make([]string, len(s.Orders))
			for i, o := range s.Orders {
				orders[i] = fmt.Sprintf("%s %s", o.Column, o.OrderType)
			}
			cql = append(cql, strings.Join(orders, ", "))
		}

		if s.LimitValue > 0 {
			cql = append(cql, fmt.Sprintf("LIMIT %d", s.LimitValue))
		}
	}

	if s.Command == InsertCmd && s.TTLValue > 0 {
		cql = append(cql, fmt.Sprintf("USING TTL %d", s.TTLValue))
	}

	return s.session.Query(strings.Join(cql, " "), args...), nil
}

func (s *StatementImpl) Do(cmd Command) Statement {
	s.Command = cmd
	return s
}

func (s *StatementImpl) From(table string) Statement {
	s.Table = Table{Name: table}
	return s
}

func (s *StatementImpl) FromType(i interface{}) Statement {
	table := GetTable(i)
	return s.From(table.Name)
}

func (s *StatementImpl) Set(column string, value interface{}) Statement {
	if s.Assignments == nil {
		s.Assignments = make(map[string]interface{})
	}
	s.Assignments[column] = value
	return s
}

// Where Conditionss are implicitly And with each other
func (s *StatementImpl) Where(cond ...Condition) Statement {
	and := And(cond[0], cond[1:]...)
	s.Conditions = &and
	return s
}

func (s *StatementImpl) OrderBy(order ...OrderBy) Statement {
	s.Orders = order
	return s
}

func (s *StatementImpl) Bind(i interface{}) Statement {
	s.values, s.Table = BindTable(i)
	return s
}

func (s *StatementImpl) Map(i interface{}) Statement {
	s.mapping, s.Table = MapTable(i)
	return s
}

func (s *StatementImpl) Limit(n int) Statement {
	s.LimitValue = n
	return s
}

func (s *StatementImpl) TTL(seconds int) Statement {
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
