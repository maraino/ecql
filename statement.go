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
	BuildQuery() (string, []interface{})
	Do(cmd Command) Statement
	From(table string) Statement
	FromType(i interface{}) Statement
	Columns(columns ...string) Statement
	Set(column string, value interface{}) Statement
	Where(cond ...Condition) Statement
	OrderBy(order ...OrderBy) Statement
	AllowFiltering() Statement
	IfExists() Statement
	IfNotExists() Statement
	Bind(i interface{}) Statement
	Map(i interface{}) Statement
	Limit(n int) Statement
	TTL(seconds int) Statement
	Timestamp(microseconds int64) Statement
}

type StatementImpl struct {
	session             *SessionImpl
	Command             Command
	Table               Table
	ColumnNames         []string
	Conditions          *Condition
	Orders              []OrderBy
	Assignments         map[string]interface{}
	LimitValue          int
	TTLValue            int
	TimestampValue      int64
	AllowFilteringValue bool
	IfExistsValue       bool
	IfNotExistsValue    bool
	mapping             map[string]interface{}
	values              []interface{}
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

// Exec builds the query statement and executes it returning nil or the gocql
// error. On DELETE and UPDATE statements, the behavior of Exec differs from
// gocql if IfExists() is used, in this case, ecql will perform a ScanCAS and
// return gocql.ErrNotFound if the query was not applied.
func (s *StatementImpl) Exec() error {
	if query, err := s.query(); err != nil {
		return err
	} else {
		// Perform a ScanCAS and reeturn an error if the update/delete are not successful.
		if s.IfExistsValue && (s.Command == UpdateCmd || s.Command == DeleteCmd) {
			if applied, err := query.ScanCAS(); err != nil {
				return err
			} else if applied == false {
				return gocql.ErrNotFound
			}
			return nil
		}

		return query.Exec()
	}
}

func (s *StatementImpl) Iter() Iter {
	return &IterImpl{
		statement: s,
	}
}

func (s *StatementImpl) query() (*gocql.Query, error) {
	stmt, args := s.BuildQuery()
	return s.session.Query(stmt, args...), nil
}

// BuildQuery returns the statement query and arguments that will be executed.
func (s *StatementImpl) BuildQuery() (string, []interface{}) {
	var cql []string

	// Query with specific column names
	withColumnNames := len(s.ColumnNames) > 0

	switch s.Command {
	case SelectCmd:
		if withColumnNames {
			cql = append(cql, fmt.Sprintf("SELECT %s FROM %s", strings.Join(s.ColumnNames, ", "), s.Table.Name))
		} else {
			cql = append(cql, fmt.Sprintf("SELECT * FROM %s", s.Table.Name))
		}
	case InsertCmd:
		if withColumnNames {
			cql = append(cql, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", s.Table.Name, strings.Join(s.ColumnNames, ", "), qms(len(s.ColumnNames))))
		} else {
			cql = append(cql, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", s.Table.Name, s.Table.getCols(), s.Table.getQms()))
		}
	case DeleteCmd:
		if withColumnNames {
			cql = append(cql, fmt.Sprintf("DELETE %s FROM %s", strings.Join(s.ColumnNames, ", "), s.Table.Name))
		} else {
			cql = append(cql, fmt.Sprintf("DELETE FROM %s", s.Table.Name))
		}
		if s.TimestampValue > 0 {
			cql = append(cql, fmt.Sprintf("USING TIMESTAMP %d", s.TimestampValue))
		}
	case UpdateCmd:
		cql = append(cql, fmt.Sprintf("UPDATE %s", s.Table.Name))
		if s.TTLValue > 0 && s.TimestampValue > 0 {
			cql = append(cql, fmt.Sprintf("USING TTL %d AND TIMESTAMP %d", s.TTLValue, s.TimestampValue))
		} else if s.TTLValue > 0 {
			cql = append(cql, fmt.Sprintf("USING TTL %d", s.TTLValue))
		} else if s.TimestampValue > 0 {
			cql = append(cql, fmt.Sprintf("USING TIMESTAMP %d", s.TimestampValue))
		}
	case CountCmd:
		cql = append(cql, fmt.Sprintf("SELECT COUNT(1) FROM %s", s.Table.Name))
	default:
		// This should not happen
		panic(ErrInvalidCommand)
	}

	var args []interface{}

	// On UPDATE: SET col = ?
	if s.Command == UpdateCmd {
		i := 0
		assignments := make([]string, len(s.Assignments)+len(s.ColumnNames))

		for _, col := range s.ColumnNames {
			assignments[i] = fmt.Sprintf("%s = ?", col)
			args = append(args, s.mapping[col])
			i++
		}
		for col, v := range s.Assignments {
			assignments[i] = fmt.Sprintf("%s = ?", col)
			args = append(args, v)
			i++
		}
		if i > 0 {
			cql = append(cql, "SET", strings.Join(assignments, ", "))
		}
	}

	// WHERE ...
	if s.Conditions != nil {
		cql = append(cql, "WHERE", s.Conditions.CQLFragment)
		args = append(args, s.Conditions.Values...)
	}

	// On SELECT: ORDER BY ... LIMIT n
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

		if s.AllowFilteringValue {
			cql = append(cql, "ALLOW FILTERING")
		}
	}

	// On INSERT: IF NOT EXISTS USING TTL n
	if s.Command == InsertCmd {
		if s.IfNotExistsValue {
			cql = append(cql, "IF NOT EXISTS")
		}

		if s.TTLValue > 0 && s.TimestampValue > 0 {
			cql = append(cql, fmt.Sprintf("USING TTL %d AND TIMESTAMP %d", s.TTLValue, s.TimestampValue))
		} else if s.TTLValue > 0 {
			cql = append(cql, fmt.Sprintf("USING TTL %d", s.TTLValue))
		} else if s.TimestampValue > 0 {
			cql = append(cql, fmt.Sprintf("USING TIMESTAMP %d", s.TimestampValue))
		}

		// Add values
		if len(s.values) > 0 {
			if withColumnNames {
				for _, col := range s.ColumnNames {
					args = append(args, s.mapping[col])
				}
			} else {
				for i := range s.values {
					args = append(args, s.values[i])
				}
			}
		}
	}

	// ON UPDATE/DELETE: ... IF EXISTS
	if s.Command == UpdateCmd || s.Command == DeleteCmd {
		if s.IfExistsValue {
			cql = append(cql, "IF EXISTS")
		}
	}

	return strings.Join(cql, " "), args
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

// Columns define a list of columns to get on SELECT statements, to set on
// UPDATE or INSERT statemets or to remove on DELETE statements.
func (s *StatementImpl) Columns(columns ...string) Statement {
	s.ColumnNames = columns
	return s
}

// Set allows to add a new Set to an UPDATE statement.
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
	s.values, s.mapping, s.Table = BindTable(i)
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

func (s *StatementImpl) Timestamp(microseconds int64) Statement {
	s.TimestampValue = microseconds
	return s
}

func (s *StatementImpl) AllowFiltering() Statement {
	s.AllowFilteringValue = true
	return s
}

func (s *StatementImpl) IfExists() Statement {
	s.IfExistsValue = true
	return s
}

func (s *StatementImpl) IfNotExists() Statement {
	s.IfNotExistsValue = true
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
