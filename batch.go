package ecql

import "github.com/gocql/gocql"

type Batch interface {
	Add(s ...Statement) Batch
	Apply() error
	ApplyCAS() (bool, error)
}

type BatchImpl struct {
	session    *SessionImpl
	batch      *gocql.Batch
	statements []Statement
}

func NewBatch(sess *SessionImpl, typ gocql.BatchType) Batch {
	return &BatchImpl{
		session: sess,
		batch:   gocql.NewBatch(typ),
	}
}

func (b *BatchImpl) Add(s ...Statement) Batch {
	b.statements = append(b.statements, s...)
	for i := range s {
		stmt, args := s[i].BuildQuery()
		b.batch.Query(stmt, args...)
	}
	return b
}

func (b *BatchImpl) Apply() error {
	return b.session.ExecuteBatch(b.batch)
}

func (b *BatchImpl) ApplyCAS() (bool, error) {
	mapping := make(map[string]interface{})
	applied, iter, err := b.session.MapExecuteBatchCAS(b.batch, mapping)
	if iter != nil {
		iter.Close()
	}
	return applied, err
}
