# ecql

[![GoDoc](http://godoc.org/github.com/maraino/ecql?status.png)](http://godoc.org/github.com/maraino/ecql)

Package ecql (EasyCQL) implements an easy to use Cassandra client for the Go programing language.

EasyCQL is based on [gocql](https://github.com/gocql/gocql).

The current interface is still experimental and it will change without notice.

## Features
Easy API:
 - [x] Map struct types with Cassandra tables.
 - [x] SELECT statements.
 - [x] INSERT statements.
 - [x] DELETE statements.
 - [ ] UPDATE statements.
 - [x] Compound primary keys.

Statement API:
 - [x] Map struct types with Cassandra tables.
 - [x] SELECT statements.
 - [x] SELECT COUNT(1) statements.
 - [x] INSERT statements.
 - [x] DELETE statements.
 - [x] UPDATE statements.
 - [ ] BATCH statements.
 - [x] Iterators to go through multiple results.
 - [x] WHERE filtering (=, >, >=, <, or <=).
 - [x] WHERE filtering (AND).
 - [x] WHERE filtering (IN).
 - [x] WHERE filtering (Interface mapping of keys).
 - [ ] WHERE filtering (CONTAINS, CONTAINS KEY)
 - [x] LIMIT on SELECT statements.
 - [x] ORDER BY on SELECT statements.
 - [x] ALLOW FILTERING ON SELECT statements.
 - [ ] IF NOT EXISTS on INSERT statements.
 - [ ] IF and IF EXISTS on DELETE statements.
 - [ ] IF and IF EXISTS on UPDATE statements.
 - [x] USING TTL on INSERT statements.
 - [ ] USING TIMESTAMP on INSERT statements.
 - [ ] USING TIMESTAMP on DELETE statements.
 - [x] USING TTL on UPDATE statements.
 - [ ] USING TIMESTAMP on UPDATE statements.
 - [ ] Counters.
 - [ ] Functions.

## Documentation.

### Defining a table.

To be able to bind a table in Cassandra to a Go struct we will need tag the struct fields using the tag `cql`, `cqltable` and `cqlkey`.
The tag `cql` defines the column name, the tag `cqltable` defines the name of the table, and `cqlkey` is a comma separated list of the
primary keys in the right order.

For example, for the CREATE TABLE statement:
```cql
CREATE TABLE tweet (
  id uuid,
  timeline text,
  text text,
  time timestamp,
  PRIMARY KEY (id)
);
```

We can use the following struct in Go:
```go
type Tweet struct {
	ID       gocql.UUID `cql:"id" cqltable:"tweet" cqlkey:"id"`
	Timeline string     `cql:"timeline"`
	Text     string     `cql:"text"`
	Time     time.Time  `cql:"time"`
}

func init() {
	ecql.Register(Tweet{})
}
```

It is recommended to register the struct on init functions, but ecql will register new types if they are not registered.

### Queries.

#### Easy API

##### sess.Get(i interface{}, keys ...interface{}) error

Creates and execute a SELECT statement in the table defined by the argument `i` using the keys as the values of the primary keys.
It stores the result in the first argument, so it must be passed as a reference.

```go
var tw Tweet
err := sess.Get(&tw, "a5450908-17d7-11e6-b9ec-542696d5770f")
```

##### sess.Set(i interface{}) error

Creates and execute a INSERT statement in the table defined by the argument `i` and sets the values in the mapped columns.

```go
tw  := Tweet{
	ID:       gocql.TimeUUID(),
	Timeline: "ecql",
	Text:     "Hello World",
	Time:     time.Now(),
}
err := sess.Set(tw)
```

##### sess.Del(i interface{}) error

Creates a DELETE statement in the table defined by the argument `i` using the filtering by the primary keys defined on `i`.

```go
uuid, _ := gocql.ParseUUID("a5450908-17d7-11e6-b9ec-542696d5770f")
tw := Tweet{
	ID: uuid,
}
err := sess.Del(tw)
```
