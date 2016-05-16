// +build integration

package ecql

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
)

var testSession *Session

type tweet struct {
	ID       gocql.UUID `cql:"id" cqltable:"tweet" cqlkey:"id"`
	Timeline string     `cql:"timeline"`
	Text     string     `cql:"text"`
	Time     time.Time  `cql:"time"`
}

type timeline struct {
	ID    string     `cql:"id" cqltable:"timeline" cqlkey:"id"`
	Time  time.Time  `cql:"time"`
	Tweet gocql.UUID `cql:"tweet"`
}

func initialize(t *testing.T) {
	sess := testSession.Session
	for _, stmt := range []string{
		"TRUNCATE tweet",
		"INSERT INTO tweet (id, timeline, text, time) VALUES (a5450908-17d7-11e6-b9ec-542696d5770f, 'ecql', 'hello world!', '2016-01-01 00:00:00')",
		"INSERT INTO tweet (id, timeline, text, time) VALUES (619f33d2-1952-11e6-9f53-542696d5770f, 'ecql', 'ciao world!', '2016-01-01 11:11:11')",
		"INSERT INTO timeline (id, time, tweet) VALUES ('ecql', '2016-01-01 00:00:00', a5450908-17d7-11e6-b9ec-542696d5770f)",
		"INSERT INTO timeline (id, time, tweet) VALUES ('ecql', '2016-01-01 11:11:11', 619f33d2-1952-11e6-9f53-542696d5770f)",
	} {
		if err := sess.Query(stmt).Exec(); err != nil {
			fmt.Fprintf(os.Stderr, "Error initializing test_ecql: %s", err.Error())
			fmt.Fprintf(os.Stderr, "Query: %s", stmt)
			t.FailNow()
		}
	}
}

func TestSelect(t *testing.T) {
	initialize(t)

	var tw tweet
	err := testSession.Get(&tw, "a5450908-17d7-11e6-b9ec-542696d5770f")
	assert.NoError(t, err)
	assert.Equal(t, "a5450908-17d7-11e6-b9ec-542696d5770f", tw.ID.String())
	assert.Equal(t, "ecql", tw.Timeline)
	assert.Equal(t, "hello world!", tw.Text)

	tw = tweet{}
	err = testSession.Select(&tw).Where(Eq("id", "a5450908-17d7-11e6-b9ec-542696d5770f")).TypeScan()
	assert.NoError(t, err)
	assert.Equal(t, "a5450908-17d7-11e6-b9ec-542696d5770f", tw.ID.String())
	assert.Equal(t, "ecql", tw.Timeline)
	assert.Equal(t, "hello world!", tw.Text)

	var tl timeline
	i := 0
	iter := testSession.Select(&tl).Where(Eq("id", "ecql")).OrderBy(Asc("time")).Iter()
	for iter.TypeScan(&tl) {
		assert.Equal(t, "ecql", tl.ID)
		switch i {
		case 0:
			assert.Equal(t, "a5450908-17d7-11e6-b9ec-542696d5770f", tl.Tweet.String())
		case 1:
			assert.Equal(t, "619f33d2-1952-11e6-9f53-542696d5770f", tl.Tweet.String())
		}
		i++
	}
	assert.NoError(t, iter.Close())

	i = 0
	iter = testSession.Select(&tl).Where(Eq("id", "ecql")).OrderBy(Desc("time")).Iter()
	for iter.TypeScan(&tl) {
		assert.Equal(t, "ecql", tl.ID)
		switch i {
		case 0:
			assert.Equal(t, "619f33d2-1952-11e6-9f53-542696d5770f", tl.Tweet.String())
		case 1:
			assert.Equal(t, "a5450908-17d7-11e6-b9ec-542696d5770f", tl.Tweet.String())
		}
		i++
	}
	assert.NoError(t, iter.Close())

	iter = testSession.Select(&tl).Where(Eq("id", "ecql")).OrderBy(Asc("time")).Limit(1).Iter()
	assert.True(t, iter.TypeScan(&tl))
	assert.Equal(t, "a5450908-17d7-11e6-b9ec-542696d5770f", tl.Tweet.String())
	assert.False(t, iter.TypeScan(&tl))
	assert.NoError(t, iter.Close())

	iter = testSession.Select(&tl).Where(Eq("id", "ecql")).OrderBy(Desc("time")).Limit(1).Iter()
	assert.True(t, iter.TypeScan(&tl))
	assert.Equal(t, "619f33d2-1952-11e6-9f53-542696d5770f", tl.Tweet.String())
	assert.False(t, iter.TypeScan(&tl))
	assert.NoError(t, iter.Close())

	err = testSession.Select(&tl).Where(Eq("id", "ecql")).OrderBy(Asc("time")).Limit(1).TypeScan()
	assert.NoError(t, err)
	assert.Equal(t, "ecql", tl.ID)
	assert.Equal(t, "a5450908-17d7-11e6-b9ec-542696d5770f", tl.Tweet.String())

	err = testSession.Select(&tl).Where(Eq("id", "ecql")).OrderBy(Desc("time")).Limit(1).TypeScan()
	assert.NoError(t, err)
	assert.Equal(t, "ecql", tl.ID)
	assert.Equal(t, "619f33d2-1952-11e6-9f53-542696d5770f", tl.Tweet.String())
}

func TestInsert(t *testing.T) {
	initialize(t)

	newTW := tweet{
		ID:       gocql.TimeUUID(),
		Timeline: "me",
		Text:     "Here's a new tweet",
		Time:     time.Now().Round(time.Millisecond).UTC(),
	}

	err := testSession.Set(newTW)
	assert.NoError(t, err)

	var tw tweet
	err = testSession.Get(&tw, newTW.ID)
	assert.NoError(t, err)
	assert.Equal(t, newTW, tw)

	newTW.ID = gocql.TimeUUID()
	err = testSession.Insert(newTW).Exec()
	assert.NoError(t, err)

	tw = tweet{}
	testSession.Select(&tw).Where(Eq("id", newTW.ID)).TypeScan()
	assert.Equal(t, newTW, tw)

	newTW.ID = gocql.TimeUUID()
	err = testSession.Insert(newTW).TTL(2).Exec()
	assert.NoError(t, err)

	tw = tweet{}
	err = testSession.Select(&tw).Where(Eq("id", newTW.ID)).TypeScan()
	assert.NoError(t, err)
	assert.Equal(t, newTW, tw)

	time.Sleep(2 * time.Second)
	tw = tweet{}
	err = testSession.Select(&tw).Where(Eq("id", newTW.ID)).TypeScan()
	assert.Equal(t, gocql.ErrNotFound, err)
}

func TestDelete(t *testing.T) {
	initialize(t)

	newTW := tweet{
		ID:       gocql.TimeUUID(),
		Timeline: "me",
		Text:     "Here's a new tweet",
		Time:     time.Now().Round(time.Millisecond).UTC(),
	}

	// With Set/Del
	err := testSession.Set(newTW)
	assert.NoError(t, err)

	var tw tweet
	err = testSession.Get(&tw, newTW.ID)
	assert.NoError(t, err)
	assert.Equal(t, newTW, tw)

	err = testSession.Del(tw)
	assert.NoError(t, err)
	assert.Equal(t, newTW, tw)

	var tww tweet
	err = testSession.Get(&tww, newTW.ID)
	assert.Error(t, gocql.ErrNotFound)
	assert.Zero(t, tww)

	// With Insert/Delete
	tw = tweet{}
	tww = tweet{}
	err = testSession.Insert(newTW).Exec()
	assert.NoError(t, err)

	err = testSession.Select(&tw).Where(Eq("id", newTW.ID)).TypeScan()
	assert.NoError(t, err)
	assert.Equal(t, newTW, tw)

	err = testSession.Delete(tw).Where(Eq("id", newTW.ID)).Exec()
	assert.NoError(t, err)
	assert.Equal(t, newTW, tw)

	err = testSession.Select(&tww).Where(Eq("id", newTW.ID)).TypeScan()
	assert.Error(t, gocql.ErrNotFound)
	assert.Zero(t, tww)
}

func TestCount(t *testing.T) {
	initialize(t)

	var count int
	err := testSession.Count(tweet{}).Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)

	err = testSession.Count(&tweet{}).Where(Eq("id", "a5450908-17d7-11e6-b9ec-542696d5770f")).Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestMain(m *testing.M) {
	flag.Parse()

	var err error
	cluster := gocql.NewCluster("localhost")
	sess, err := cluster.CreateSession()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting cassandra: %s", err.Error())
		os.Exit(1)
	}

	// Remove test keyspace
	cleanup := func() {
		if err := sess.Query("DROP KEYSPACE test_ecql").Exec(); err != nil {
			fmt.Fprintf(os.Stderr, "Error dropping test_ecql: %s", err.Error())
			os.Exit(1)
		}
		sess.Close()
	}

	// Initialize test keyspace
	if err := sess.Query("CREATE KEYSPACE test_ecql WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }").Exec(); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating test_ecql: %s", err.Error())
		cleanup()
		os.Exit(1)
	}

	cluster.Keyspace = "test_ecql"
	sess2, err := cluster.CreateSession()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting cassandra: %s", err.Error())
		cleanup()
		os.Exit(1)
	}

	// Create test tables
	for _, stmt := range []string{
		"CREATE TABLE tweet (id uuid PRIMARY KEY, timeline text, text text, time timestamp)",
		"CREATE TABLE timeline (id text, time timestamp, tweet uuid, PRIMARY KEY(id, time))",
	} {
		if err := sess2.Query(stmt).Exec(); err != nil {
			fmt.Fprintf(os.Stderr, "Error initializing test_ecql: %s", err.Error())
			fmt.Fprintf(os.Stderr, "Query: %s", stmt)
			cleanup()
			os.Exit(1)
		}
	}
	sess2.Close()

	// Initialize ecql.Session
	cluster.Keyspace = "test_ecql"
	testSession, err = NewSession(*cluster)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting cassandra: %s", err.Error())
		cleanup()
		os.Exit(1)
	}

	// Run tests
	result := m.Run()
	cleanup()
	os.Exit(result)
}
