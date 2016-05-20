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

var testSession Session

type tweet struct {
	ID       gocql.UUID `cql:"id" cqltable:"tweet" cqlkey:"id"`
	Timeline string     `cql:"timeline"`
	Text     string     `cql:"text"`
	Time     time.Time  `cql:"time"`
}

type timeline struct {
	ID    string     `cql:"id" cqltable:"timeline" cqlkey:"id,time"`
	Time  time.Time  `cql:"time"`
	Tweet gocql.UUID `cql:"tweet"`
}

func initialize(t *testing.T) {
	sess := testSession.(*SessionImpl).Session
	for _, stmt := range []string{
		"TRUNCATE tweet",
		"INSERT INTO tweet (id, timeline, text, time) VALUES (a5450908-17d7-11e6-b9ec-542696d5770f, 'ecql', 'hello world!', '2016-01-01 00:00:00-0000')",
		"INSERT INTO tweet (id, timeline, text, time) VALUES (619f33d2-1952-11e6-9f53-542696d5770f, 'ecql', 'ciao world!', '2016-01-01 11:11:11-0000')",
		"INSERT INTO timeline (id, time, tweet) VALUES ('ecql', '2016-01-01 00:00:00-0000', a5450908-17d7-11e6-b9ec-542696d5770f)",
		"INSERT INTO timeline (id, time, tweet) VALUES ('ecql', '2016-01-01 11:11:11-0000', 619f33d2-1952-11e6-9f53-542696d5770f)",
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
	var tl timeline
	err := testSession.Get(&tw, "a5450908-17d7-11e6-b9ec-542696d5770f")
	assert.NoError(t, err)
	assert.Equal(t, "a5450908-17d7-11e6-b9ec-542696d5770f", tw.ID.String())
	assert.Equal(t, "ecql", tw.Timeline)
	assert.Equal(t, "hello world!", tw.Text)

	err = testSession.Get(&tl, "ecql", time.Date(2016, 1, 1, 0, 0, 0, 0, time.UTC))
	assert.NoError(t, err)
	assert.Equal(t, "ecql", tl.ID)
	assert.Equal(t, "2016-01-01 00:00:00 +0000 UTC", tw.Time.String())
	assert.Equal(t, "a5450908-17d7-11e6-b9ec-542696d5770f", tl.Tweet.String())

	err = testSession.Select(&tw).Where(Eq("id", "a5450908-17d7-11e6-b9ec-542696d5770f")).TypeScan()
	assert.NoError(t, err)
	assert.Equal(t, "a5450908-17d7-11e6-b9ec-542696d5770f", tw.ID.String())
	assert.Equal(t, "ecql", tw.Timeline)
	assert.Equal(t, "hello world!", tw.Text)

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

	newTL := timeline{
		ID:    "me",
		Time:  newTW.Time,
		Tweet: newTW.ID,
	}

	// With Set/Del
	err := testSession.Set(newTW)
	assert.NoError(t, err)

	err = testSession.Set(newTL)
	assert.NoError(t, err)

	var tw tweet
	var tl timeline
	err = testSession.Get(&tw, newTW.ID)
	assert.NoError(t, err)
	assert.Equal(t, newTW, tw)

	err = testSession.Get(&tl, newTL.ID, newTL.Time)
	assert.NoError(t, err)
	assert.Equal(t, newTL, tl)

	err = testSession.Del(tw)
	assert.NoError(t, err)
	assert.Equal(t, newTW, tw)

	err = testSession.Del(tl)
	assert.NoError(t, err)
	assert.Equal(t, newTL, tl)

	var tww tweet
	var tll timeline
	err = testSession.Get(&tww, newTW.ID)
	assert.Error(t, gocql.ErrNotFound)
	assert.Zero(t, tww)

	err = testSession.Get(&tll, newTL.ID, newTL.Time)
	assert.Error(t, gocql.ErrNotFound)
	assert.Zero(t, tll)

	// With Insert/Delete
	tw = tweet{}
	tww = tweet{}
	tll = timeline{}
	newTW.ID = gocql.TimeUUID()
	newTW.Time = time.Now().Round(time.Millisecond).UTC()
	newTL.Tweet = newTW.ID
	newTL.Time = newTW.Time

	err = testSession.Insert(newTW).Exec()
	assert.NoError(t, err)

	err = testSession.Insert(newTL).Exec()
	assert.NoError(t, err)

	err = testSession.Select(&tw).Where(Eq("id", newTW.ID)).TypeScan()
	assert.NoError(t, err)
	assert.Equal(t, newTW, tw)

	err = testSession.Select(&tl).Where(Eq("id", newTL.ID), Eq("time", newTL.Time)).TypeScan()
	assert.NoError(t, err)
	assert.Equal(t, newTL, tl)

	err = testSession.Delete(tw).Where(EqInt(newTW)).Exec()
	assert.NoError(t, err)
	assert.Equal(t, newTW, tw)

	err = testSession.Delete(tl).Where(Eq("id", newTL.ID), Eq("time", newTL.Time)).Exec()
	assert.NoError(t, err)
	assert.Equal(t, newTL, tl)

	err = testSession.Select(&tww).Where(Eq("id", newTW.ID)).TypeScan()
	assert.Error(t, gocql.ErrNotFound)
	assert.Zero(t, tww)

	err = testSession.Select(&tll).Where(Eq("id", newTL.ID), Eq("time", newTL.Time)).TypeScan()
	assert.Error(t, gocql.ErrNotFound)
	assert.Zero(t, tll)
}

func TestUpdate(t *testing.T) {
	initialize(t)

	var tw tweet
	err := testSession.Get(&tw, "a5450908-17d7-11e6-b9ec-542696d5770f")
	assert.NoError(t, err)

	err = testSession.Update(tw).Set("text", "updated tweet").Where(Eq("id", tw.ID)).Exec()
	assert.NoError(t, err)

	err = testSession.Get(&tw, "a5450908-17d7-11e6-b9ec-542696d5770f")
	assert.NoError(t, err)
	assert.Equal(t, "a5450908-17d7-11e6-b9ec-542696d5770f", tw.ID.String())
	assert.Equal(t, "ecql", tw.Timeline)
	assert.Equal(t, "updated tweet", tw.Text)
	assert.Equal(t, "2016-01-01 00:00:00 +0000 UTC", tw.Time.String())

	now := time.Now()
	err = testSession.Update(tw).Set("text", "foobar tweet").Set("timeline", "foobar").Set("time", now).Where(Eq("id", tw.ID)).Exec()
	assert.NoError(t, err)

	err = testSession.Get(&tw, "a5450908-17d7-11e6-b9ec-542696d5770f")
	assert.NoError(t, err)
	assert.Equal(t, "a5450908-17d7-11e6-b9ec-542696d5770f", tw.ID.String())
	assert.Equal(t, "foobar", tw.Timeline)
	assert.Equal(t, "foobar tweet", tw.Text)
	assert.Equal(t, now.Unix(), tw.Time.Unix())

	err = testSession.Update(tw).TTL(2).Set("text", "tweet with ttl").Where(Eq("id", tw.ID)).Exec()
	assert.NoError(t, err)

	err = testSession.Get(&tw, "a5450908-17d7-11e6-b9ec-542696d5770f")
	assert.NoError(t, err)
	assert.Equal(t, "a5450908-17d7-11e6-b9ec-542696d5770f", tw.ID.String())
	assert.Equal(t, "foobar", tw.Timeline)
	assert.Equal(t, "tweet with ttl", tw.Text)
	assert.Equal(t, now.Unix(), tw.Time.Unix())

	time.Sleep(2 * time.Second)
	err = testSession.Get(&tw, "a5450908-17d7-11e6-b9ec-542696d5770f")
	assert.NoError(t, err)
	assert.Equal(t, "a5450908-17d7-11e6-b9ec-542696d5770f", tw.ID.String())
	assert.Equal(t, "foobar", tw.Timeline)
	assert.Equal(t, "", tw.Text)
	assert.Equal(t, now.Unix(), tw.Time.Unix())
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
