// +build integration

package ecql

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
)

var testSession *Session

type tweet struct {
	ID       gocql.UUID `cql:"id" cqltable:"tweet" cqlkey:"id"`
	Timeline string     `cql:"timeline"`
	Text     string     `cql:"text"`
}

func TestSelect(t *testing.T) {
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
}

func TestInsert(t *testing.T) {
	newTW := tweet{
		ID:       gocql.TimeUUID(),
		Timeline: "me",
		Text:     "Here's a new tweet",
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
}

func TestDelete(t *testing.T) {
	newTW := tweet{
		ID:       gocql.TimeUUID(),
		Timeline: "me",
		Text:     "Here's a new tweet",
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

func TestStatement(t *testing.T) {
	var tw tweet
	err := testSession.Select(&tw).Where(Eq("id", "a5450908-17d7-11e6-b9ec-542696d5770f")).TypeScan()
	assert.NoError(t, err)
	assert.Equal(t, "a5450908-17d7-11e6-b9ec-542696d5770f", tw.ID.String())
	assert.Equal(t, "ecql", tw.Timeline)
	assert.Equal(t, "hello world!", tw.Text)

	var count int
	err = testSession.Count(&tw).Where(Eq("id", "a5450908-17d7-11e6-b9ec-542696d5770f")).Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	err = testSession.Count(&tw).Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 3, count)
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
		"CREATE TABLE tweet (id uuid PRIMARY KEY, timeline text, text text)",
		"INSERT INTO tweet (id, timeline, text) VALUES (a5450908-17d7-11e6-b9ec-542696d5770f, 'ecql', 'hello world!')",
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
