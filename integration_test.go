// +build integration

package ecql

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/gocql/gocql"
)

var testSession *Session

type tweet struct {
	ID       gocql.UUID `cql:"id" cqltable:"tweet" cqlkey:"id"`
	Timeline string     `cql:"timeline"`
	Text     string     `cql:"text"`
}

func TestSelect(t *testing.T) {
	var tw tweet
	testSession.Select(&tw, "a5450908-17d7-11e6-b9ec-542696d5770f")
	assert.Equal(t, "a5450908-17d7-11e6-b9ec-542696d5770f", tw.ID.String())
	assert.Equal(t, "ecql", tw.Timeline)
	assert.Equal(t, "hello world!", tw.Text)
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
