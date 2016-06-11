// +build integration

package ecql

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
)

var testSession Session
var cqlVersion []int

type user struct {
	ID        string            `cql:"id" cqltable:"users", cqlkey:"id"`
	Following []string          `cql:"following`
	Details   map[string]string `cql:"details"`
}

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
		"INSERT INTO users (id, following, details) VALUES ('ecql', ['foo','bar'], {'handle':'@ecql','url':'https://github.com/maraino/ecql'})",
		"INSERT INTO tweet (id, timeline, text, time) VALUES (a5450908-17d7-11e6-b9ec-542696d5770f, 'ecql', 'hello world!', '2016-01-01 00:00:00-0000')",
		"INSERT INTO tweet (id, timeline, text, time) VALUES (619f33d2-1952-11e6-9f53-542696d5770f, 'ecql', 'ciao world!', '2016-01-01 11:11:11-0000')",
		"INSERT INTO timeline (id, time, tweet) VALUES ('ecql', '2016-01-01 00:00:00-0000', a5450908-17d7-11e6-b9ec-542696d5770f)",
		"INSERT INTO timeline (id, time, tweet) VALUES ('ecql', '2016-01-01 11:11:11-0000', 619f33d2-1952-11e6-9f53-542696d5770f)",
	} {
		if err := sess.Query(stmt).Exec(); err != nil {
			fmt.Fprintf(os.Stderr, "Error initializing test_ecql: %s\n", err.Error())
			fmt.Fprintf(os.Stderr, "Query: %s\n", stmt)
			t.FailNow()
		}
	}
}

func TestSelect(t *testing.T) {
	initialize(t)

	var u user
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

	// Supported on 3.2.0
	if cqlVersion[0] > 3 || (cqlVersion[0] >= 3 && cqlVersion[1] >= 2) {
		err = testSession.Select(&u).Where(Eq("id", "ecql"), Contains("following", "bar")).TypeScan()
		assert.NoError(t, err)
		assert.Equal(t, "ecql", u.ID)

		err = testSession.Select(&u).Where(Eq("id", "ecql"), Contains("following", "zar")).TypeScan()
		assert.Equal(t, gocql.ErrNotFound, err)

		err = testSession.Select(&u).Where(Eq("id", "ecql"), ContainsKey("details", "handle")).TypeScan()
		assert.NoError(t, err)
		assert.Equal(t, "ecql", u.ID)

		err = testSession.Select(&u).Where(Eq("id", "ecql"), ContainsKey("details", "github")).TypeScan()
		assert.Equal(t, gocql.ErrNotFound, err)
	}
}

func TestSelectWithColumns(t *testing.T) {
	initialize(t)
	var tw tweet
	err := testSession.Select(&tw).Columns("text").Where(Eq("id", "a5450908-17d7-11e6-b9ec-542696d5770f")).TypeScan()
	assert.NoError(t, err)
	assert.Equal(t, "00000000-0000-0000-0000-000000000000", tw.ID.String())
	assert.Equal(t, "", tw.Timeline)
	assert.Equal(t, "hello world!", tw.Text)
	assert.Equal(t, time.Time{}, tw.Time)
}

func TestSelectAllowFiltering(t *testing.T) {
	initialize(t)
	tiTime := time.Date(2016, 1, 1, 0, 0, 0, 0, time.UTC)

	var ti timeline
	err := testSession.Select(&ti).Where(Eq("time", tiTime)).TypeScan()
	assert.Error(t, err)

	err = testSession.Select(&ti).Where(Eq("time", tiTime)).AllowFiltering().TypeScan()
	assert.NoError(t, err)
	assert.Equal(t, "ecql", ti.ID)
	assert.Equal(t, "2016-01-01 00:00:00 +0000 UTC", ti.Time.String())
	assert.Equal(t, "a5450908-17d7-11e6-b9ec-542696d5770f", ti.Tweet.String())
}

func TestInsert(t *testing.T) {
	initialize(t)

	newTW := tweet{
		ID:       gocql.TimeUUID(),
		Timeline: "me",
		Text:     "Here's a new tweet",
		Time:     Now().UTC(),
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

	// With TTL
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

	// With Timestamp
	newTW.ID = gocql.TimeUUID()
	microseconds := Now().Unix() * 1e6
	err = testSession.Insert(newTW).Timestamp(microseconds).Exec()
	assert.NoError(t, err)

	var writetime int64
	query := testSession.Query("SELECT writetime(time) FROM tweet WHERE id = ?", newTW.ID)
	err = query.Scan(&writetime)
	assert.NoError(t, err)
	assert.Equal(t, microseconds, writetime)

	// With TTL + Timestamp
	var uuid gocql.UUID
	newTW.ID = gocql.TimeUUID()
	microseconds = Now().Unix() * 1e6
	err = testSession.Insert(newTW).TTL(2).Timestamp(microseconds).Exec()
	assert.NoError(t, err)

	query = testSession.Query("SELECT id, writetime(time) FROM tweet WHERE id = ?", newTW.ID)
	err = query.Scan(&uuid, &writetime)
	assert.NoError(t, err)
	assert.Equal(t, newTW.ID, uuid)
	assert.Equal(t, microseconds, writetime)

	time.Sleep(2 * time.Second)
	tw = tweet{}
	err = testSession.Select(&tw).Where(Eq("id", newTW.ID)).TypeScan()
	assert.Equal(t, gocql.ErrNotFound, err)
}

func TestInsertColumns(t *testing.T) {
	initialize(t)

	newTW := tweet{
		ID:       gocql.TimeUUID(),
		Timeline: "me",
		Text:     "Here's a new tweet",
		Time:     Now().UTC(),
	}

	err := testSession.Insert(newTW).Columns("id").Exec()
	assert.NoError(t, err)

	var tw tweet
	testSession.Get(&tw, newTW.ID)
	assert.Equal(t, newTW.ID, tw.ID)
	assert.Equal(t, "", tw.Timeline)
	assert.Equal(t, "", tw.Text)
	assert.Equal(t, time.Time{}, tw.Time)

	err = testSession.Insert(newTW).Columns("id", "timeline", "text").Exec()
	assert.NoError(t, err)

	testSession.Get(&tw, newTW.ID)
	assert.Equal(t, newTW.ID, tw.ID)
	assert.Equal(t, newTW.Timeline, tw.Timeline)
	assert.Equal(t, newTW.Text, tw.Text)
	assert.Equal(t, time.Time{}, tw.Time)
}

func TestInsertIfNotExists(t *testing.T) {
	initialize(t)

	newTW := tweet{
		ID:       gocql.TimeUUID(),
		Timeline: "me",
		Text:     "Here's a new tweet",
		Time:     Now().UTC(),
	}

	err := testSession.Insert(newTW).IfNotExists().Exec()
	assert.NoError(t, err)

	tw := tweet{}
	err = testSession.Get(&tw, newTW.ID)
	assert.NoError(t, err)
	assert.Equal(t, newTW, tw)

	err = testSession.Insert(newTW).IfNotExists().Exec()
	assert.NoError(t, err)
}

func TestDelete(t *testing.T) {
	initialize(t)

	newTW := tweet{
		ID:       gocql.TimeUUID(),
		Timeline: "me",
		Text:     "Here's a new tweet",
		Time:     Now().UTC(),
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
	newTW.Time = Now().UTC()
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

func TestDeleteAutoBinding(t *testing.T) {
	initialize(t)

	var tw tweet
	err := testSession.Get(&tw, "a5450908-17d7-11e6-b9ec-542696d5770f")
	assert.NoError(t, err)

	err = testSession.Delete(tw).Exec()
	assert.NoError(t, err)

	err = testSession.Get(&tw, "a5450908-17d7-11e6-b9ec-542696d5770f")
	assert.Error(t, gocql.ErrNotFound)
}

func TestDeleteColumns(t *testing.T) {
	initialize(t)

	var tw tweet
	err := testSession.Get(&tw, "a5450908-17d7-11e6-b9ec-542696d5770f")
	assert.NoError(t, err)

	err = testSession.Delete(tw).Columns("text", "time").Exec()
	assert.NoError(t, err)

	tw = tweet{}
	err = testSession.Get(&tw, "a5450908-17d7-11e6-b9ec-542696d5770f")
	assert.NoError(t, err)
	assert.Equal(t, "a5450908-17d7-11e6-b9ec-542696d5770f", tw.ID.String())
	assert.Equal(t, "ecql", tw.Timeline)
	assert.Equal(t, "", tw.Text)
	assert.Equal(t, time.Time{}, tw.Time)
}

func TestDeleteIfExists(t *testing.T) {
	initialize(t)

	tw := tweet{
		ID: gocql.TimeUUID(),
	}

	err := testSession.Delete(tw).IfExists().Exec()
	assert.Equal(t, gocql.ErrNotFound, err)

	tw.ID = MustUUID("a5450908-17d7-11e6-b9ec-542696d5770f")
	err = testSession.Delete(tw).IfExists().Exec()
	assert.NoError(t, err)

	err = testSession.Get(&tw, "a5450908-17d7-11e6-b9ec-542696d5770f")
	assert.Error(t, gocql.ErrNotFound)
}

func TestUpdate(t *testing.T) {
	initialize(t)

	var tw tweet
	err := testSession.Get(&tw, "619f33d2-1952-11e6-9f53-542696d5770f")
	assert.NoError(t, err)

	// With column names and automatic binding
	tw.Text = "updated tweet"
	err = testSession.Update(tw).Columns("text").Exec()
	assert.NoError(t, err)

	err = testSession.Get(&tw, "619f33d2-1952-11e6-9f53-542696d5770f")
	assert.NoError(t, err)
	assert.Equal(t, "619f33d2-1952-11e6-9f53-542696d5770f", tw.ID.String())
	assert.Equal(t, "ecql", tw.Timeline)
	assert.Equal(t, "updated tweet", tw.Text)
	assert.Equal(t, "2016-01-01 11:11:11 +0000 UTC", tw.Time.String())

	now := time.Now()
	tw.Text = "foobar tweet"
	tw.Timeline = "foobar"
	tw.Time = now
	err = testSession.Update(tw).Columns("text", "timeline", "time").Exec()
	assert.NoError(t, err)

	err = testSession.Get(&tw, "619f33d2-1952-11e6-9f53-542696d5770f")
	assert.NoError(t, err)
	assert.Equal(t, "619f33d2-1952-11e6-9f53-542696d5770f", tw.ID.String())
	assert.Equal(t, "foobar", tw.Timeline)
	assert.Equal(t, "foobar tweet", tw.Text)
	assert.Equal(t, now.Unix(), tw.Time.Unix())

	tw.Text = "tweet with ttl"
	err = testSession.Update(tw).Columns("text").TTL(2).Exec()
	assert.NoError(t, err)

	err = testSession.Get(&tw, "619f33d2-1952-11e6-9f53-542696d5770f")
	assert.NoError(t, err)
	assert.Equal(t, "619f33d2-1952-11e6-9f53-542696d5770f", tw.ID.String())
	assert.Equal(t, "foobar", tw.Timeline)
	assert.Equal(t, "tweet with ttl", tw.Text)
	assert.Equal(t, now.Unix(), tw.Time.Unix())

	time.Sleep(2 * time.Second)
	err = testSession.Get(&tw, "619f33d2-1952-11e6-9f53-542696d5770f")
	assert.NoError(t, err)
	assert.Equal(t, "619f33d2-1952-11e6-9f53-542696d5770f", tw.ID.String())
	assert.Equal(t, "foobar", tw.Timeline)
	assert.Equal(t, "", tw.Text)
	assert.Equal(t, now.Unix(), tw.Time.Unix())
}

func TestUpdateSet(t *testing.T) {
	initialize(t)

	var tw tweet
	err := testSession.Get(&tw, "619f33d2-1952-11e6-9f53-542696d5770f")
	assert.NoError(t, err)

	err = testSession.Update(tw).Set("text", "updated tweet").Exec()
	assert.NoError(t, err)

	err = testSession.Get(&tw, "619f33d2-1952-11e6-9f53-542696d5770f")
	assert.NoError(t, err)
	assert.Equal(t, "619f33d2-1952-11e6-9f53-542696d5770f", tw.ID.String())
	assert.Equal(t, "ecql", tw.Timeline)
	assert.Equal(t, "updated tweet", tw.Text)
	assert.Equal(t, "2016-01-01 11:11:11 +0000 UTC", tw.Time.String())

	// Avoid errors with some cassandra versions
	time.Sleep(2 * time.Second)

	now := time.Now()
	err = testSession.Update(tw).Set("text", "foobar tweet").Set("timeline", "foobar").Set("time", now).Exec()
	assert.NoError(t, err)

	err = testSession.Get(&tw, "619f33d2-1952-11e6-9f53-542696d5770f")
	assert.NoError(t, err)
	assert.Equal(t, "619f33d2-1952-11e6-9f53-542696d5770f", tw.ID.String())
	assert.Equal(t, "foobar", tw.Timeline)
	assert.Equal(t, "foobar tweet", tw.Text)
	assert.Equal(t, now.Unix(), tw.Time.Unix())
}

func TestUpdateSetWhere(t *testing.T) {
	initialize(t)

	var tw tweet
	err := testSession.Get(&tw, "619f33d2-1952-11e6-9f53-542696d5770f")
	assert.NoError(t, err)

	err = testSession.Update(tw).Set("text", "updated tweet").Where(Eq("id", "a5450908-17d7-11e6-b9ec-542696d5770f")).Exec()
	assert.NoError(t, err)

	err = testSession.Get(&tw, "a5450908-17d7-11e6-b9ec-542696d5770f")
	assert.NoError(t, err)
	assert.Equal(t, "a5450908-17d7-11e6-b9ec-542696d5770f", tw.ID.String())
	assert.Equal(t, "ecql", tw.Timeline)
	assert.Equal(t, "updated tweet", tw.Text)
	assert.Equal(t, "2016-01-01 00:00:00 +0000 UTC", tw.Time.String())

	// Avoid errors with some cassandra versions
	time.Sleep(2 * time.Second)

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

func TestUpdateIfExists(t *testing.T) {
	initialize(t)

	tw := tweet{
		ID: gocql.TimeUUID(),
	}

	err := testSession.Update(tw).Set("text", "foobar tweet").IfExists().Exec()
	assert.Equal(t, gocql.ErrNotFound, err)

	tw.ID = MustUUID("a5450908-17d7-11e6-b9ec-542696d5770f")
	err = testSession.Update(tw).Set("text", "foobar tweet").IfExists().Exec()
	assert.NoError(t, err)

	err = testSession.Get(&tw, "a5450908-17d7-11e6-b9ec-542696d5770f")
	assert.NoError(t, err)
	assert.Equal(t, "a5450908-17d7-11e6-b9ec-542696d5770f", tw.ID.String())
	assert.Equal(t, "foobar tweet", tw.Text)
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

func TestBatch(t *testing.T) {
	initialize(t)

	tw1 := tweet{
		ID:       gocql.TimeUUID(),
		Timeline: "me",
		Text:     "First tweet",
		Time:     Now().UTC(),
	}

	tw2 := tweet{
		ID:       gocql.TimeUUID(),
		Timeline: "me",
		Text:     "Second tweet",
		Time:     Now().UTC(),
	}

	// Apply: ok
	stmt1 := testSession.Insert(tw1)
	stmt2 := testSession.Insert(tw2)
	batch := testSession.Batch().Add(stmt1, stmt2)

	err := batch.Apply()
	assert.NoError(t, err)

	tw := tweet{}
	err = testSession.Get(&tw, tw1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tw1, tw)

	err = testSession.Get(&tw, tw2.ID)
	assert.NoError(t, err)
	assert.Equal(t, tw2, tw)

	// ApplyCAS: ok
	now := Now().UTC()
	tw1.ID = gocql.TimeUUID()
	stmt1 = testSession.Insert(tw1).IfNotExists()
	stmt2 = testSession.Update(tw1).Set("time", now)
	batch = testSession.Batch().Add(stmt1, stmt2)

	applied, err := batch.ApplyCAS()
	assert.True(t, applied)
	assert.NoError(t, err)

	tw1.Time = now
	// Avoid errors with some cassandra versions
	time.Sleep(2 * time.Second)

	tw = tweet{}
	err = testSession.Get(&tw, tw1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tw1, tw)

	// ApplyCAS: not applied
	now = Now().UTC()
	stmt1 = testSession.Insert(tw1).IfNotExists()
	stmt2 = testSession.Update(tw1).Set("time", now)
	batch = testSession.Batch().Add(stmt1, stmt2)

	applied, err = batch.ApplyCAS()
	assert.False(t, applied)
	assert.NoError(t, err)
}

func TestMain(m *testing.M) {
	flag.Parse()

	var err error
	cluster := gocql.NewCluster("localhost")
	sess, err := cluster.CreateSession()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting cassandra: %s\n", err.Error())
		os.Exit(1)
	}

	// Get CQL version
	var version string
	if err := sess.Query("SELECT cql_version FROM system.local").Scan(&version); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading cql_version: %s\n", err.Error())
		os.Exit(1)
	}

	versionParts := strings.Split(version, ".")
	cqlVersion = make([]int, len(versionParts))
	for i := range cqlVersion {
		cqlVersion[i], _ = strconv.Atoi(versionParts[i])
	}

	// Remove test keyspace
	cleanup := func() {
		sess.Query("DROP KEYSPACE test_ecql").Exec()
		sess.Close()
	}

	// Initialize test keyspace
	if err := sess.Query("CREATE KEYSPACE test_ecql WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }").Exec(); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating test_ecql: %s\n", err.Error())
		cleanup()
		os.Exit(1)
	}

	cluster.Keyspace = "test_ecql"
	sess2, err := cluster.CreateSession()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting cassandra: %s\n", err.Error())
		cleanup()
		os.Exit(1)
	}

	// Create test tables
	stmts := []string{
		"CREATE TABLE users (id text PRIMARY KEY, following list<text>, details map<text,text>)",
		"CREATE TABLE tweet (id uuid PRIMARY KEY, timeline text, text text, time timestamp)",
		"CREATE TABLE timeline (id text, time timestamp, tweet uuid, PRIMARY KEY(id, time))",
	}
	// Supported on 3.2.0
	if cqlVersion[0] > 3 || (cqlVersion[0] >= 3 && cqlVersion[1] >= 2) {
		stmts = append(stmts, "CREATE INDEX ON users (following)")
		stmts = append(stmts, "CREATE INDEX ON users (keys(details))")
	}
	for _, stmt := range stmts {
		if err := sess2.Query(stmt).Exec(); err != nil {
			fmt.Fprintf(os.Stderr, "Error initializing test_ecql: %s\n", err.Error())
			fmt.Fprintf(os.Stderr, "Query: %s\n", stmt)
			cleanup()
			os.Exit(1)
		}
	}
	sess2.Close()

	// Initialize ecql.Session
	cluster.Keyspace = "test_ecql"
	testSession, err = NewSession(*cluster)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting cassandra: %s\n", err.Error())
		cleanup()
		os.Exit(1)
	}

	// Run tests
	result := m.Run()
	cleanup()
	os.Exit(result)
}
