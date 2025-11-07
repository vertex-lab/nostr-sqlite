package sqlite

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/nbd-wtf/go-nostr"
)

func TestConcurrency(t *testing.T) {
	size := 10_000
	ctx := context.Background()
	path := t.TempDir() + "/test.sqlite"
	store, err := New(path)
	if err != nil {
		t.Fatal(err)
	}

	wg := &sync.WaitGroup{}
	done := make(chan struct{})
	errC := make(chan error, size)

	go func() {
		wg.Add(size)
		for i := range size {
			go func(i int) {
				defer wg.Done()
				event := nostr.Event{Kind: i}
				err := store.Save(ctx, &event)
				if err != nil {
					errC <- err
				}
			}(i)
		}

		wg.Wait()
		close(done)
	}()

	select {
	case err := <-errC:
		t.Fatalf("test failed: %v", err)

	case <-done:
		// test passed
	}
}

func TestSave(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/test.sqlite"
	store, err := New(path)
	if err != nil {
		t.Fatal(err)
	}

	event := nostr.Event{
		Kind:    30000,
		Content: "test",
		Tags:    nostr.Tags{{"d", "test-tag"}},
	}

	if err := store.Save(ctx, &event); err != nil {
		t.Fatal(err)
	}

	res, err := store.Query(ctx, nostr.Filter{Tags: nostr.TagMap{"d": []string{"test-tag"}}, Limit: 1})
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if len(res) != 1 {
		t.Fatalf("expected one event, got %v", res)
	}

	if !reflect.DeepEqual(res[0], event) {
		t.Errorf("the event is not what it was before!")
		t.Fatalf(" expected %v\n got %v", event, res[0])
	}
}

func TestReplace(t *testing.T) {
	event10 := nostr.Event{ID: "bbb", Kind: 0, PubKey: "key", CreatedAt: 10}
	event100 := nostr.Event{ID: "aaa", Kind: 0, PubKey: "key", CreatedAt: 100}

	tests := []struct {
		name        string
		stored      nostr.Event
		new         nostr.Event
		replacement bool
	}{
		{
			name:        "no replace (event is not newer)",
			stored:      event100,
			new:         event10,
			replacement: false,
		},
		{
			name:        "valid replace (event is newer)",
			stored:      event10,
			new:         event100,
			replacement: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			path := t.TempDir() + "/test.sqlite"
			store, err := New(path)
			if err != nil {
				t.Fatal(err)
			}

			if err := store.Save(ctx, &test.stored); err != nil {
				t.Fatal(err)
			}

			stored, err := store.Replace(ctx, &test.new)
			if err != nil {
				t.Fatal(err)
			}

			if stored != test.replacement {
				t.Fatalf("expected replacement %v, got %v", test.replacement, stored)
			}

			res, err := store.Query(ctx, nostr.Filter{IDs: []string{test.stored.ID, test.new.ID}, Limit: 1})
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}

			if len(res) != 1 {
				t.Errorf("expected one event, got %d", len(res))
			}

			switch stored {
			case true:
				if !reflect.DeepEqual(res[0], test.new) {
					t.Fatalf("new was not saved correctly.\n original %v, got %v", test.new, res[0])
				}

			case false:
				if !reflect.DeepEqual(res[0], test.stored) {
					t.Fatalf("stored has been altered.\n original %v, got %v", test.stored, res[0])
				}
			}
		})
	}
}

func TestDefaultQueryBuilder(t *testing.T) {
	tests := []struct {
		name    string
		filters nostr.Filters
		query   Query
	}{
		{
			name:    "single filter, kind",
			filters: nostr.Filters{{Kinds: []int{0, 1}, Limit: 100}},
			query: Query{
				SQL:  "SELECT e.* FROM events AS e WHERE e.kind IN (?,?) ORDER BY e.created_at DESC, e.id ASC LIMIT ?",
				Args: []any{0, 1, 100},
			},
		},
		{
			name:    "single filter, authors",
			filters: nostr.Filters{{Authors: []string{"aaa", "bbb", "xxx"}, Limit: 11}},
			query: Query{
				SQL:  "SELECT e.* FROM events AS e WHERE e.pubkey IN (?,?,?) ORDER BY e.created_at DESC, e.id ASC LIMIT ?",
				Args: []any{"aaa", "bbb", "xxx", 11},
			},
		},
		{
			name: "single filter, tag",
			filters: nostr.Filters{{
				Limit: 11,
				Tags: nostr.TagMap{
					"e": {"xxx"},
				},
			}},

			query: Query{
				SQL:  "SELECT e.* FROM events AS e JOIN event_tags AS t ON t.event_id = e.id WHERE (t.key = ? AND t.value = ?) GROUP BY e.id ORDER BY e.created_at DESC, e.id ASC LIMIT ?",
				Args: []any{"e", "xxx", 11},
			},
		},
		{
			name: "single filter, tags",
			filters: nostr.Filters{{
				Limit: 11,
				Tags: nostr.TagMap{
					"e": {"xxx", "yyy"},
					"p": {"someone"},
				},
			}},

			query: Query{
				SQL:  "SELECT e.* FROM events AS e JOIN event_tags AS t ON t.event_id = e.id WHERE (t.key = ? AND t.value IN (?,?)) OR (t.key = ? AND t.value = ?) GROUP BY e.id ORDER BY e.created_at DESC, e.id ASC LIMIT ?",
				Args: []any{"e", "xxx", "yyy", "p", "someone", 11},
			},
		},
		{
			name: "multiple filter",
			filters: nostr.Filters{
				{Kinds: []int{0, 1}, Limit: 69},
				{Authors: []string{"aaa", "bbb"}, Limit: 420},
			},
			query: Query{
				SQL:  "SELECT * FROM (SELECT e.* FROM events AS e WHERE e.kind IN (?,?) UNION ALL SELECT e.* FROM events AS e WHERE e.pubkey IN (?,?)) GROUP BY id ORDER BY created_at DESC, id ASC LIMIT ?",
				Args: []any{0, 1, "aaa", "bbb", 69 + 420},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query, err := DefaultQueryBuilder(test.filters...)
			if err != nil {
				t.Fatalf("expected error nil, got %v", err)
			}

			if !reflect.DeepEqual(query[0], test.query) {
				t.Fatalf("expected query %v, got %v", test.query, query[0])
			}
		})
	}
}

func TestDefaultCountBuilder(t *testing.T) {
	tests := []struct {
		name    string
		filters nostr.Filters
		query   Query
	}{
		{
			name:    "single filter, kind",
			filters: nostr.Filters{{Kinds: []int{0}}},
			query: Query{
				SQL:  "SELECT COUNT(e.id) FROM events AS e WHERE e.kind = ?",
				Args: []any{0},
			},
		},
		{
			name:    "single filter, authors",
			filters: nostr.Filters{{Authors: []string{"aaa", "bbb", "xxx"}}},
			query: Query{
				SQL:  "SELECT COUNT(e.id) FROM events AS e WHERE e.pubkey IN (?,?,?)",
				Args: []any{"aaa", "bbb", "xxx"},
			},
		},
		{
			name: "single filter, tags",
			filters: nostr.Filters{{
				Limit: 11,
				Tags: nostr.TagMap{
					"e": {"xxx", "yyy"},
					"p": {"someone"},
				},
			}},

			query: Query{
				SQL:  "SELECT COUNT(DISTINCT e.id) FROM events AS e JOIN event_tags AS t ON t.event_id = e.id WHERE (t.key = ? AND t.value IN (?,?)) OR (t.key = ? AND t.value = ?)",
				Args: []any{"e", "xxx", "yyy", "p", "someone"},
			},
		},
		{
			name: "multiple filter",
			filters: nostr.Filters{
				{Kinds: []int{0, 1}},
				{Authors: []string{"aaa", "bbb"}},
			},
			query: Query{
				SQL:  "SELECT ((SELECT COUNT(e.id) FROM events AS e WHERE e.kind IN (?,?)) + (SELECT COUNT(e.id) FROM events AS e WHERE e.pubkey IN (?,?)))",
				Args: []any{0, 1, "aaa", "bbb"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query, err := DefaultCountBuilder(test.filters...)
			if err != nil {
				t.Fatalf("expected error nil, got %v", err)
			}

			if !reflect.DeepEqual(query[0], test.query) {
				t.Fatalf("expected query %v, got %v", test.query, query[0])
			}
		})
	}
}
