package sqlite

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

var ctx = context.Background()

func TestConcurrency(t *testing.T) {
	size := 1000
	store, err := New(
		"file::memory:?cache=shared",
		WithOptimisationEvery(500),
		WithBusyTimeout(time.Second/2),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	wg := &sync.WaitGroup{}
	done := make(chan struct{})
	errC := make(chan error, size)

	wg.Add(size)
	go func() {
		for i := range size {
			go func(i int) {
				defer wg.Done()
				event := nostr.Event{ID: strconv.Itoa(i)}
				_, err := store.Save(ctx, &event)
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
		// check the size of the db
		actual, err := store.Size(ctx)
		if err != nil {
			t.Fatalf("Size unexpected error: %v", err)
		}

		if actual != size {
			t.Fatalf("expected size %d, got %v", size, actual)
		}

		// check if the optimization where performed
		writes := store.writeCount.Load()
		if writes > store.optimizeEvery {
			t.Fatalf("write count %d is greater than optimizeEvery %d", writes, store.optimizeEvery)
		}
	}
}

func TestSave(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	event := nostr.Event{
		Kind:    30000,
		Content: "test",
		Tags:    nostr.Tags{{"d", "test-tag"}},
	}

	saved, err := store.Save(ctx, &event)
	if err != nil {
		t.Fatal(err)
	}

	if !saved {
		t.Fatalf("expected saved true, got false")
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

func TestQuery(t *testing.T) {
	store, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// seed 10 kind 0 and 10 kind 1 events
	events := make([]nostr.Event, 0, 20)
	for i := range 10 {
		events = append(events, nostr.Event{
			ID:        "kind0-" + strconv.Itoa(i),
			Kind:      0,
			CreatedAt: nostr.Timestamp(i),
		})

		events = append(events, nostr.Event{
			ID:        "kind1-" + strconv.Itoa(i),
			Kind:      1,
			CreatedAt: nostr.Timestamp(i),
		})
	}

	for _, event := range events {
		if _, err := store.Save(ctx, &event); err != nil {
			t.Fatalf("Save failed: %v", err)
		}
	}

	results, err := store.Query(ctx,
		nostr.Filter{Kinds: []int{0}, Limit: 2},
		nostr.Filter{Kinds: []int{1}, Limit: 5},
	)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	kind0, kind1 := 0, 0
	for _, e := range results {
		switch e.Kind {
		case 0:
			kind0++
		case 1:
			kind1++
		}
	}

	if kind0 != 2 {
		t.Errorf("expected 2 kind-0 events, got %d", kind0)
	}
	if kind1 != 5 {
		t.Errorf("expected 5 kind-1 events, got %d", kind1)
	}
}

func TestReplace(t *testing.T) {
	event10 := nostr.Event{ID: "bbb", Kind: 0, PubKey: "key", CreatedAt: 10}
	event100 := nostr.Event{ID: "aaa", Kind: 0, PubKey: "key", CreatedAt: 100}

	tests := []struct {
		name     string
		stored   nostr.Event
		new      nostr.Event
		replaced bool
	}{
		{
			name:     "no replace (event is not newer)",
			stored:   event100,
			new:      event10,
			replaced: false,
		},
		{
			name:     "valid replace (event is newer)",
			stored:   event10,
			new:      event100,
			replaced: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := New(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()

			saved, err := store.Replace(ctx, &test.stored)
			if err != nil {
				t.Fatal(err)
			}

			if !saved {
				t.Fatalf("expected saved true, got false")
			}

			replaced, err := store.Replace(ctx, &test.new)
			if err != nil {
				t.Fatal(err)
			}

			if replaced != test.replaced {
				t.Fatalf("expected replaced %v, got %v", test.replaced, replaced)
			}

			res, err := store.Query(ctx, nostr.Filter{IDs: []string{test.stored.ID, test.new.ID}, Limit: 1})
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}

			if len(res) != 1 {
				t.Errorf("expected one event, got %d", len(res))
			}

			switch replaced {
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
		queries []Query
	}{
		{
			name:    "single filter, kind",
			filters: nostr.Filters{{Kinds: []int{0, 1}, Limit: 100}},
			queries: []Query{{
				SQL:  "SELECT e.* FROM events AS e WHERE e.kind IN (?,?) ORDER BY e.created_at DESC, e.id ASC LIMIT ?",
				Args: []any{0, 1, 100},
			}},
		},
		{
			name:    "single filter, authors",
			filters: nostr.Filters{{Authors: []string{"aaa", "bbb", "xxx"}, Limit: 11}},
			queries: []Query{{
				SQL:  "SELECT e.* FROM events AS e WHERE e.pubkey IN (?,?,?) ORDER BY e.created_at DESC, e.id ASC LIMIT ?",
				Args: []any{"aaa", "bbb", "xxx", 11},
			}},
		},
		{
			name: "single filter, tag",
			filters: nostr.Filters{{
				Limit: 11,
				Tags: nostr.TagMap{
					"e": {"xxx"},
				},
			}},
			queries: []Query{{
				SQL:  "SELECT e.* FROM events AS e WHERE e.id IN (SELECT event_id FROM tags WHERE key = ? AND value = ?) ORDER BY e.created_at DESC, e.id ASC LIMIT ?",
				Args: []any{"e", "xxx", 11},
			}},
		},
		{
			name: "single filter, tags",
			filters: nostr.Filters{{
				Limit: 11,
				Tags: nostr.TagMap{
					"e": {"xxx", "yyy"},
					"p": {"alice", "bob"},
				},
			}},
			queries: []Query{{
				SQL:  "SELECT e.* FROM events AS e WHERE e.id IN (SELECT event_id FROM tags WHERE key = ? AND value IN (?,?)) AND e.id IN (SELECT event_id FROM tags WHERE key = ? AND value IN (?,?)) ORDER BY e.created_at DESC, e.id ASC LIMIT ?",
				Args: []any{"e", "xxx", "yyy", "p", "alice", "bob", 11},
			}},
		},
		{
			name: "single filter, kinds and tags",
			filters: nostr.Filters{{
				Limit: 11,
				Kinds: []int{0, 1},
				Tags: nostr.TagMap{
					"e": {"xxx", "yyy"},
					"p": {"alice", "bob"},
				},
			}},
			queries: []Query{{
				SQL:  "SELECT e.* FROM events AS e WHERE e.kind IN (?,?) AND e.id IN (SELECT event_id FROM tags WHERE key = ? AND value IN (?,?)) AND e.id IN (SELECT event_id FROM tags WHERE key = ? AND value IN (?,?)) ORDER BY e.created_at DESC, e.id ASC LIMIT ?",
				Args: []any{0, 1, "e", "xxx", "yyy", "p", "alice", "bob", 11},
			}},
		},
		{
			name: "multiple filter",
			filters: nostr.Filters{
				{Kinds: []int{0, 1}, Limit: 69},
				{Authors: []string{"aaa", "bbb"}, Limit: 420},
			},
			queries: []Query{
				{
					SQL:  "SELECT e.* FROM events AS e WHERE e.kind IN (?,?) ORDER BY e.created_at DESC, e.id ASC LIMIT ?",
					Args: []any{0, 1, 69},
				},
				{
					SQL:  "SELECT e.* FROM events AS e WHERE e.pubkey IN (?,?) ORDER BY e.created_at DESC, e.id ASC LIMIT ?",
					Args: []any{"aaa", "bbb", 420},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queries, err := DefaultQueryBuilder(test.filters...)
			if err != nil {
				t.Fatalf("expected error nil, got %v", err)
			}

			if len(queries) != len(test.queries) {
				t.Fatalf("expected %d queries, got %d", len(test.queries), len(queries))
			}

			for i, expected := range test.queries {
				if queries[i].SQL != expected.SQL {
					t.Fatalf("query[%d]: expected SQL %v, got %v", i, expected.SQL, queries[i].SQL)
				}

				// compare the set of args to avoid false positives caused by argument order
				argsSet := toSet(queries[i].Args)
				expectedSet := toSet(expected.Args)
				if !reflect.DeepEqual(argsSet, expectedSet) {
					t.Fatalf("query[%d]: expected Args %v, got %v", i, expected.Args, queries[i].Args)
				}
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
				SQL:  "SELECT COUNT(*) FROM events AS e WHERE (e.kind = ?)",
				Args: []any{0},
			},
		},
		{
			name:    "single filter, authors",
			filters: nostr.Filters{{Authors: []string{"aaa", "bbb", "xxx"}}},
			query: Query{
				SQL:  "SELECT COUNT(*) FROM events AS e WHERE (e.pubkey IN (?,?,?))",
				Args: []any{"aaa", "bbb", "xxx"},
			},
		},
		{
			name: "single filter, tags",
			filters: nostr.Filters{{
				Limit: 11,
				Tags: nostr.TagMap{
					"e": {"xxx", "yyy"},
					"p": {"alice", "bob"},
				},
			}},

			query: Query{
				SQL:  "SELECT COUNT(*) FROM events AS e WHERE (e.id IN (SELECT event_id FROM tags WHERE key = ? AND value IN (?,?)) AND e.id IN (SELECT event_id FROM tags WHERE key = ? AND value IN (?,?)))",
				Args: []any{"e", "xxx", "yyy", "p", "alice", "bob"},
			},
		},
		{
			name: "multiple filter",
			filters: nostr.Filters{
				{Kinds: []int{0, 1}},
				{Authors: []string{"aaa", "bbb"}},
			},
			query: Query{
				SQL:  "SELECT COUNT(*) FROM events AS e WHERE (e.kind IN (?,?)) OR (e.pubkey IN (?,?))",
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

			sql := query[0].SQL
			if sql != test.query.SQL {
				t.Fatalf("expected SQL %v, got %v", test.query.SQL, sql)
			}

			// compare the set of args as to avoid false positives caused by the order of the arguments
			args := toSet(query[0].Args)
			expected := toSet(test.query.Args)
			if !reflect.DeepEqual(args, expected) {
				t.Fatalf("expected Args %v, got %v", test.query.Args, args)
			}
		})
	}
}

func TestDeleteRequest(t *testing.T) {
	const (
		alice = "alice"
		bob   = "bob"
	)

	tests := []struct {
		name    string
		stored  []nostr.Event
		request nostr.Event
		err     error
		deleted int
	}{
		{
			name:    "wrong kind",
			request: nostr.Event{Kind: 1},
			err:     ErrInvalidDeletionRequest,
		},
		{
			name:    "no e or a tags",
			request: nostr.Event{Kind: 5, PubKey: alice},
		},
		{
			name: "e tag: matches pubkey",
			stored: []nostr.Event{
				{ID: "event1", PubKey: alice, Kind: 1},
			},
			request: nostr.Event{
				Kind:   5,
				PubKey: alice,
				Tags:   nostr.Tags{{"e", "event1"}},
			},
			deleted: 1,
		},
		{
			name: "e tag: pubkey mismatch",
			stored: []nostr.Event{
				{ID: "event1", PubKey: bob, Kind: 1},
			},
			request: nostr.Event{
				Kind:   5,
				PubKey: alice,
				Tags:   nostr.Tags{{"e", "event1"}},
			},
			deleted: 0,
		},
		{
			name: "e tag: event not found",
			request: nostr.Event{
				Kind:   5,
				PubKey: alice,
				Tags:   nostr.Tags{{"e", "nonexistent"}},
			},
			deleted: 0,
		},
		{
			name: "multiple e tags",
			stored: []nostr.Event{
				{ID: "e1", PubKey: alice, Kind: 1},
				{ID: "e2", PubKey: alice, Kind: 1},
			},
			request: nostr.Event{
				Kind:   5,
				PubKey: alice,
				Tags:   nostr.Tags{{"e", "e1"}, {"e", "e2"}},
			},
			deleted: 2,
		},
		{
			name: "multiple e tags: partial match",
			stored: []nostr.Event{
				{ID: "e1", PubKey: alice, Kind: 1},
			},
			request: nostr.Event{
				Kind:   5,
				PubKey: alice,
				Tags:   nostr.Tags{{"e", "e1"}, {"e", "nonexistent"}},
			},
			deleted: 1,
		},
		{
			name: "a tag: deletes addressable event up to created_at",
			stored: []nostr.Event{
				{ID: "addr1", PubKey: alice, Kind: 30000, Tags: nostr.Tags{{"d", "test"}}, CreatedAt: 100},
			},
			request: nostr.Event{
				Kind:      5,
				PubKey:    alice,
				CreatedAt: 200,
				Tags:      nostr.Tags{{"a", "30000:alice:test"}},
			},
			deleted: 1,
		},
		{
			name: "a tag: skips addressable event newer than created_at",
			stored: []nostr.Event{
				{ID: "addr1", PubKey: alice, Kind: 30000, Tags: nostr.Tags{{"d", "test"}}, CreatedAt: 300},
			},
			request: nostr.Event{
				Kind:      5,
				PubKey:    alice,
				CreatedAt: 200,
				Tags:      nostr.Tags{{"a", "30000:alice:test"}},
			},
			deleted: 0,
		},
		{
			name: "a tag: pubkey mismatch",
			stored: []nostr.Event{
				{ID: "addr1", PubKey: bob, Kind: 30000, Tags: nostr.Tags{{"d", "test"}}, CreatedAt: 100},
			},
			request: nostr.Event{
				Kind:      5,
				PubKey:    alice,
				CreatedAt: 200,
				Tags:      nostr.Tags{{"a", "30000:bob:test"}},
			},
			deleted: 0,
		},
		{
			name: "a tag: malformed (missing separators)",
			request: nostr.Event{
				Kind:      5,
				PubKey:    alice,
				CreatedAt: 200,
				Tags:      nostr.Tags{{"a", "notvalid"}},
			},
			deleted: 0,
		},
		{
			name: "a tag: malformed (non-integer kind)",
			request: nostr.Event{
				Kind:      5,
				PubKey:    alice,
				CreatedAt: 200,
				Tags:      nostr.Tags{{"a", "notakind:alice:test"}},
			},
			deleted: 0,
		},
		{
			name: "a tag: multiple versions, deletes only up to created_at",
			stored: []nostr.Event{
				{ID: "v1", PubKey: alice, Kind: 30000, Tags: nostr.Tags{{"d", "doc"}}, CreatedAt: 50},
				{ID: "v2", PubKey: alice, Kind: 30000, Tags: nostr.Tags{{"d", "doc"}}, CreatedAt: 150},
				{ID: "v3", PubKey: alice, Kind: 30000, Tags: nostr.Tags{{"d", "doc"}}, CreatedAt: 250},
			},
			request: nostr.Event{
				Kind:      5,
				PubKey:    alice,
				CreatedAt: 200,
				Tags:      nostr.Tags{{"a", "30000:alice:doc"}},
			},
			deleted: 2,
		},
		{
			name: "mixed e and a tags",
			stored: []nostr.Event{
				{ID: "regular1", PubKey: alice, Kind: 1, CreatedAt: 100},
				{ID: "addr1", PubKey: alice, Kind: 30000, Tags: nostr.Tags{{"d", "home"}}, CreatedAt: 100},
			},
			request: nostr.Event{
				Kind:      5,
				PubKey:    alice,
				CreatedAt: 200,
				Tags:      nostr.Tags{{"e", "regular1"}, {"a", "30000:alice:home"}},
			},
			deleted: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := New(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()

			for i := range test.stored {
				if _, err := store.Save(ctx, &test.stored[i]); err != nil {
					t.Fatalf("Save failed: %v", err)
				}
			}

			deleted, err := store.DeleteRequest(ctx, &test.request)
			if !errors.Is(err, test.err) {
				t.Fatalf("expected error %v, got %v", test.err, err)
			}
			if deleted != test.deleted {
				t.Fatalf("expected %d deleted, got %d", test.deleted, deleted)
			}
		})
	}
}

func BenchmarkSaveRegular(b *testing.B) {
	path := b.TempDir() + "/test.sqlite"
	store, err := New(path)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	events := make([]*nostr.Event, b.N)
	for i := range b.N {
		events[i] = &nostr.Event{
			ID:   strconv.Itoa(i),
			Kind: 1,
		}
	}

	b.ResetTimer()
	for i := range b.N {
		_, err := store.Save(ctx, events[i])
		if err != nil {
			b.Fatalf("Save failed: %v", err)
		}
	}
}

func BenchmarkSaveAddressable(b *testing.B) {
	path := b.TempDir() + "/test.sqlite"
	store, err := New(path)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	events := make([]*nostr.Event, b.N)
	for i := range b.N {
		events[i] = &nostr.Event{
			ID:   strconv.Itoa(i),
			Kind: 30_000,
			Tags: nostr.Tags{{"d", strconv.Itoa(i)}},
		}
	}

	b.ResetTimer()
	for i := range b.N {
		_, err := store.Save(ctx, events[i])
		if err != nil {
			b.Fatalf("Save failed: %v", err)
		}
	}
}

func BenchmarkDelete(b *testing.B) {
	path := b.TempDir() + "/test.sqlite"
	store, err := New(path)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	ids := make([]string, b.N)
	for i := range b.N {
		ids[i] = strconv.Itoa(i)
		event := &nostr.Event{
			ID:   ids[i],
			Kind: 1,
		}

		_, err := store.Save(ctx, event)
		if err != nil {
			b.Fatalf("failed to setup: %v", err)
		}
	}

	b.ResetTimer()
	for i := range b.N {
		_, err := store.Delete(ctx, ids[i])
		if err != nil {
			b.Fatalf("Delete failed: %v", err)
		}
	}
}

func BenchmarkDeleteRequestETags(b *testing.B) {
	path := b.TempDir() + "/test.sqlite"
	store, err := New(path)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	ids := make([]string, b.N)
	for i := range b.N {
		ids[i] = strconv.Itoa(i)
		if _, err := store.Save(ctx, &nostr.Event{
			ID:     ids[i],
			PubKey: "pubkey",
			Kind:   1,
		}); err != nil {
			b.Fatalf("failed to setup: %v", err)
		}
	}

	b.ResetTimer()
	for i := range b.N {
		request := &nostr.Event{
			Kind:   5,
			PubKey: "pubkey",
			Tags:   nostr.Tags{{"e", ids[i]}},
		}
		if _, err := store.DeleteRequest(ctx, request); err != nil {
			b.Fatalf("DeleteRequest failed: %v", err)
		}
	}
}

func BenchmarkDeleteRequestATags(b *testing.B) {
	path := b.TempDir() + "/test.sqlite"
	store, err := New(path)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	ids := make([]string, b.N)
	for i := range b.N {
		ids[i] = strconv.Itoa(i)
		if _, err := store.Save(ctx, &nostr.Event{
			ID:     ids[i],
			PubKey: "pubkey",
			Kind:   30_000,
			Tags:   nostr.Tags{{"d", ids[i]}},
		}); err != nil {
			b.Fatalf("failed to setup: %v", err)
		}
	}

	b.ResetTimer()
	for i := range b.N {
		request := &nostr.Event{
			Kind:      5,
			PubKey:    "pubkey",
			CreatedAt: 1000,
			Tags:      nostr.Tags{{"a", "30000:pubkey:" + ids[i]}},
		}
		if _, err := store.DeleteRequest(ctx, request); err != nil {
			b.Fatalf("DeleteRequest failed: %v", err)
		}
	}
}

func BenchmarkReplaceReplaceable(b *testing.B) {
	path := b.TempDir() + "/test.sqlite"
	store, err := New(path)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	events := make([]*nostr.Event, b.N)
	for i := range b.N {
		events[i] = &nostr.Event{
			ID:        strconv.Itoa(i),
			Kind:      0,
			CreatedAt: nostr.Timestamp(i),
		}
	}

	b.ResetTimer()
	for i := range b.N {
		_, err := store.Replace(ctx, events[i])
		if err != nil {
			b.Fatalf("Replace failed: %v", err)
		}
	}
}

func BenchmarkReplaceAddressable(b *testing.B) {
	path := b.TempDir() + "/test.sqlite"
	store, err := New(path)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	events := make([]*nostr.Event, b.N)
	for i := range b.N {
		events[i] = &nostr.Event{
			ID:        strconv.Itoa(i),
			Kind:      30_000,
			CreatedAt: nostr.Timestamp(i),
			Tags:      nostr.Tags{{"d", "test"}},
		}
	}

	b.ResetTimer()
	for i := range b.N {
		if _, err := store.Replace(ctx, events[i]); err != nil {
			b.Fatalf("Replace failed: %v", err)
		}
	}
}

func toSet(slice []any) map[any]struct{} {
	set := make(map[any]struct{})
	for _, item := range slice {
		set[item] = struct{}{}
	}
	return set
}
