package sqlite

import (
	"context"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

func TestConcurrency(t *testing.T) {
	size := 1000
	ctx := context.Background()
	path := t.TempDir() + "/test.sqlite"

	store, err := New(
		path,
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
	ctx := context.Background()
	path := t.TempDir() + "/test.sqlite"

	store, err := New(path)
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
			ctx := context.Background()
			path := t.TempDir() + "/test.sqlite"

			store, err := New(path)
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
				SQL:  "SELECT e.* FROM events AS e JOIN tags AS t ON t.event_id = e.id WHERE (t.key = ? AND t.value = ?) GROUP BY e.id ORDER BY e.created_at DESC, e.id ASC LIMIT ?",
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
				SQL:  "SELECT e.* FROM events AS e JOIN tags AS t ON t.event_id = e.id WHERE (t.key = ? AND t.value IN (?,?)) OR (t.key = ? AND t.value = ?) GROUP BY e.id ORDER BY e.created_at DESC, e.id ASC LIMIT ?",
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
				SQL:  "SELECT COUNT(DISTINCT e.id) FROM events AS e JOIN tags AS t ON t.event_id = e.id WHERE (t.key = ? AND t.value IN (?,?)) OR (t.key = ? AND t.value = ?)",
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

func BenchmarkSaveRegular(b *testing.B) {
	ctx := context.Background()
	path := b.TempDir() + "/test.sqlite"
	store, err := New(path)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	size := 10_000
	events := make([]*nostr.Event, size)
	for i := range size {
		events[i] = &nostr.Event{
			ID:   strconv.Itoa(i),
			Kind: 1,
		}
	}

	b.ResetTimer()
	for i := range b.N {
		_, err := store.Save(ctx, events[i%size])
		if err != nil {
			b.Fatalf("Save failed: %v", err)
		}
	}
}

func BenchmarkSaveAddressable(b *testing.B) {
	ctx := context.Background()
	path := b.TempDir() + "/test.sqlite"
	store, err := New(path)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	size := 10_000
	events := make([]*nostr.Event, size)
	for i := range size {
		events[i] = &nostr.Event{
			ID:   strconv.Itoa(i),
			Kind: 30_000,
			Tags: nostr.Tags{{"d", strconv.Itoa(i)}},
		}
	}

	b.ResetTimer()
	for i := range b.N {
		_, err := store.Save(ctx, events[i%size])
		if err != nil {
			b.Fatalf("Save failed: %v", err)
		}
	}
}

func BenchmarkDeleteRegular(b *testing.B) {
	ctx := context.Background()
	path := b.TempDir() + "/test.sqlite"
	store, err := New(path)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	size := 10_000
	ids := make([]string, size)
	for i := range size {
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
		_, err := store.Delete(ctx, ids[i%size])
		if err != nil {
			b.Fatalf("Delete failed: %v", err)
		}
	}
}

func BenchmarkDeleteAddressable(b *testing.B) {
	ctx := context.Background()
	path := b.TempDir() + "/test.sqlite"
	store, err := New(path)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	size := 10_000
	ids := make([]string, size)
	for i := range size {
		ids[i] = strconv.Itoa(i)
		event := &nostr.Event{
			ID:   ids[i],
			Kind: 30_000,
			Tags: nostr.Tags{{"d", ids[i]}},
		}

		_, err := store.Save(ctx, event)
		if err != nil {
			b.Fatalf("failed to setup: %v", err)
		}
	}

	b.ResetTimer()
	for i := range b.N {
		_, err := store.Delete(ctx, ids[i%size])
		if err != nil {
			b.Fatalf("Delete failed: %v", err)
		}
	}
}

func BenchmarkReplaceReplaceable(b *testing.B) {
	ctx := context.Background()
	path := b.TempDir() + "/test.sqlite"
	store, err := New(path)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	size := 10_000
	events := make([]*nostr.Event, size)
	for i := range size {
		events[i] = &nostr.Event{
			ID:        strconv.Itoa(i),
			Kind:      0,
			CreatedAt: nostr.Timestamp(i),
		}
	}

	b.ResetTimer()
	for i := range b.N {
		_, err := store.Replace(ctx, events[i%size])
		if err != nil {
			b.Fatalf("Replace failed: %v", err)
		}
	}
}

func BenchmarkReplaceAddressable(b *testing.B) {
	ctx := context.Background()
	path := b.TempDir() + "/test.sqlite"
	store, err := New(path)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	size := 10_000
	events := make([]*nostr.Event, size)
	for i := range size {
		events[i] = &nostr.Event{
			ID:        strconv.Itoa(i),
			Kind:      30_000,
			CreatedAt: nostr.Timestamp(i),
			Tags:      nostr.Tags{{"d", "test"}},
		}
	}

	b.ResetTimer()
	for i := range b.N {
		_, err := store.Replace(ctx, events[i%size])
		if err != nil {
			b.Fatalf("Replace failed: %v", err)
		}
	}
}
