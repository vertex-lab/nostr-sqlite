//go:build !js

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"fiatjaf.com/nostr"
	_ "github.com/mattn/go-sqlite3"
)

func testOpenDB(path string) (*sql.DB, error) {
	return sql.Open("sqlite3", path)
}

func TestConcurrency(t *testing.T) {
	size := 1000
	ctx := context.Background()
	path := t.TempDir() + "/test.sqlite"
	db, err := testOpenDB(path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(
		db,
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
				id, err := nostr.IDFromHex(fmt.Sprintf("%064d", i))
				if err != nil {
					errC <- err
					return
				}
				event := nostr.Event{ID: id}
				_, err = store.Save(ctx, &event)
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
			t.Fatalf("expected size:\n%d\ngot\n%v", size, actual)
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
	db, err := testOpenDB(path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(db)
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
		t.Fatalf(" expected:\n%v\ngot\n%v", event, res[0])
	}
}

func TestReplace(t *testing.T) {
	idBbb, _ := nostr.IDFromHex("0000000000000000000000000000000000000000000000000000000000000bbb")
	idAaa, _ := nostr.IDFromHex("0000000000000000000000000000000000000000000000000000000000000aaa")
	key, _ := nostr.PubKeyFromHex("0000000000000000000000000000000000000000000000000000000000000001")
	event10 := nostr.Event{ID: idBbb, Kind: 0, PubKey: key, CreatedAt: 10}
	event100 := nostr.Event{ID: idAaa, Kind: 0, PubKey: key, CreatedAt: 100}

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
			db, err := testOpenDB(path)
			if err != nil {
				t.Fatal(err)
			}
			store, err := New(db)
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
				t.Fatalf("expected replaced:\n%v\ngot\n%v", test.replaced, replaced)
			}
			res, err := store.Query(ctx, nostr.Filter{IDs: []nostr.ID{test.stored.ID, test.new.ID}, Limit: 1})
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
	pkAaa, _ := nostr.PubKeyFromHex("0000000000000000000000000000000000000000000000000000000000000aaa")
	pkBbb, _ := nostr.PubKeyFromHex("0000000000000000000000000000000000000000000000000000000000000bbb")
	pkXxx, _ := nostr.PubKeyFromHex("0000000000000000000000000000000000000000000000000000000000000ccc")
	tests := []struct {
		name    string
		filters []nostr.Filter
		query   Query
	}{
		{
			name:    "single filter, kind",
			filters: []nostr.Filter{{Kinds: []nostr.Kind{0, 1}, Limit: 100}},
			query: Query{
				SQL:  "SELECT e.* FROM events AS e WHERE e.kind IN (?,?) ORDER BY e.created_at DESC, e.id ASC LIMIT ?",
				Args: []any{0, 1, 100},
			},
		},
		{
			name:    "single filter, authors",
			filters: []nostr.Filter{{Authors: []nostr.PubKey{pkAaa, pkBbb, pkXxx}, Limit: 11}},
			query: Query{
				SQL:  "SELECT e.* FROM events AS e WHERE e.pubkey IN (?,?,?) ORDER BY e.created_at DESC, e.id ASC LIMIT ?",
				Args: []any{"0000000000000000000000000000000000000000000000000000000000000aaa", "0000000000000000000000000000000000000000000000000000000000000bbb", "0000000000000000000000000000000000000000000000000000000000000ccc", 11},
			},
		},
		{
			name: "single filter, tag",
			filters: []nostr.Filter{{
				Limit: 11,
				Tags: nostr.TagMap{
					"e": {"0000000000000000000000000000000000000000000000000000000000000ccc"},
				},
			}},

			query: Query{
				SQL:  "SELECT e.* FROM events AS e JOIN tags AS t ON t.event_id = e.id WHERE (t.key = ? AND t.value = ?) GROUP BY e.id ORDER BY e.created_at DESC, e.id ASC LIMIT ?",
				Args: []any{"e", "0000000000000000000000000000000000000000000000000000000000000ccc", 11},
			},
		},
		{
			name: "single filter, tags",
			filters: []nostr.Filter{{
				Limit: 11,
				Tags: nostr.TagMap{
					"e": {"0000000000000000000000000000000000000000000000000000000000000ccc", "0000000000000000000000000000000000000000000000000000000000000ddd"},
					"p": {"0000000000000000000000000000000000000000000000000000000000000fff"},
				},
			}},

			query: Query{
				SQL:  "SELECT e.* FROM events AS e JOIN tags AS t ON t.event_id = e.id WHERE (t.key = ? AND t.value IN (?,?)) OR (t.key = ? AND t.value = ?) GROUP BY e.id ORDER BY e.created_at DESC, e.id ASC LIMIT ?",
				Args: []any{"e", "0000000000000000000000000000000000000000000000000000000000000ccc", "0000000000000000000000000000000000000000000000000000000000000ddd", "p", "0000000000000000000000000000000000000000000000000000000000000fff", 11},
			},
		},
		{
			name: "multiple filter",
			filters: []nostr.Filter{
				{Kinds: []nostr.Kind{0, 1}, Limit: 69},
				{Authors: []nostr.PubKey{pkAaa, pkBbb}, Limit: 420},
			},
			query: Query{
				SQL:  "SELECT * FROM (SELECT e.* FROM events AS e WHERE e.kind IN (?,?) UNION ALL SELECT e.* FROM events AS e WHERE e.pubkey IN (?,?)) GROUP BY id ORDER BY created_at DESC, id ASC LIMIT ?",
				Args: []any{0, 1, "0000000000000000000000000000000000000000000000000000000000000aaa", "0000000000000000000000000000000000000000000000000000000000000bbb", 69 + 420},
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
				t.Fatalf("expected query:\n%v\ngot\n%v", test.query, query[0])
			}
		})
	}
}

func TestDefaultCountBuilder(t *testing.T) {
	pkAaa, _ := nostr.PubKeyFromHex("0000000000000000000000000000000000000000000000000000000000000aaa")
	pkBbb, _ := nostr.PubKeyFromHex("0000000000000000000000000000000000000000000000000000000000000bbb")
	pkXxx, _ := nostr.PubKeyFromHex("0000000000000000000000000000000000000000000000000000000000000ccc")
	tests := []struct {
		name    string
		filters []nostr.Filter
		query   Query
	}{
		{
			name:    "single filter, kind",
			filters: []nostr.Filter{{Kinds: []nostr.Kind{0}}},
			query: Query{
				SQL:  "SELECT COUNT(e.id) FROM events AS e WHERE e.kind = ?",
				Args: []any{0},
			},
		},
		{
			name:    "single filter, authors",
			filters: []nostr.Filter{{Authors: []nostr.PubKey{pkAaa, pkBbb, pkXxx}}},
			query: Query{
				SQL:  "SELECT COUNT(e.id) FROM events AS e WHERE e.pubkey IN (?,?,?)",
				Args: []any{"0000000000000000000000000000000000000000000000000000000000000aaa", "0000000000000000000000000000000000000000000000000000000000000bbb", "0000000000000000000000000000000000000000000000000000000000000ccc"},
			},
		},
		{
			name: "single filter, tags",
			filters: []nostr.Filter{{
				Limit: 11,
				Tags: nostr.TagMap{
					"e": {"0000000000000000000000000000000000000000000000000000000000000ccc", "0000000000000000000000000000000000000000000000000000000000000ddd"},
					"p": {"0000000000000000000000000000000000000000000000000000000000000fff"},
				},
			}},

			query: Query{
				SQL:  "SELECT COUNT(DISTINCT e.id) FROM events AS e JOIN tags AS t ON t.event_id = e.id WHERE (t.key = ? AND t.value IN (?,?)) OR (t.key = ? AND t.value = ?)",
				Args: []any{"e", "0000000000000000000000000000000000000000000000000000000000000ccc", "0000000000000000000000000000000000000000000000000000000000000ddd", "p", "0000000000000000000000000000000000000000000000000000000000000fff"},
			},
		},
		{
			name: "multiple filter",
			filters: []nostr.Filter{
				{Kinds: []nostr.Kind{0, 1}},
				{Authors: []nostr.PubKey{pkAaa, pkBbb}},
			},
			query: Query{
				SQL:  "SELECT ((SELECT COUNT(e.id) FROM events AS e WHERE e.kind IN (?,?)) + (SELECT COUNT(e.id) FROM events AS e WHERE e.pubkey IN (?,?)))",
				Args: []any{0, 1, "0000000000000000000000000000000000000000000000000000000000000aaa", "0000000000000000000000000000000000000000000000000000000000000bbb"},
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
				t.Fatalf("expected query:\n%v\ngot\n%v", test.query, query[0])
			}
		})
	}
}

func BenchmarkSaveRegular(b *testing.B) {
	ctx := context.Background()
	path := b.TempDir() + "/test.sqlite"
	db, err := testOpenDB(path)
	if err != nil {
		b.Fatal(err)
	}
	store, err := New(db)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	size := 10_000
	events := make([]*nostr.Event, size)
	for i := range size {
		id, err := nostr.IDFromHex(fmt.Sprintf("%064d", i))
		if err != nil {
			b.Fatal(err)
		}
		events[i] = &nostr.Event{
			ID:   id,
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
	db, err := testOpenDB(path)
	if err != nil {
		b.Fatal(err)
	}
	store, err := New(db)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	size := 10_000
	events := make([]*nostr.Event, size)
	for i := range size {
		id, err := nostr.IDFromHex(fmt.Sprintf("%064d", i))
		if err != nil {
			b.Fatal(err)
		}
		events[i] = &nostr.Event{
			ID:   id,
			Kind: 30_000,
			Tags: nostr.Tags{{"d", id.Hex()}},
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
	db, err := testOpenDB(path)
	if err != nil {
		b.Fatal(err)
	}
	store, err := New(db)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	size := 10_000
	ids := make([]nostr.ID, size)
	for i := range size {
		id, err := nostr.IDFromHex(fmt.Sprintf("%064d", i))
		if err != nil {
			b.Fatal(err)
		}
		ids[i] = id
		event := &nostr.Event{
			ID:   ids[i],
			Kind: 1,
		}

		_, err = store.Save(ctx, event)
		if err != nil {
			b.Fatalf("failed to setup: %v", err)
		}
	}

	b.ResetTimer()
	for i := range b.N {
		_, err := store.Delete(ctx, ids[i%size].Hex())
		if err != nil {
			b.Fatalf("Delete failed: %v", err)
		}
	}
}

func BenchmarkDeleteAddressable(b *testing.B) {
	ctx := context.Background()
	path := b.TempDir() + "/test.sqlite"
	db, err := testOpenDB(path)
	if err != nil {
		b.Fatal(err)
	}
	store, err := New(db)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	size := 10_000
	ids := make([]nostr.ID, size)
	for i := range size {
		id, err := nostr.IDFromHex(fmt.Sprintf("%064d", i))
		if err != nil {
			b.Fatal(err)
		}
		ids[i] = id
		event := &nostr.Event{
			ID:   ids[i],
			Kind: 30_000,
			Tags: nostr.Tags{{"d", ids[i].Hex()}},
		}

		_, err = store.Save(ctx, event)
		if err != nil {
			b.Fatalf("failed to setup: %v", err)
		}
	}

	b.ResetTimer()
	for i := range b.N {
		_, err := store.Delete(ctx, ids[i%size].Hex())
		if err != nil {
			b.Fatalf("Delete failed: %v", err)
		}
	}
}

func BenchmarkReplaceReplaceable(b *testing.B) {
	ctx := context.Background()
	path := b.TempDir() + "/test.sqlite"
	db, err := testOpenDB(path)
	if err != nil {
		b.Fatal(err)
	}
	store, err := New(db)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	size := 10_000
	events := make([]*nostr.Event, size)
	for i := range size {
		id, err := nostr.IDFromHex(fmt.Sprintf("%064d", i))
		if err != nil {
			b.Fatal(err)
		}
		events[i] = &nostr.Event{
			ID:        id,
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
	db, err := testOpenDB(path)
	if err != nil {
		b.Fatal(err)
	}
	store, err := New(db)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	size := 10_000
	events := make([]*nostr.Event, size)
	for i := range size {
		id, err := nostr.IDFromHex(fmt.Sprintf("%064d", i))
		if err != nil {
			b.Fatal(err)
		}
		events[i] = &nostr.Event{
			ID:        id,
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
