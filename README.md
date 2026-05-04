# Nostr Sqlite
Nostr-sqlite is a Go library providing a performant and highly customizable sqlite3 store for Nostr events.

[![Go Report Card](https://goreportcard.com/badge/github.com/vertex-lab/nostr-sqlite)](https://goreportcard.com/report/github.com/vertex-lab/nostr-sqlite)
[![Go Reference](https://pkg.go.dev/badge/github.com/vertex-lab/nostr-sqlite.svg)](https://pkg.go.dev/github.com/vertex-lab/nostr-sqlite)


## Installation

```
go get github.com/vertex-lab/nostr-sqlite
```

## Simple & Customizable

Nostr-sqlite works out of the box, no need to configure anything more than the database path. However, it allows you to customize your database to fit your needs, giving you access to the underlying `*sql.DB` connection, and allowing convenient setup using functional options.

```golang
store, err := sqlite.New(
    "/your/db/path/example.sqlite",
    sqlite.WithEventPolicy(myPolicy)
    sqlite.WithAdditionalSchema(mySchema),
    sqlite.WithCacheSize(64 * sqlite.MiB),
)
```

To find all available options, see [options.go](/options.go).

## Minimal & Powerful API

```golang
// Save the event in the store in an idempotent way.
Save(context.Context, *nostr.Event) (bool, error)

// Replace an old event with the new one according to NIP-01.
Replace(context.Context, *nostr.Event) (bool, error)

// Delete all events matching the provided filters and returns the number of deleted events.
Delete(context.Context, ...nostr.Filter) (int, error)

// DeleteRequest processes a NIP-09 deletion request (kind 5 event), deleting all referenced events.
DeleteRequest(ctx context.Context, event *nostr.Event) (int, error)

// Query stored events matching the provided filters.
Query(ctx context.Context, filters ...nostr.Filter) ([]nostr.Event, error)

// Count stored events matching the provided filters.
Count(ctx context.Context, filters ...nostr.Filter) (int64, error)

// Has returns true if the store contains at least one event matching the provided filters.
Has(ctx context.Context, filters ...nostr.Filter) (bool, error)
```

## Database Schema

The database schema contains two tables, one for the events, and one for the tags. By default, only the first `d` tag of addressable events is added to the tags table, making it efficient to fetch events in the same "category" (same `kind`, `pubkey` and `d` tag).

To index more tags, you can add triggers in the contructor using the `WithAdditionalSchema` option. Only the tags indexed in the tags table will be used in queries by the default query builder.

```golang
myTrigger = `
    CREATE TRIGGER IF NOT EXISTS e_tags_ai AFTER INSERT ON events
	WHEN NEW.kind = 1 
	BEGIN
	INSERT OR IGNORE INTO tags (event_id, key, value)
		SELECT NEW.id, 'e', json_extract(value, '$[1]')
		FROM json_each(NEW.tags)
		WHERE json_type(value) = 'array' AND json_array_length(value) > 1 AND json_extract(value, '$[0]') = 'e'
		LIMIT 1;
	END;`

store, err := sqlite.New(
    "/your/db/path/example.sqlite",
    sqlite.WithAdditionalSchema(myTrigger),
)

// Now querying for kind 1 events with specific "e" tags can return results
```

## Performance

Sqlite can often outperform server-based databases (e.g. Postgres) as the network overhead is often the major bottleneck. However, when performing many thousands of concurrent writes every second, sqlite performance degrades because there can only be a single writer at a time. More in the [concurrency section](#concurrency).

## Concurrency

This store implements all recommended concurrency optimisations for sqlite,
such as `journal_mode=WAL` and `busy_timeout=5000`.

However, sqlite fundamental limitation remains: there can only be one writer at a time. If two write operations are called concurrently, one will get the database lock, while the other will be blocked waiting. The default maximal wait before returning an error is 5s.

Under heavy concurrency, it is possible that this wait is not enough for all previous queued operations to complete, resulting in the error `sqlite3.ErrBusy` to be returned.
To reduce the likelihood of this happening, you can:
- increase the `busy_timeout` with the option `WithBusyTimeout` (default is 5s).
- provide synchronisation, for example with a mutex or channel(s). This however won't help if there are other programs writing to the same sqlite file.

If instead you want to handle all `sqlite3.ErrBusy` in your application,
use `WithBusyTimeout(0)` to make blocked writers return immediately.

You can learn more about WAL mode and concurrency in the [official documentation](https://sqlite.org/wal.html).
