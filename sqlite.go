// The sqlite package defines an extensible sqlite3 store for Nostr events.
package sqlite

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	_ "github.com/mattn/go-sqlite3"
	"github.com/nbd-wtf/go-nostr"
)

var (
	ErrInvalidAddressableEvent = errors.New("addressable event doesn't have a 'd' tag")
	ErrInvalidReplacement      = errors.New("called Replace on a non-replaceable event")
	ErrInternalQuery           = errors.New("internal query error")
)

//go:embed schema.sql
var schema string

// Store of Nostr events that uses an sqlite3 database.
// It embeds the *sql.DB connection for direct interaction and can use
// custom filter/event policies and query builders.
//
// All methods are safe for concurrent use. However, due to the file-based
// architecture of SQLite, there can only be one writer at the time.
//
// This limitation remains even after applying the recommended concurrency optimisations,
// such as journal_mode=WAL and PRAGMA busy_timeout=1s, which are applied by default in this implementation.
//
// Therefore it remains possible that methods return the error [sqlite3.ErrBusy].
// To reduce the likelihood of this happening, you can:
//   - increase the busy_timeout with the option [WithBusyTimeout] (default is 1s).
//   - provide synchronisation, for example with a mutex or channel(s). This however won't
//     help if there are other programs writing to the same sqlite file.
//
// If instead you want to handle all [sqlite3.ErrBusy] in your application,
// use WithBusyTimeout(0) to make blocked writers return immediatly.
//
// More about WAL mode and concurrency: https://sqlite.org/wal.html
type Store struct {
	*sql.DB

	optimizeEvery int32        // the threshold of writes that trigger PRAGMA optimize
	writeCount    atomic.Int32 // successful writes since last PRAGMA optimize

	filterPolicy FilterPolicy
	eventPolicy  EventPolicy

	queryBuilder QueryBuilder
	countBuilder QueryBuilder
}

// New returns an sqlite3 store connected to the sqlite file located at the provided
// file path, after applying the base schema, and the provided options.
func New(path string, opts ...Option) (*Store, error) {
	DB, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to sqlite3 at %s: %w", path, err)
	}

	if _, err := DB.Exec(schema); err != nil {
		return nil, fmt.Errorf("failed to apply base schema: %w", err)
	}

	if _, err := DB.Exec("PRAGMA journal_mode = WAL;"); err != nil {
		return nil, fmt.Errorf("failed to set WAL mode: %w", err)
	}

	if _, err := DB.Exec("PRAGMA busy_timeout = 1000;"); err != nil {
		return nil, fmt.Errorf("failed to set busy timeout: %w", err)
	}

	store := &Store{
		DB:            DB,
		optimizeEvery: 5000,
		filterPolicy:  defaultFilterPolicy,
		eventPolicy:   defaultEventPolicy,
		queryBuilder:  DefaultQueryBuilder,
		countBuilder:  DefaultCountBuilder,
	}

	for _, opt := range opts {
		if err := opt(store); err != nil {
			return nil, err
		}
	}

	// run full optimize after options, to inform the query planner about new indexes (if any).
	if _, err := DB.Exec("PRAGMA optimize=0x10002;"); err != nil {
		return nil, fmt.Errorf("failed to PRAGMA optimize: %w", err)
	}
	return store, nil
}

// QueryBuilder converts multiple nostr filters into one or more sqlite queries and lists of arguments.
// Not all filters can be combined into a single query, but many can.
// Filters passed to the query builder have been previously validated by the [Store.filterPolicy].
//
// It's useful to specify custom query/count builders to leverage additional schemas that have been
// provided in the [New] constructor.
//
// For examples, check out the [DefaultQueryBuilder] and [DefaultCountBuilder]
type QueryBuilder func(filters ...nostr.Filter) (queries []Query, err error)

type Query struct {
	SQL  string
	Args []any
}

// Size returns the number of events stored in the "events" table.
func (s *Store) Size(ctx context.Context) (size int, err error) {
	row := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM events;")
	if err = row.Scan(&size); err != nil {
		return -1, err
	}
	return size, nil
}

// Optimize runs "PRAGMA optimize", which updates the statistics and heuristics
// of the query planner, which should result in improved read performance.
// The Store's write methods call Optimize (roughly) every [Store.optimizeEvery]
// successful insertions or deletions.
func (s *Store) Optimize(ctx context.Context) error {
	_, err := s.DB.ExecContext(ctx, "PRAGMA optimize;")
	return err
}

func (s *Store) checkOptimize(ctx context.Context) error {
	tot := s.writeCount.Add(1)
	if tot < s.optimizeEvery {
		return nil
	}

	if s.writeCount.CompareAndSwap(tot, 0) {
		return s.Optimize(ctx)
	}
	return nil
}

// Close the underlying database connection, committing all temporary data to disk.
func (s *Store) Close() error {
	return s.DB.Close()
}

// Save the event in the store. Save is idempotent, meaning successful calls to Save
// with the same event are no-ops.
//
// Save returns true if the event has been saved, false in case of errors or if the event was already present.
// For replaceable/addressable events, it is recommended to call [Store.Replace] instead.
func (s *Store) Save(ctx context.Context, e *nostr.Event) (bool, error) {
	if err := s.eventPolicy(e); err != nil {
		return false, err
	}

	tags, err := json.Marshal(e.Tags)
	if err != nil {
		return false, fmt.Errorf("failed to marshal the event tags: %w", err)
	}

	res, err := s.DB.ExecContext(ctx, `INSERT OR IGNORE INTO events (id, pubkey, created_at, kind, tags, content, sig)
        VALUES ($1, $2, $3, $4, $5, $6, $7)`, e.ID, e.PubKey, e.CreatedAt, e.Kind, tags, e.Content, e.Sig)

	if err != nil {
		return false, fmt.Errorf("failed to execute: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to check for rows affected: %w", err)
	}

	if rows > 0 {
		s.checkOptimize(ctx)
		return true, nil
	}
	return false, nil
}

// Delete the event with the provided id. If the event is not found, nothing happens and nil is returned.
// Delete returns true if the event was deleted, false in case of errors or if the event
// was never present.
func (s *Store) Delete(ctx context.Context, id string) (bool, error) {
	res, err := s.DB.ExecContext(ctx, "DELETE FROM events WHERE id = $1", id)
	if err != nil {
		return false, fmt.Errorf("failed to execute: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to check for rows affected: %w", err)
	}

	if rows > 0 {
		s.checkOptimize(ctx)
		return true, nil
	}
	return false, nil
}

// Replace an old event with the new one according to NIP-01.
//
// The replacement happens if the event is strictly newer than the stored event
// within the same 'category' (kind, pubkey, and d-tag if addressable).
// If no such stored event exists, and the event is a replaceable/addressable kind, it is simply saved.
//
// Calling Replace on a non-replaceable/addressable event returns [ErrInvalidReplacement]
//
// Replace returns true if the event has been saved/superseded a previous one,
// false in case of errors or if a stored event in the same 'category' is newer or equal.
//
// More info here: https://github.com/nostr-protocol/nips/blob/master/01.md#kinds
func (s *Store) Replace(ctx context.Context, event *nostr.Event) (bool, error) {
	if err := s.eventPolicy(event); err != nil {
		return false, err
	}

	var query Query
	switch {
	case nostr.IsReplaceableKind(event.Kind):
		query = Query{
			SQL:  "SELECT id, created_at FROM events WHERE kind = $1 AND pubkey = $2",
			Args: []any{event.Kind, event.PubKey},
		}

	case nostr.IsAddressableKind(event.Kind):
		dTag := event.Tags.GetD()
		if dTag == "" {
			return false, ErrInvalidAddressableEvent
		}

		query = Query{
			SQL: `SELECT e.id, e.created_at FROM events AS e 
				JOIN tags AS t ON e.id = t.event_id 
				WHERE e.kind = $1 AND e.pubkey = $2 AND t.key = 'd' AND t.value = $3;`,
			Args: []any{event.Kind, event.PubKey, dTag},
		}

	default:
		return false, ErrInvalidReplacement
	}

	replaced, err := s.replace(ctx, event, query)
	if err != nil {
		return false, fmt.Errorf("failed to replace: %w", err)
	}
	return replaced, nil
}

// replace the event with the provided id with the new event.
// It's an atomic version of Save(ctx, new) + Delete(ctx, id)
func (s *Store) replace(ctx context.Context, event *nostr.Event, query Query) (bool, error) {
	tags, err := json.Marshal(event.Tags)
	if err != nil {
		return false, fmt.Errorf("failed to marshal the event tags: %w", err)
	}

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("failed to begin the transaction: %w", err)
	}
	defer tx.Rollback()

	var oldID string
	var oldCreatedAt nostr.Timestamp

	row := tx.QueryRowContext(ctx, query.SQL, query.Args...)
	err = row.Scan(&oldID, &oldCreatedAt)

	if errors.Is(err, sql.ErrNoRows) {
		// no old event found, insert the new one
		_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO events (id, pubkey, created_at, kind, tags, content, sig)
        VALUES ($1, $2, $3, $4, $5, $6, $7)`, event.ID, event.PubKey, event.CreatedAt, event.Kind, tags, event.Content, event.Sig)

		if err != nil {
			return false, err
		}

		if err = tx.Commit(); err != nil {
			return false, err
		}

		s.checkOptimize(ctx)
		return true, nil
	}

	if err != nil {
		// fail on different non nil errors
		return false, fmt.Errorf("failed to query for old event: %w", err)
	}

	if oldCreatedAt >= event.CreatedAt {
		return false, nil
	}

	if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO events (id, pubkey, created_at, kind, tags, content, sig)
	VALUES ($1, $2, $3, $4, $5, $6, $7)`, event.ID, event.PubKey, event.CreatedAt, event.Kind, tags, event.Content, event.Sig); err != nil {
		return false, err
	}

	if _, err = tx.ExecContext(ctx, "DELETE FROM events WHERE id = $1", oldID); err != nil {
		return false, err
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// Query stored events matching the provided filters.
func (s *Store) Query(ctx context.Context, filters ...nostr.Filter) ([]nostr.Event, error) {
	return s.QueryWithBuilder(ctx, s.queryBuilder, filters...)
}

// QueryWithBuilder generates an sqlite query for the filters with the provided [QueryBuilder], and executes it.
func (s *Store) QueryWithBuilder(ctx context.Context, build QueryBuilder, filters ...nostr.Filter) ([]nostr.Event, error) {
	filters, err := s.filterPolicy(filters...)
	if err != nil {
		return nil, err
	}

	queries, err := build(filters...)
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	var events []nostr.Event
	for i, query := range queries {
		rows, err := s.DB.QueryContext(ctx, query.SQL, query.Args...)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("failed to fetch events with query %s: %w", queries[i], err)
		}
		defer rows.Close()

		for rows.Next() {
			var event nostr.Event
			err = rows.Scan(&event.ID, &event.PubKey, &event.CreatedAt, &event.Kind, &event.Tags, &event.Content, &event.Sig)
			if err != nil {
				return events, fmt.Errorf("%w: failed to scan event row: %w", ErrInternalQuery, err)
			}

			events = append(events, event)
		}

		if err := rows.Err(); err != nil {
			return events, fmt.Errorf("%w: failed to scan event row: %w", ErrInternalQuery, err)
		}
	}
	return events, nil
}

// Count stored events matching the provided filters.
func (s *Store) Count(ctx context.Context, filters ...nostr.Filter) (int64, error) {
	return s.CountWithBuilder(ctx, s.countBuilder, filters...)
}

// CountWithBuilder generates an sqlite query for the filters with the provided [QueryBuilder], and executes it.
func (s *Store) CountWithBuilder(ctx context.Context, build QueryBuilder, filters ...nostr.Filter) (int64, error) {
	queries, err := build(filters...)
	if err != nil {
		return 0, fmt.Errorf("failed to build count query: %w", err)
	}

	var total int64
	for i, query := range queries {
		var count int64
		row := s.DB.QueryRowContext(ctx, query.SQL, query.Args...)
		err := row.Scan(&count)
		if err != nil {
			return 0, fmt.Errorf("failed to count events with query %s: %w", queries[i], err)
		}

		total += count
	}
	return total, nil
}

func DefaultQueryBuilder(filters ...nostr.Filter) ([]Query, error) {
	switch len(filters) {
	case 0:
		return nil, nil

	case 1:
		query, args := buildQuery(filters[0])
		query += " ORDER BY e.created_at DESC, e.id ASC LIMIT ?"
		args = append(args, filters[0].Limit)
		return []Query{{SQL: query, Args: args}}, nil

	default:
		subQueries := make([]string, 0, len(filters))
		allArgs := make([]any, 0, len(filters))
		limit := 0

		for _, filter := range filters {
			query, args := buildQuery(filter)
			subQueries = append(subQueries, query)
			allArgs = append(allArgs, args...)
			limit += filter.Limit
		}

		query := "SELECT * FROM (" + strings.Join(subQueries, " UNION ALL ") + ")" +
			" GROUP BY id ORDER BY created_at DESC, id ASC LIMIT ?"
		allArgs = append(allArgs, limit)
		return []Query{{SQL: query, Args: allArgs}}, nil
	}
}

func DefaultCountBuilder(filters ...nostr.Filter) ([]Query, error) {
	switch len(filters) {
	case 0:
		return nil, nil

	case 1:
		query, args := buildCount(filters[0])
		return []Query{{SQL: query, Args: args}}, nil

	default:
		subQueries := make([]string, 0, len(filters))
		allArgs := make([]any, 0, len(filters))

		for _, filter := range filters {
			query, args := buildCount(filter)
			subQueries = append(subQueries, "("+query+")")
			allArgs = append(allArgs, args...)
		}

		// TODO: we are summing all counts together, without any deduplication
		query := "SELECT (" + strings.Join(subQueries, " + ") + ")"
		return []Query{{SQL: query, Args: allArgs}}, nil
	}
}

func buildQuery(filter nostr.Filter) (string, []any) {
	sql := toSql(filter)
	if sql.JoinTags {
		query := "SELECT e.* FROM events AS e JOIN tags AS t ON t.event_id = e.id" +
			" WHERE " + strings.Join(sql.Conditions, " AND ") + " GROUP BY e.id"
		return query, sql.Args
	}

	query := "SELECT e.* FROM events AS e"
	if len(sql.Conditions) > 0 {
		query += " WHERE " + strings.Join(sql.Conditions, " AND ")
	}
	return query, sql.Args
}

func buildCount(filter nostr.Filter) (string, []any) {
	sql := toSql(filter)
	if sql.JoinTags {
		query := "SELECT COUNT(DISTINCT e.id) FROM events AS e JOIN tags AS t ON t.event_id = e.id" +
			" WHERE " + strings.Join(sql.Conditions, " AND ")
		return query, sql.Args
	}

	query := "SELECT COUNT(e.id) FROM events AS e"
	if len(sql.Conditions) > 0 {
		query += " WHERE " + strings.Join(sql.Conditions, " AND ")
	}
	return query, sql.Args
}

type sqlFilter struct {
	Conditions []string
	Args       []any
	JoinTags   bool
}

func toSql(filter nostr.Filter) sqlFilter {
	s := sqlFilter{}
	if len(filter.IDs) > 0 {
		s.Conditions = append(s.Conditions, "e.id"+equalityClause(filter.IDs))
		for _, id := range filter.IDs {
			s.Args = append(s.Args, id)
		}
	}

	if len(filter.Kinds) > 0 {
		s.Conditions = append(s.Conditions, "e.kind"+equalityClause(filter.Kinds))
		for _, kind := range filter.Kinds {
			s.Args = append(s.Args, kind)
		}
	}

	if len(filter.Authors) > 0 {
		s.Conditions = append(s.Conditions, "e.pubkey"+equalityClause(filter.Authors))
		for _, pk := range filter.Authors {
			s.Args = append(s.Args, pk)
		}
	}

	if filter.Until != nil {
		s.Conditions = append(s.Conditions, "e.created_at <= ?")
		s.Args = append(s.Args, filter.Until.Time().Unix())
	}

	if filter.Since != nil {
		s.Conditions = append(s.Conditions, "e.created_at >= ?")
		s.Args = append(s.Args, filter.Since.Time().Unix())
	}

	if len(filter.Tags) > 0 {
		conds := make([]string, 0, len(filter.Tags))
		args := make([]any, 0, len(filter.Tags))

		for key, vals := range filter.Tags {
			if len(vals) == 0 {
				continue
			}

			conds = append(conds, "(t.key = ? AND t.value"+equalityClause(vals)+")")
			args = append(args, key)
			for _, v := range vals {
				args = append(args, v)
			}
		}

		if len(conds) > 0 {
			s.JoinTags = true
			s.Conditions = append(s.Conditions, strings.Join(conds, " OR "))
			s.Args = append(s.Args, args...)
		}
	}
	return s
}

// equalityClause returns the appropriate SQL comparison operator and placeholder(s)
// for use in a WHERE clause, based on the number of values provided.
// If the slice contains one value, it returns " = ?".
// If it contains multiple values, it returns " IN (?, ?, ... )" with the correct number of placeholders.
// It panics is vals is nil or empty.
func equalityClause[T any](vals []T) string {
	if len(vals) == 1 {
		return " = ?"
	}
	return " IN (?" + strings.Repeat(",?", len(vals)-1) + ")"
}
