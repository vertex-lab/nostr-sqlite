package sqlite

import (
	"errors"
	"fmt"
	"time"

	"fiatjaf.com/nostr"
)

type Option func(*Store) error

// WithAdditionalSchema allows to specify an additional database schema, like new tables,
// virtual tables, indexes and triggers.
func WithAdditionalSchema(schema string) Option {
	return func(s *Store) error {
		if _, err := s.DB.Exec(schema); err != nil {
			return fmt.Errorf("failed to apply additional schema: %w", err)
		}
		return nil
	}
}

// WithBusyTimeout sets the SQLite PRAGMA busy_timeout. This allows concurrent
// operations to retry when the database is locked, up to the specified duration.
// The duration is internally converted to milliseconds, as required by SQLite.
func WithBusyTimeout(d time.Duration) Option {
	return func(s *Store) error {
		// had to use string manipulation rather than the '?' syntax because
		// apparently doesn't work with PRAGMA commands.
		cmd := fmt.Sprintf("PRAGMA busy_timeout = %d", d.Milliseconds())
		_, err := s.DB.Exec(cmd)
		if err != nil {
			return fmt.Errorf("failed to set busy timeout: %w", err)
		}
		return nil
	}
}

// WithOptimisationEvery sets how many writes trigger a PRAGMA optimize operation.
func WithOptimisationEvery(n int) Option {
	return func(s *Store) error {
		if n < 0 {
			return errors.New("number of write before PRAGMA optimize must be non-negative")
		}
		s.optimizeEvery = int32(n)
		return nil
	}
}

// WithFilterPolicy sets a custom [nastro.FilterPolicy] on the Store.
// It will be used to validate and modify filters before executing queries.
func WithFilterPolicy(p FilterPolicy) Option {
	return func(s *Store) error {
		s.filterPolicy = p
		return nil
	}
}

// WithEventPolicy sets a custom [nastro.EventPolicy] on the Store.
// It will be used to validate events before inserting them into the database.
func WithEventPolicy(p EventPolicy) Option {
	return func(s *Store) error {
		s.eventPolicy = p
		return nil
	}
}

// WithQueryBuilder allows to specify the query builder used by the store in [Store.Query].
func WithQueryBuilder(b QueryBuilder) Option {
	return func(s *Store) error {
		s.queryBuilder = b
		return nil
	}
}

// WithCountBuilder allows to specify the query builder used by the store in [Store.Count].
func WithCountBuilder(b QueryBuilder) Option {
	return func(s *Store) error {
		s.countBuilder = b
		return nil
	}
}

// EventPolicy validates a nostr event before it's written to the store.
type EventPolicy func(*nostr.Event) error

// FilterPolicy sanitizes a list of filters before building a query.
// It returns a potentially modified list and an error if the input is invalid.
type FilterPolicy func(...nostr.Filter) ([]nostr.Filter, error)

// DefaultEventPolicy returns an error if the event has too many tags, or if the
// content is too big.
func defaultEventPolicy(e *nostr.Event) error {
	if len(e.Tags) > 100_000 {
		return fmt.Errorf("event has too many tags: %d", len(e.Tags))
	}
	if len(e.Content) > 10_000_000 {
		return fmt.Errorf("event too much content: %d", len(e.Content))
	}
	return nil
}

// DefaultFilterPolicy enforces 4 rules:
//  1. Filters must be less than 200.
//  2. Filters can't have the "search" field, as NIP-50 is not supported by default.
//  3. Filters with LimitZero set are ignored (i.e., removed).
//  4. Remaining filters must have a Limit > 0.
//
// It returns the cleaned list of filters or an error.
func defaultFilterPolicy(filters ...nostr.Filter) ([]nostr.Filter, error) {
	if len(filters) > 200 {
		return nil, fmt.Errorf("filters must be less than 200: %d", len(filters))
	}

	if containSearch(filters) {
		return nil, errors.New("NIP-50 search is not supported")
	}

	result := make([]nostr.Filter, 0, len(filters))
	for _, f := range filters {
		if !f.LimitZero {
			if f.Limit < 1 {
				return nil, errors.New("unspecified filter's limit")
			}
			result = append(result, f)
		}
	}
	return result, nil
}

func containSearch(filters []nostr.Filter) bool {
	for _, f := range filters {
		if f.Search != "" {
			return true
		}
	}
	return false
}
