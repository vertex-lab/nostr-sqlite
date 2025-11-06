package sqlite

import (
	"errors"
	"fmt"

	"github.com/nbd-wtf/go-nostr"
)

var (
	ErrUnspecifiedLimit  = errors.New("unspecified filter's limit")
	ErrUnsupportedSearch = errors.New("NIP-50 search is not supported")
)

type Option func(*Store) error

// WithRetries sets how many times to retry a locked database operation
// after the first failed attempt. Each retry waits 20ms + jitter (~5ms on average).
func WithRetries(n int) Option {
	return func(s *Store) error {
		if n < 0 {
			return errors.New("number of retries must be non-negative")
		}
		s.retries = n
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

// EventPolicy validates a nostr event before it's written to the store.
type EventPolicy func(*nostr.Event) error

// FilterPolicy sanitizes a list of filters before building a query.
// It returns a potentially modified list and an error if the input is invalid.
type FilterPolicy func(...nostr.Filter) (nostr.Filters, error)

// DefaultEventPolicy never returns an error
func defaultEventPolicy(event *nostr.Event) error {
	return nil
}

// DefaultFilterPolicy is a basic filter policy that enforces two rules:
//  1. Filters with LimitZero set are ignored (i.e., removed).
//  2. Remaining filters must have a Limit > 0, otherwise an error is returned.
//
// It returns the cleaned list of filters or an error if any filter is invalid.
func defaultFilterPolicy(filters ...nostr.Filter) (nostr.Filters, error) {
	result := make([]nostr.Filter, 0, len(filters))
	for _, f := range filters {
		if f.Search != "" {
			return nil, ErrUnsupportedSearch
		}

		if !f.LimitZero {
			if f.Limit < 1 {
				return nil, ErrUnspecifiedLimit
			}
			result = append(result, f)
		}
	}
	return result, nil
}
