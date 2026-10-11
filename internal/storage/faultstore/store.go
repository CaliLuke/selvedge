// Package faultstore provides storage fault injection for engine tests.
package faultstore

import (
	"context"
	"sync"

	"github.com/CaliLuke/selvedge/internal/storage"
)

// Hook can block a call or return an injected crash. Configure before use.
type Hook func(context.Context, string) error

// Store decorates storage with hooks and an ordered call log.
// An After error represents a lost acknowledgement after the durable write.
type Store struct {
	storage.Storage
	Before, After Hook
	mu            sync.Mutex
	calls         []string
	statements    []Statement
}

func (s *Store) before(ctx context.Context, method string) error {
	s.mu.Lock()
	s.calls = append(s.calls, method)
	s.mu.Unlock()
	if s.Before != nil {
		return s.Before(ctx, method)
	}
	return nil
}
func (s *Store) after(ctx context.Context, method string, err error) error {
	if err != nil {
		return err
	}
	if s.After != nil {
		return s.After(ctx, method)
	}
	return nil
}

// Calls returns a snapshot of method names in call order.
func (s *Store) Calls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

// Statement is one observed SQL operation, including its bound arguments.
type Statement struct {
	SQL  string
	Args []any
}

// New installs SQL observation and wraps storage for fault injection. Call before concurrent use.
func New(s storage.Storage) *Store {
	f := &Store{Storage: s}
	storage.WithTestStatementObserver(func(q string, args []any) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.statements = append(f.statements, Statement{q, append([]any(nil), args...)})
	})(s)
	return f
}

// Statements returns a snapshot of observed SQL in execution order.
func (s *Store) Statements() []Statement {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Statement(nil), s.statements...)
}
