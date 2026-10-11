package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteConcurrentFirstOpen(t *testing.T) {
	const pools = 16
	path := filepath.Join(t.TempDir(), "fresh.db")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := make(chan struct{})
	results := make(chan error, pools)
	for range pools {
		go func() {
			<-start
			s, err := NewSQLiteStorage(path)
			if err != nil {
				results <- err
				return
			}
			t.Cleanup(func() { _ = s.Close() })
			// No pool is opened before the barrier, and none is pre-pinged.
			var mode string
			err = s.DB().QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode)
			if err == nil && mode != "wal" {
				t.Errorf("journal mode = %q, want wal", mode)
			}
			results <- err
		}()
	}
	close(start)
	for range pools {
		if err := <-results; err != nil {
			t.Errorf("concurrent first open: %v", err)
		}
	}
}
