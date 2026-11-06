package sqliteutil

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestWithoutBusyTimeoutPreservesPooledConnectionSetting(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		name := "success"
		if cancelled {
			name = "cancelled"
		}
		t.Run(name, func(t *testing.T) {
			db, err := sql.Open("sqlite", ":memory:?_pragma=busy_timeout(1234)")
			require.NoError(t, err)
			defer db.Close()
			db.SetMaxOpenConns(1)
			conn, err := db.Conn(context.Background())
			require.NoError(t, err)
			defer conn.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			err = WithoutBusyTimeout(ctx, conn, func() error {
				if cancelled {
					cancel()
				}
				return ctx.Err()
			})
			if cancelled {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, conn.Close())
			var timeout int
			require.NoError(t, db.QueryRow("PRAGMA busy_timeout").Scan(&timeout))
			require.Equal(t, 1234, timeout, "later operations must retain the connection's original lock-wait behavior")
		})
	}
}
