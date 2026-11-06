package testkit

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestPostgresQueryPreservesParametersWhenEditingURLs(t *testing.T) {
	for _, test := range []struct{ raw, want string }{
		{"space%20name", "space name"},
		{"literal+plus", "literal+plus"},
		{"encoded%2Bplus", "encoded+plus"},
	} {
		t.Run(test.raw, func(t *testing.T) {
			values, err := PostgresQuery("application_name=" + test.raw)
			require.NoError(t, err)
			values.Set("default_transaction_isolation", "repeatable read")
			config, err := pgx.ParseConfig("postgres://user@localhost/database?" + EncodePostgresQuery(values))
			require.NoError(t, err)
			require.Equal(t, test.want, config.RuntimeParams["application_name"])
			require.Equal(t, "repeatable read", config.RuntimeParams["default_transaction_isolation"])
		})
	}
	_, err := PostgresQuery("application_name=%ZZ")
	require.Error(t, err)
}
