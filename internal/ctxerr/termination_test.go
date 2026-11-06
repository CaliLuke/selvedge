package ctxerr

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type multiError []error

func (e multiError) Error() string   { return "multiple errors" }
func (e multiError) Unwrap() []error { return e }

func TestIsTerminationPreservesUnrelatedCauses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	failure := errors.New("rollback failed")
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"cancelled", context.Canceled, true},
		{"wrapped", fmt.Errorf("query: %w", context.Canceled), true},
		{"all cancelled", errors.Join(context.Canceled, fmt.Errorf("cleanup: %w", context.Canceled)), true},
		{"mixed", errors.Join(context.Canceled, failure), false},
		{"mixed reversed", errors.Join(failure, context.Canceled), false},
		{"wrapped mixed", fmt.Errorf("transaction: %w", errors.Join(context.Canceled, failure)), false},
		{"unrelated", failure, false},
		{"other context cause", context.DeadlineExceeded, false},
		{"empty tree", multiError{}, false},
		{"nil child", multiError{nil, context.Canceled}, true},
		{"nil wrap", fmt.Errorf("invalid: %w", error(nil)), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, IsTermination(ctx, test.err))
		})
	}
	require.False(t, IsTermination(context.Background(), context.Canceled))
	deadline, stop := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer stop()
	require.True(t, IsTermination(deadline, fmt.Errorf("query: %w", context.DeadlineExceeded)))
}
