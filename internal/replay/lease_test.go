package replay

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/internal/testkit"
)

type delayedRenewal struct {
	storage.Storage
	clock *testkit.Clock
}

func (s delayedRenewal) RenewLease(context.Context, storage.Fence, time.Duration) (bool, error) {
	s.clock.Advance(801 * time.Millisecond)
	return true, nil
}

func TestExecutionAllowsExactMargin(t *testing.T) {
	clk := &testkit.Clock{}
	x := &Execution{clk: clk, grantedAt: clk.Now(), ttl: time.Second, margin: 200 * time.Millisecond}
	clk.Advance(800 * time.Millisecond)
	require.NoError(t, x.Check())
	clk.Advance(time.Millisecond)
	require.ErrorIs(t, x.Check(), ErrLeaseLost)
}
func TestRenewalBudgetStartsBeforeRequest(t *testing.T) {
	clk := &testkit.Clock{}
	x := &Execution{clk: clk, grantedAt: clk.Now(), ttl: time.Second, margin: 200 * time.Millisecond}
	x.renew(context.Background(), delayedRenewal{clock: clk})
	require.ErrorIs(t, x.Check(), ErrLeaseLost)
}
