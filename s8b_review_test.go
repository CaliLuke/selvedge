package selvedge

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/replay"
)

func TestFutureGetWaitUsesCurrentPosition(t *testing.T) {
	testFutureGetWaitUsesCurrentPosition(t, createTestApp)
}
func testFutureGetWaitUsesCurrentPosition(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	var waits []replay.Wait
	var position int64
	h.app.ctx = replay.WithTestYieldObserver(h.app.ctx, func(w replay.Wait) { waits = append(waits, w) })
	a := DefineActivity("A", func(context.Context, string) (string, error) { t.Error("held task ran"); return "", nil })
	w := DefineWorkflow("wait_position", func(ctx *WorkflowContext, in string) (string, error) {
		f, err := a.ExecuteAsync(ctx, in)
		if err != nil {
			return "", err
		}
		if err := Sleep(ctx, 0); err != nil {
			return "", err
		}
		position = ctx.execCtx.Position()
		return f.Get(ctx)
	})
	id := seedErrorWorkflow(t, h, w)
	seedTask(t, h, id)
	claim, err := h.ClaimTask(id, "#1")
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.NoError(t, h.RunOnce(id))
	require.EqualValues(t, 2, position)
	require.Equal(t, []replay.Wait{{Seq: position, Key: "#1", Kind: replay.WaitActivity}}, waits)
}

func TestFutureFailureSurfacesAtGetLiveAndReplay(t *testing.T) {
	testFutureFailureSurfacesAtGetLiveAndReplay(t, createTestApp)
}
func testFutureFailureSurfacesAtGetLiveAndReplay(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"panic", "decode"} {
		t.Run(kind, func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			a := DefineActivity("A", func(context.Context, string) (string, error) {
				if kind == "panic" {
					panic("boom")
				}
				return "value", nil
			})
			// A worker can store valid JSON that does not match the future's Go type.
			// Complete a harness claim during Get's read on the first execution.
			var claim *harnessTaskClaim
			var issueErrors, getErrors, afterIssueErrors []error
			var futurePresent []bool
			w := DefineWorkflow("failure_future", func(ctx *WorkflowContext, in string) (string, error) {
				f, err := a.ExecuteAsync(ctx, in)
				issueErrors = append(issueErrors, err)
				futurePresent = append(futurePresent, f != nil)
				if err != nil {
					return "", err
				}
				// Issuing another command must not surface a future failure before Get.
				err = Sleep(ctx, 0)
				afterIssueErrors = append(afterIssueErrors, err)
				if err != nil {
					return "", err
				}
				_, err = f.Get(ctx)
				getErrors = append(getErrors, err)
				return "ignored", nil
			})
			id := seedErrorWorkflow(t, h, w)
			if kind == "decode" {
				seedTask(t, h, id)
				var err error
				claim, err = h.ClaimTask(id, "#1")
				require.NoError(t, err)
				require.NotNil(t, claim)
				fs.Before = func(_ context.Context, method string) error {
					if method == "GetHistoryEvent" {
						return claim.Complete(`123`)
					}
					return nil
				}
			}
			first := h.RunOnce(id)
			fs.Before = nil
			_, err := fs.DB().Exec("UPDATE workflow_instances SET status='running',runnable=TRUE WHERE instance_id=$1", id)
			require.NoError(t, err)
			// Failed-run lifecycle rows do not participate in command replay.
			second := h.RunOnce(id)
			require.Equal(t, []bool{true, true}, futurePresent)
			require.Equal(t, []error{nil, nil}, issueErrors)
			require.Equal(t, []error{nil, nil}, afterIssueErrors)
			require.Len(t, getErrors, 2)
			for _, err := range append(getErrors, first, second) {
				if kind == "panic" {
					var target *WorkflowPanicError
					require.ErrorAs(t, err, &target)
				} else {
					var target *ReplayDecodeError
					require.ErrorAs(t, err, &target)
				}
			}
		})
	}
}
