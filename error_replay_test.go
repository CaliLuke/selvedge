package selvedge

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/retry"
)

func TestActivityErrorTypePreservedOnReplay(t *testing.T) {
	testActivityErrorTypePreservedOnReplay(t, createTestApp)
}
func testActivityErrorTypePreservedOnReplay(t *testing.T, factory replayAppFactory) {
	t.Helper()
	appErr := NewApplicationError("bad input", "Validation", struct{ Count int }{3}, context.Canceled)
	cases := []struct {
		name string
		err  error
	}{
		{"terminal", NewTerminalError(context.Canceled)},
		{"retry_exhausted", &RetryExhaustedError{ActivityName: "inner", Attempts: 7, LastErr: context.DeadlineExceeded}},
		{"application", appErr},
		{"nonretryable", NewNonRetryableApplicationError("stop", "Stop", nil, context.DeadlineExceeded)},
		{"canceled", context.Canceled}, {"deadline", context.DeadlineExceeded},
		{"workflow_cancelled", &WorkflowCancelledError{InstanceID: "original", Reason: "requested"}},
		{"wrapped_application", fmt.Errorf("outer: %w", appErr)},
	}
	for _, tc := range cases {
		for _, wrapped := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/wrapped=%v", tc.name, wrapped), func(t *testing.T) {
				original := tc.err
				if wrapped {
					original = fmt.Errorf("wrapper: %w", original)
				}
				h := newTypedReplayHarness(t, factory)
				calls := 0
				activity := DefineActivity("failure", func(context.Context, string) (string, error) { calls++; return "", original }, WithRetryPolicy[string, string](retry.Fixed(1, 0)))
				var seen []error
				id := addErrorReplayWorkflow(t, h, activity, &seen)
				require.NoError(t, h.RunOnce(id))
				h.forceFire(t, id)
				require.NoError(t, h.RunOnce(id))
				require.Len(t, seen, 2)
				require.Equal(t, 1, calls)
				assertErrorShape(t, original, seen[0])
				assertErrorShape(t, seen[0], seen[1])
				require.EqualError(t, seen[1], seen[0].Error())
				var app *ApplicationError
				if errors.As(seen[0], &app) && app.Type == "Validation" {
					require.Equal(t, map[string]any{"Count": float64(3)}, app.Details)
				}
			})
		}
	}
}

func assertErrorShape(t *testing.T, expected, actual error) {
	t.Helper()
	require.Error(t, actual)
	require.Equal(t, errors.Is(expected, context.Canceled), errors.Is(actual, context.Canceled))
	require.Equal(t, errors.Is(expected, context.DeadlineExceeded), errors.Is(actual, context.DeadlineExceeded))
	var et, at *TerminalError
	require.Equal(t, errors.As(expected, &et), errors.As(actual, &at))
	var ea, aa *ApplicationError
	require.Equal(t, errors.As(expected, &ea), errors.As(actual, &aa))
	if ea != nil {
		require.Equal(t, ea.Type, aa.Type)
		require.Equal(t, ea.Message, aa.Message)
		require.Equal(t, ea.NonRetryable, aa.NonRetryable)
	}
	var ew, aw *WorkflowCancelledError
	require.Equal(t, errors.As(expected, &ew), errors.As(actual, &aw))
	require.Equal(t, ew, aw)
	var er, ar *RetryExhaustedError
	if errors.As(expected, &er) {
		require.ErrorAs(t, actual, &ar)
		// An activity can add an outer retry-exhausted error. Inspect the matching inner error.
		for ar.ActivityName != er.ActivityName {
			require.ErrorAs(t, ar.LastErr, &ar)
		}
		require.Equal(t, er.Attempts, ar.Attempts)
		if er.LastErr == nil {
			require.Nil(t, ar.LastErr)
		} else {
			require.Equal(t, er.LastErr.Error(), ar.LastErr.Error())
		}
	}
	require.Contains(t, actual.Error(), expected.Error())
}

func addErrorReplayWorkflow(t *testing.T, h *typedReplayHarness, activity *Activity[string, string], seen *[]error) string {
	t.Helper()
	workflow := DefineWorkflow("error_replay", func(ctx *WorkflowContext, input string) (string, error) {
		_, err := activity.Execute(ctx, input, WithActivityID("result"))
		*seen = append(*seen, err)
		return "caught", Sleep(ctx, time.Hour, WithSleepID("pause"))
	})
	return seedErrorWorkflow(t, h, workflow)
}
func seedErrorWorkflow(t *testing.T, h *typedReplayHarness, w Workflow[string, string]) string {
	t.Helper()
	RegisterWorkflow(h.app, w)
	const id = "error-instance"
	require.NoError(t, h.app.storage.CreateInstance(h.app.ctx, &storage.WorkflowInstance{InstanceID: id, WorkflowName: w.Name(), Status: storage.StatusRunning, InputData: []byte(`"input"`)}))
	return id
}

func TestFailedActivityNotRerunOnReplay(t *testing.T) {
	testFailedActivityNotRerunOnReplay(t, createTestApp)
}
func testFailedActivityNotRerunOnReplay(t *testing.T, factory replayAppFactory) {
	t.Helper()
	h := newTypedReplayHarness(t, factory)
	calls := 0
	activity := DefineActivity("failure", func(ctx context.Context, _ string) (string, error) {
		calls++
		require.False(t, h.app.storage.InTransaction(ctx))
		require.NotNil(t, GetWorkflowContext(ctx))
		return "", NewTerminalError(context.Canceled)
	})
	var seen []error
	id := addErrorReplayWorkflow(t, h, activity, &seen)
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, h.RunOnce(id))
	h.forceFire(t, id)
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, 1, calls)
	require.Len(t, seen, 3)
	assertErrorShape(t, seen[0], seen[1])
	assertErrorShape(t, seen[0], seen[2])
}

func TestNonRetryableApplicationErrorNotRetried(t *testing.T) {
	testNonRetryableApplicationErrorNotRetried(t, createTestApp)
}
func testNonRetryableApplicationErrorNotRetried(t *testing.T, factory replayAppFactory) {
	t.Helper()
	h := newTypedReplayHarness(t, factory)
	calls := 0
	activity := DefineActivity("failure", func(context.Context, string) (string, error) {
		calls++
		return "", fmt.Errorf("wrapped: %w", NewNonRetryableApplicationError("stop", "Stop", nil, nil))
	}, WithRetryPolicy[string, string](retry.Fixed(3, 0)))
	var seen []error
	id := addErrorReplayWorkflow(t, h, activity, &seen)
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, 1, calls)
	var app *ApplicationError
	require.ErrorAs(t, seen[0], &app)
	require.True(t, app.NonRetryable)
	var exhausted *RetryExhaustedError
	require.False(t, errors.As(seen[0], &exhausted))
}

func TestWorkflowResultErrorTyped(t *testing.T) { testWorkflowResultErrorTyped(t, createTestApp) }
func testWorkflowResultErrorTyped(t *testing.T, factory replayAppFactory) {
	t.Helper()
	h := newTypedReplayHarness(t, factory)
	w := DefineWorkflow("long_failure", func(ctx *WorkflowContext, _ string) (string, error) {
		filler := DefineActivity("filler", func(context.Context, string) (bool, error) { t.Fatal("recorded filler executed"); return false, nil })
		for i := 0; i < 1001; i++ {
			if _, err := filler.Execute(ctx, "", WithActivityID(fmt.Sprintf("filler:%d", i))); err != nil {
				return "", err
			}
		}
		return "", NewApplicationError("workflow failed", "Validation", map[string]any{"count": 3}, context.Canceled)
	})
	id := seedErrorWorkflow(t, h, w)
	for i := 0; i < 1001; i++ {
		requireRecordedHistory(t, h.app.storage, h.app.ctx, &storage.HistoryEvent{InstanceID: id, ActivityID: fmt.Sprintf("filler:%d", i), Command: "activity:filler", EventType: storage.HistoryActivityCompleted, EventData: []byte(`true`), DataType: "json"})
	}
	require.Error(t, h.RunOnce(id))
	res, err := GetWorkflowResult[string](h.app.ctx, h.app, id)
	require.NoError(t, err)
	require.Equal(t, "failed", res.Status)
	var app *ApplicationError
	require.ErrorAs(t, res.Error, &app)
	require.Equal(t, "Validation", app.Type)
	require.Equal(t, map[string]any{"count": float64(3)}, app.Details)
	require.ErrorIs(t, res.Error, context.Canceled)
	iter := storage.NewHistoryIterator(h.app.ctx, h.app.storage, id, nil)
	rows, err := iter.Collect()
	require.NoError(t, err)
	require.Len(t, rows, 1002)
	require.Equal(t, "$workflow_failed", rows[1001].ActivityID)
}

func TestErrorEnvelopeUnknownKindDecodesGeneric(t *testing.T) {
	testErrorEnvelopeUnknownKindDecodesGeneric(t, createTestApp)
}
func testErrorEnvelopeUnknownKindDecodesGeneric(t *testing.T, factory replayAppFactory) {
	t.Helper()
	for _, payload := range []string{`{"v":1,"kind":"future","Message":"future failure"}`, `{"v":99,"kind":"application","Message":"future failure"}`, `{"v":0,"kind":"application","Message":"future failure"}`} {
		t.Run(payload, func(t *testing.T) {
			h := newTypedReplayHarness(t, factory)
			a := DefineActivity("never", func(context.Context, string) (string, error) { t.Fatal("recorded error reran"); return "", nil })
			var seen []error
			id := addErrorReplayWorkflow(t, h, a, &seen)
			requireRecordedHistory(t, h.app.storage, h.app.ctx, &storage.HistoryEvent{InstanceID: id, ActivityID: "result", Command: "activity:never", EventType: storage.HistoryActivityFailed, EventData: []byte(payload), DataType: "json"})
			require.NoError(t, h.RunOnce(id))
			require.EqualError(t, seen[0], "future failure")
			var app *ApplicationError
			require.False(t, errors.As(seen[0], &app))
		})
	}
}

func TestCorruptFailureFailsRunEvenWhenCaught(t *testing.T) {
	testCorruptFailureFailsRunEvenWhenCaught(t, createTestApp)
}
func testCorruptFailureFailsRunEvenWhenCaught(t *testing.T, factory replayAppFactory) {
	t.Helper()
	for _, payload := range []string{`{`, `"string"`, `null`, `{"v":1,"kind":"application","Cause":4}`} {
		t.Run(payload, func(t *testing.T) {
			h := newTypedReplayHarness(t, factory)
			calls := 0
			a := DefineActivity("never", func(context.Context, string) (string, error) { calls++; return "", nil })
			var caught error
			w := DefineWorkflow("corrupt_failure", func(ctx *WorkflowContext, input string) (string, error) {
				_, caught = a.Execute(ctx, input, WithActivityID("result"))
				_, _ = a.Execute(ctx, input, WithActivityID("later"))
				return "caught", nil
			})
			id := seedErrorWorkflow(t, h, w)
			requireRecordedHistory(t, h.app.storage, h.app.ctx, &storage.HistoryEvent{InstanceID: id, ActivityID: "result", Command: "activity:never", EventType: storage.HistoryActivityFailed, EventData: []byte(payload), DataType: "json"})
			err := h.RunOnce(id)
			var decode *ReplayDecodeError
			require.ErrorAs(t, err, &decode)
			require.Same(t, caught, err)
			require.Equal(t, id, decode.InstanceID)
			require.Equal(t, "result", decode.Key)
			require.Equal(t, 0, calls)
			instance, getErr := h.app.storage.GetInstance(h.app.ctx, id)
			require.NoError(t, getErr)
			require.Equal(t, storage.StatusFailed, instance.Status)
		})
	}
}

func TestLiveActivityUsesStoredOutcome(t *testing.T) {
	testLiveActivityUsesStoredOutcome(t, createTestApp)
}
func testLiveActivityUsesStoredOutcome(t *testing.T, factory replayAppFactory) {
	t.Helper()
	for _, tc := range []struct {
		name    string
		kind    storage.HistoryEventType
		payload string
		liveErr error
		want    string
		failed  bool
	}{
		{"stored_failure_live_success", storage.HistoryActivityFailed, `{"v":1,"kind":"canceled","Message":"context canceled"}`, nil, "", true},
		{"stored_failure_live_failure", storage.HistoryActivityFailed, `{"v":1,"kind":"canceled","Message":"context canceled"}`, NewTerminalError(errors.New("different")), "", true},
		{"stored_success_live_failure", storage.HistoryActivityCompleted, `"winner"`, NewTerminalError(errors.New("loser")), "winner", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTypedReplayHarness(t, factory)
			calls := 0
			a := DefineActivity("competing", func(ctx context.Context, _ string) (string, error) {
				calls++
				wc := GetWorkflowContext(ctx)
				requireRecordedHistory(t, h.app.storage, ctx, &storage.HistoryEvent{InstanceID: wc.InstanceID(), ActivityID: "result", Command: "activity:competing", EventType: tc.kind, EventData: []byte(tc.payload), DataType: "json"})
				return "live", tc.liveErr
			})
			var values []string
			var seen []error
			w := DefineWorkflow("stored_outcome", func(ctx *WorkflowContext, input string) (string, error) {
				value, err := a.Execute(ctx, input, WithActivityID("result"))
				values = append(values, value)
				seen = append(seen, err)
				return "", Sleep(ctx, time.Hour, WithSleepID("pause"))
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			h.forceFire(t, id)
			require.NoError(t, h.RunOnce(id))
			require.Equal(t, 1, calls)
			require.Equal(t, []string{tc.want, tc.want}, values)
			if tc.failed {
				require.ErrorIs(t, seen[0], context.Canceled)
				require.ErrorIs(t, seen[1], context.Canceled)
			} else {
				require.NoError(t, seen[0])
				require.NoError(t, seen[1])
			}
		})
	}
}

func TestLiveCorruptFailureFailsRunEvenWhenCaught(t *testing.T) {
	testLiveCorruptFailureFailsRunEvenWhenCaught(t, createTestApp)
}
func testLiveCorruptFailureFailsRunEvenWhenCaught(t *testing.T, factory replayAppFactory) {
	t.Helper()
	h := newTypedReplayHarness(t, factory)
	calls := 0
	activity := DefineActivity("corrupt_live", func(ctx context.Context, _ string) (string, error) {
		calls++
		requireRecordedHistory(t, h.app.storage, ctx, &storage.HistoryEvent{
			InstanceID: GetWorkflowContext(ctx).InstanceID(), ActivityID: "result", Command: "activity:corrupt_live", EventType: storage.HistoryActivityFailed, EventData: []byte(`{`), DataType: "json",
		})
		return "live success", nil
	}, WithRetryPolicy[string, string](retry.Fixed(3, 0)))
	var caught error
	workflow := DefineWorkflow("live_corrupt_failure", func(ctx *WorkflowContext, input string) (string, error) {
		_, caught = activity.Execute(ctx, input, WithActivityID("result"))
		return "caught", nil
	})
	id := seedErrorWorkflow(t, h, workflow)
	err := h.RunOnce(id)
	var decodeErr *ReplayDecodeError
	require.ErrorAs(t, err, &decodeErr)
	require.Same(t, caught, err)
	require.Equal(t, id, decodeErr.InstanceID)
	require.Equal(t, "result", decodeErr.Key)
	require.Equal(t, 1, calls)
	instance, getErr := h.app.storage.GetInstance(h.app.ctx, id)
	require.NoError(t, getErr)
	require.Equal(t, storage.StatusFailed, instance.Status)
	require.NoError(t, h.RunOnce(id)) // A terminal instance is not dispatched again.
	require.Equal(t, 1, calls)
}
