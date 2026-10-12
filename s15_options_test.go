package selvedge

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/retry"
)

func TestActivityOptionsResolution(t *testing.T) { testActivityOptionsResolution(t, createTestApp) }
func testActivityOptionsResolution(t *testing.T, factory replayAppFactory) {
	for _, tc := range []struct {
		name string
		opts ActivityOptions
	}{
		{"negative start", ActivityOptions{StartToCloseTimeout: -1}},
		{"negative schedule", ActivityOptions{ScheduleToStartTimeout: -1}},
		{"negative heartbeat", ActivityOptions{HeartbeatTimeout: -1}},
		{"negative attempts", ActivityOptions{RetryPolicy: &retry.Policy{MaxAttempts: -1}}},
		{"negative interval", ActivityOptions{RetryPolicy: &retry.Policy{InitialInterval: -1}}},
		{"negative max interval", ActivityOptions{RetryPolicy: &retry.Policy{MaxInterval: -1}}},
		{"nan", ActivityOptions{RetryPolicy: &retry.Policy{Multiplier: math.NaN()}}},
		{"queue", ActivityOptions{Queue: strings.Repeat("x", 256)}},
		{"errors", ActivityOptions{RetryPolicy: &retry.Policy{NonRetryableErrors: []error{errors.New("bad")}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTypedReplayHarness(t, factory)
			a := DefineActivity("invalid", func(context.Context, string) (string, error) { t.Error("invalid options ran"); return "", nil })
			w := DefineWorkflow("invalid", func(ctx *WorkflowContext, in string) (string, error) {
				_, _ = a.ExecuteAsync(ctx, in, WithActivityOptions(tc.opts))
				return "ignored", nil
			})
			id := seedErrorWorkflow(t, h, w)
			require.Error(t, h.RunOnce(id))
			require.Zero(t, taskCount(t, h, id))
			var n int
			require.NoError(t, h.app.storage.DB().QueryRow("SELECT count(*) FROM workflow_history WHERE instance_id=$1 AND command LIKE 'activity:%'", id).Scan(&n))
			require.Zero(t, n)
		})
	}
}
func TestActivityOptionsRecorded(t *testing.T) { testActivityOptionsRecorded(t, createTestApp) }
func testActivityOptionsRecorded(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	var observed []ActivityOptions
	a := DefineActivity("options", func(ctx context.Context, _ string) (string, error) {
		info := GetActivityInfo(ctx)
		observed = append(observed, info.Options)
		info.Options.RetryPolicy.MaxAttempts = 99
		return "ok", nil
	}, WithActivityDefaults[string, string](ActivityOptions{Queue: "a", StartToCloseTimeout: time.Hour, RetryPolicy: &retry.Policy{MaxAttempts: 2, InitialInterval: time.Second, Multiplier: .5, RandomizationFactor: 2}}))
	w := DefineWorkflow("options", func(ctx *WorkflowContext, in string) (string, error) {
		if _, err := a.Execute(ctx, in); err != nil {
			return "", err
		}
		if _, err := a.Execute(ctx, in, WithActivityOptions(ActivityOptions{Queue: "b", HeartbeatTimeout: 2 * time.Hour, RetryPolicy: &retry.Policy{MaxAttempts: 3}})); err != nil {
			return "", err
		}
		return a.Execute(ctx, in, WithActivityOptions(ActivityOptions{ScheduleToStartTimeout: time.Minute}))
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	tasks, err := ListActivityTasks(h.app.ctx, h.app, id)
	require.NoError(t, err)
	require.Len(t, tasks, 3)
	require.Equal(t, "a", tasks[2].Queue)
	require.Equal(t, time.Minute, tasks[2].Options.ScheduleToStartTimeout)
	require.Equal(t, 2, tasks[2].Options.RetryPolicy.MaxAttempts)
	require.Equal(t, "a", tasks[0].Queue)
	require.Equal(t, "b", tasks[1].Queue)
	require.Equal(t, int64(1), tasks[0].Seq)
	require.Equal(t, int64(2), tasks[1].Seq)
	require.Equal(t, 2, tasks[0].Options.RetryPolicy.MaxAttempts)
	require.Equal(t, 3, tasks[1].Options.RetryPolicy.MaxAttempts)
	require.Equal(t, 100*time.Second, tasks[0].Options.RetryPolicy.MaxInterval)
	require.Equal(t, float64(1), tasks[0].Options.RetryPolicy.Multiplier)
	require.Equal(t, float64(1), tasks[0].Options.RetryPolicy.RandomizationFactor)
	require.Equal(t, time.Hour, tasks[1].Options.HeartbeatTimeout)
	require.Equal(t, time.Hour, observed[1].HeartbeatTimeout)
	_, err = ListActivityTasks(h.app.ctx, h.app, "missing")
	var nf *WorkflowNotFoundError
	require.ErrorAs(t, err, &nf)
	_, err = ListActivityTasks(h.app.ctx, NewApp(), id)
	require.ErrorIs(t, err, ErrAppNotStarted)
}

func TestActivityOptionsNormalized(t *testing.T) { testActivityOptionsNormalized(t, createTestApp) }
func testActivityOptionsNormalized(t *testing.T, factory replayAppFactory) {
	for _, tc := range []struct {
		name   string
		policy *retry.Policy
		want   *retry.Policy
	}{
		{"default", nil, retry.DefaultPolicy()},
		{"empty", &retry.Policy{}, &retry.Policy{InitialInterval: 100 * time.Millisecond, MaxInterval: 10 * time.Second, Multiplier: 1}},
		{"negative_jitter", &retry.Policy{RandomizationFactor: -1}, &retry.Policy{InitialInterval: 100 * time.Millisecond, MaxInterval: 10 * time.Second, Multiplier: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTypedReplayHarness(t, factory)
			a := DefineActivity("normalize", func(context.Context, string) (string, error) { return "done", nil }, WithRetryPolicy[string, string](tc.policy))
			w := DefineWorkflow("normalize", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			rows, err := ListActivityTasks(h.app.ctx, h.app, id)
			require.NoError(t, err)
			require.Equal(t, tc.want, rows[0].Options.RetryPolicy)
		})
	}
}
