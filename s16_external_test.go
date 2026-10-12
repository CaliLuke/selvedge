package selvedge_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge"
)

// This example compiles using only the public package. Register workflows and
// start app before calling it with stored instances.
func ExampleApp_FindInstancesWithOptions() {
	ctx := context.Background()
	app := selvedge.NewApp()
	instances, err := app.FindInstancesWithOptions(ctx, selvedge.FindInstancesOptions{
		StatusFilter: selvedge.StatusRunning,
		Limit:        100,
	})
	if err != nil {
		return
	}
	for _, instance := range instances {
		if err := selvedge.ReplayInstance(ctx, app, instance.InstanceID); err != nil {
			return
		}
	}
}

func TestParseHistoryFormatDiscriminator(t *testing.T) {
	for _, data := range []string{
		`{"format":"selvedge.history/v2","new_field":true}`,
		`{"format":"other/v1","instance":{"new_field":true}}`,
	} {
		t.Run(data, func(t *testing.T) {
			_, err := selvedge.ParseHistory([]byte(data))
			require.ErrorIs(t, err, selvedge.ErrUnsupportedHistoryFormat)
		})
	}
	const valid = `{"format":"selvedge.history/v1","instance":{"instance_id":"i","workflow_name":"w","status":"running"},"events":[],"signals":[]}`
	_, err := selvedge.ParseHistory([]byte(valid))
	require.NoError(t, err)
	for _, data := range []string{
		strings.Replace(valid, `"format":`, `"new_field":true,"format":`, 1),
		strings.Replace(valid, `"instance_id":`, `"new_field":true,"instance_id":`, 1),
		strings.Replace(valid, `"events":[]`, `"events":true`, 1),
		valid + ` {}`,
	} {
		_, err := selvedge.ParseHistory([]byte(data))
		require.Error(t, err)
		require.NotErrorIs(t, err, selvedge.ErrUnsupportedHistoryFormat)
	}
}
