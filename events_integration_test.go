//go:build integration

package selvedge

import "testing"

func TestPostgresIntegration_WorkflowWithWaitEvent(t *testing.T) {
	testWorkflowWithWaitEvent(t, createTestAppWithPostgres)
}
