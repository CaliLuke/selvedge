package selvedge

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkflowContextStorageHasNoDatabaseAccess(t *testing.T) {
	method, ok := reflect.TypeFor[*WorkflowContext]().MethodByName("Storage")
	require.True(t, ok)
	api := method.Type.Out(0)
	for _, forbidden := range []string{"Conn", "DB", "Close", "BeginTransaction", "RecordHistory", "CreateInstance"} {
		_, exposed := api.MethodByName(forbidden)
		require.False(t, exposed, "WorkflowContext.Storage exposes %s", forbidden)
	}
}
