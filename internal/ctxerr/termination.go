// Package ctxerr classifies context termination without hiding joined failures.
package ctxerr

import (
	"context"
	"errors"
)

// IsTermination reports whether every cause in err matches ctx's termination.
// An active context, nil error, or any unrelated joined cause returns false.
func IsTermination(ctx context.Context, err error) bool {
	cause := ctx.Err()
	return cause != nil && matchesOnly(err, cause)
}

func matchesOnly(err, cause error) bool {
	if err == nil {
		return false
	}
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		children := wrapped.Unwrap()
		found := false
		for _, child := range children {
			if child == nil {
				continue
			}
			if !matchesOnly(child, cause) {
				return false
			}
			found = true
		}
		return found
	case interface{ Unwrap() error }:
		return matchesOnly(wrapped.Unwrap(), cause)
	default:
		return errors.Is(err, cause)
	}
}
