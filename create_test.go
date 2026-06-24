package parter

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// create.yass.yaml parter.CreatePass ERROR / parter.CircuitBreaker RETURN:
// classifyCreateError returns transient=true ONLY for connection loss,
// lock_not_available (55P03), and serialization_failure (40001); every other
// error (including check_violation 23514 and the attach/boundary sentinel) is
// deterministic so it advances the breaker.
func TestClassifyCreateError(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		transient bool
	}{
		// create.yass.yaml ERROR: lock_not_available is transient.
		{"lock_not_available_55P03", &pgconn.PgError{Code: "55P03"}, true},
		// create.yass.yaml ERROR: serialization_failure is transient.
		{"serialization_failure_40001", &pgconn.PgError{Code: "40001"}, true},
		// create.yass.yaml RETURN / ERROR: check_violation 23514 (non-empty
		// DEFAULT holds rows in range) is DETERMINISTIC and counts toward the
		// 5-attempt budget.
		{"check_violation_23514", &pgconn.PgError{Code: "23514"}, false},
		// A generic non-PgError is deterministic (e.g. invalid range surfaced
		// outside SQLSTATE, programmer error).
		{"generic_error", errors.New("boom"), false},
		// create.yass.yaml SIDE-EFFECT: the attach/boundary verification
		// sentinel is modeled as a non-PgError and is deterministic.
		{"attach_not_attached_sentinel", errNotAttached, false},
		{"boundary_mismatch_sentinel", errBoundaryMismatch, false},
		// A wrapped attach sentinel is still deterministic.
		{"wrapped_attach_sentinel", errors.New("verify: " + errNotAttached.Error()), false},
		// A PgError with an undefined_table code (parent missing) is
		// deterministic.
		{"undefined_table_42P01", &pgconn.PgError{Code: "42P01"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyCreateError(tc.err); got != tc.transient {
				t.Errorf("classifyCreateError(%v) transient = %v, want %v", tc.err, got, tc.transient)
			}
		})
	}
}

// create.yass.yaml parter.CreatePass ERROR: a wrapped transient PgError is
// still classified transient (errors.As unwraps).
func TestClassifyCreateErrorWrapped(t *testing.T) {
	wrapped := errors.Join(errors.New("during create"), &pgconn.PgError{Code: "40001"})
	if !classifyCreateError(wrapped) {
		t.Errorf("wrapped serialization_failure should classify transient")
	}
}

// create.yass.yaml parter.CreatePass ERROR: context cancellation and deadline
// are handled as cancellation (exit pass, leave row pending, no breaker), NOT
// routed through classifyCreateError. isCtxError recognizes both.
func TestIsCtxError(t *testing.T) {
	if !isCtxError(context.Canceled) {
		t.Error("context.Canceled should be recognized as a ctx error")
	}
	if !isCtxError(context.DeadlineExceeded) {
		t.Error("context.DeadlineExceeded should be recognized as a ctx error")
	}
	if !isCtxError(errors.Join(errors.New("wrap"), context.Canceled)) {
		t.Error("wrapped context.Canceled should be recognized as a ctx error")
	}
	if isCtxError(&pgconn.PgError{Code: "23514"}) {
		t.Error("a PgError must not be recognized as a ctx error")
	}
	if isCtxError(errNotAttached) {
		t.Error("the attach sentinel must not be recognized as a ctx error")
	}
}
