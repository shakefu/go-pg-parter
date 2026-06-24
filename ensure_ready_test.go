package parter

import (
	"context"
	"testing"
)

// ensure-ready.yass.yaml parter.EnsureReady ERROR (Config invalid per the
// in-memory validation conditions) + RETURN ("validate Config first, before
// any other work"). EnsureReady must short-circuit at step 1 (ValidateConfig)
// and return a non-nil error BEFORE touching the Pool, so an empty/nil-Pool
// Config returns an error without any DB access.
//
// The zero Config has a nil Pool, which ValidateConfig rejects. If EnsureReady
// reached any DB step it would dereference the nil Pool and panic; a clean
// non-nil error return therefore proves no DB work occurred.
func TestEnsureReadyValidateConfigShortCircuit(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("EnsureReady performed DB work before validation (panic: %v)", r)
		}
	}()

	err := EnsureReady(context.Background(), Config{})
	if err == nil {
		t.Fatal("EnsureReady(Config{}) returned nil, want a validation error")
	}

	// Explicit nil Pool case (equivalent short-circuit path).
	err = EnsureReady(context.Background(), Config{Pool: nil})
	if err == nil {
		t.Fatal("EnsureReady(Config{Pool: nil}) returned nil, want a validation error")
	}
}
