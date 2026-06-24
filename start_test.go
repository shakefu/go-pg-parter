package parter

import (
	"context"
	"testing"
)

// didPanic runs fn and reports whether it panicked. It is the recover-based
// assertion helper for Start's panic contract.
func didPanic(fn func()) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
		}
	}()
	fn()
	return false
}

// start.yass.yaml parter.Start ERROR (Config invalid): Start panics, applying
// the same conditions as the in-memory validation (CONFORMS
// ValidateConfig::ERROR). Validation runs first, so no DB is needed: a nil Pool
// (and, separately, empty Tables) must panic before any DB work.
func TestStartPanicsOnInvalidConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		// nil Pool — fails the very first ValidateConfig check.
		{"nil Pool", Config{}},
		// Empty Tables — also invalid; uses a non-nil dummy Pool so the failure
		// is specifically the empty-Tables condition, not the nil-Pool one.
		{"empty Tables", Config{Pool: dummyPool(), Tables: nil}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !didPanic(func() { Start(context.Background(), tc.cfg) }) {
				t.Fatalf("Start did not panic on invalid config (%s)", tc.name)
			}
		})
	}
}
