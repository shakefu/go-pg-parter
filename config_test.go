package parter

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// dummyPool is a non-nil *pgxpool.Pool used only to satisfy the Pool!=nil check
// in ValidateConfig. ValidateConfig MUST NOT touch the DB, so this pool is
// never connected.
func dummyPool() *pgxpool.Pool { return &pgxpool.Pool{} }

func validNamer(t time.Time) string { return t.Format("2006_01") }

// validConfig returns a Config that passes ValidateConfig, for mutation in the
// rejection matrix.
func validConfig() Config {
	return Config{
		Pool: dummyPool(),
		Tables: []Table{{
			Name:      "contacts.contact_state_events",
			Strategy:  Monthly,
			Lookahead: 3,
			Retention: 6,
			Namer:     validNamer,
		}},
		Schema:   "parter",
		Instance: nil,
	}
}

// config.yass.yaml parter.RangeStrategy INPUT/ERROR: valid set is
// {Monthly=0, Weekly=1, Daily=2}; anything else is invalid.
func TestRangeStrategyValid(t *testing.T) {
	cases := []struct {
		s    RangeStrategy
		want bool
	}{
		{Monthly, true},
		{Weekly, true},
		{Daily, true},
		{RangeStrategy(-1), false},
		{RangeStrategy(3), false},
		{RangeStrategy(99), false},
	}
	if Monthly != 0 || Weekly != 1 || Daily != 2 {
		t.Fatalf("enum values wrong: Monthly=%d Weekly=%d Daily=%d", Monthly, Weekly, Daily)
	}
	for _, c := range cases {
		if got := c.s.valid(); got != c.want {
			t.Errorf("RangeStrategy(%d).valid() = %v, want %v", c.s, got, c.want)
		}
	}
}

// config.yass.yaml parter.Config: schema() defaults to "parter".
func TestConfigSchemaDefault(t *testing.T) {
	if got := (Config{}).schema(); got != "parter" {
		t.Errorf("empty Schema schema() = %q, want %q", got, "parter")
	}
	if got := (Config{Schema: "custom"}).schema(); got != "custom" {
		t.Errorf("Schema=custom schema() = %q, want %q", got, "custom")
	}
}

// config.yass.yaml parter.ValidateConfig: a valid config passes.
func TestValidateConfigValid(t *testing.T) {
	if err := ValidateConfig(validConfig()); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	// A nil-Instance config and an Instance=0 config are both valid.
	c := validConfig()
	c.Instance = new(0)
	if err := ValidateConfig(c); err != nil {
		t.Fatalf("Instance=0 rejected: %v", err)
	}
}

// config.yass.yaml parter.ValidateConfig ERROR: the full rejection matrix.
// Each MUST-be-rejected condition is one sub-case.
func TestValidateConfigRejectionMatrix(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(c *Config)
	}{
		{"Pool nil", func(c *Config) { c.Pool = nil }},
		{"Tables empty", func(c *Config) { c.Tables = nil }},
		{"Name empty", func(c *Config) { c.Tables[0].Name = "" }},
		{"Namer nil", func(c *Config) { c.Tables[0].Namer = nil }},
		{"Lookahead 0", func(c *Config) { c.Tables[0].Lookahead = 0 }},
		{"Lookahead negative", func(c *Config) { c.Tables[0].Lookahead = -1 }},
		{"Retention negative", func(c *Config) { c.Tables[0].Retention = -1 }},
		{"Strategy invalid", func(c *Config) { c.Tables[0].Strategy = RangeStrategy(7) }},
		{"Instance negative", func(c *Config) { c.Instance = new(-1) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfig()
			tc.mutate(&c)
			if err := ValidateConfig(c); err == nil {
				t.Fatalf("ValidateConfig accepted invalid config (%s)", tc.name)
			}
		})
	}
}

// CONTRACT §13 / config.yass.yaml ValidateConfig: validation is in-memory and
// MUST NOT touch the DB. We assert by passing a Pool that would panic on any
// use; a clean return proves no DB access. The dummy pool here is the
// uninitialized struct — any method call would dereference nil internals and
// panic, so a non-panicking valid run proves no DB call.
func TestValidateConfigDoesNotTouchDB(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("ValidateConfig touched the DB (panic: %v)", r)
		}
	}()
	if err := ValidateConfig(validConfig()); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

// CONTRACT §1 / config.yass.yaml Table RETURN: parent schema is extracted from
// Table.Name (before the first dot).
func TestParentSchema(t *testing.T) {
	cases := []struct {
		name       string
		wantSchema string
		wantRel    string
	}{
		{"contacts.contact_state_events", "contacts", "contact_state_events"},
		{"public.events", "public", "events"},
		{"events", "public", "events"}, // no dot -> default search path schema
		{"a.b.c", "a", "b.c"},          // split on FIRST dot
	}
	for _, c := range cases {
		gotSchema, gotRel := parentSchema(c.name)
		if gotSchema != c.wantSchema || gotRel != c.wantRel {
			t.Errorf("parentSchema(%q) = (%q,%q), want (%q,%q)",
				c.name, gotSchema, gotRel, c.wantSchema, c.wantRel)
		}
	}
}
