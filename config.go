package parter

import (
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RangeStrategy denotes the partition period unit, which determines both
// period-boundary truncation and retention-interval conversion.
//
// config.yass.yaml parter.RangeStrategy: int enum declared with iota,
// Monthly=0, Weekly=1, Daily=2.
type RangeStrategy int

const (
	// Monthly partitions span one calendar month.
	Monthly RangeStrategy = iota
	// Weekly partitions span one ISO week (Monday start).
	Weekly
	// Daily partitions span one calendar day.
	Daily
)

// valid reports whether s is one of the defined RangeStrategy values.
func (s RangeStrategy) valid() bool {
	return s >= Monthly && s <= Daily
}

// Table configures one managed parent partitioned table.
//
// config.yass.yaml parter.Table.
type Table struct {
	// Name is the fully-qualified parent table name, e.g.
	// "contacts.contact_state_events". Required and non-empty.
	Name string
	// Strategy is the partition period unit.
	Strategy RangeStrategy
	// Lookahead is the count of periods to pre-create ahead of now; >= 1.
	Lookahead int
	// Retention is the count of periods after which partitions are dropped;
	// 0 means never drop and skips the retention pass entirely.
	Retention int
	// Namer receives the range start time and returns the unqualified
	// partition name. Required and non-nil.
	Namer func(rangeStart time.Time) string
}

// Config is the argument bundle for both Start and EnsureReady.
//
// config.yass.yaml parter.Config.
type Config struct {
	// Pool is the pgx connection pool; non-nil.
	Pool *pgxpool.Pool
	// Tables is the set of managed tables; at least one entry.
	Tables []Table
	// Schema names the metadata table's schema, defaulting to "parter".
	Schema string
	// Instance is an optional instance ordinal; nil means not provided. When
	// set it must be >= 0.
	Instance *int
}

// schema returns Config.Schema, or "parter" when empty.
func (c Config) schema() string {
	if c.Schema == "" {
		return "parter"
	}
	return c.Schema
}

// parentSchema splits a fully-qualified table name into its parent schema and
// relation. It splits on the FIRST dot (schema=before, rel=after). When Name
// has no dot, the relation lives in the default search path, reported here as
// schema "public".
//
// CONTRACT §1.
func parentSchema(name string) (schema, rel string) {
	if schema, rel, found := strings.Cut(name, "."); found {
		return schema, rel
	}
	return "public", name
}

// ValidateConfig validates a Config in memory before any database operation.
// It returns a non-nil descriptive error naming the offending field, or nil
// when valid. It MUST NOT perform the DEFAULT-partition catalog check.
//
// config.yass.yaml parter.ValidateConfig.
func ValidateConfig(cfg Config) error {
	if cfg.Pool == nil {
		return fmt.Errorf("parter: Config.Pool must be non-nil")
	}
	if len(cfg.Tables) == 0 {
		return fmt.Errorf("parter: Config.Tables must have at least one entry")
	}
	for i, t := range cfg.Tables {
		if t.Name == "" {
			return fmt.Errorf("parter: Config.Tables[%d].Name must be non-empty", i)
		}
		if t.Namer == nil {
			return fmt.Errorf("parter: Config.Tables[%d] (%s).Namer must be non-nil", i, t.Name)
		}
		if t.Lookahead < 1 {
			return fmt.Errorf("parter: Config.Tables[%d] (%s).Lookahead must be >= 1, got %d", i, t.Name, t.Lookahead)
		}
		if t.Retention < 0 {
			return fmt.Errorf("parter: Config.Tables[%d] (%s).Retention must be >= 0, got %d", i, t.Name, t.Retention)
		}
		if !t.Strategy.valid() {
			return fmt.Errorf("parter: Config.Tables[%d] (%s).Strategy is not a valid RangeStrategy: %d", i, t.Name, t.Strategy)
		}
	}
	if cfg.Instance != nil && *cfg.Instance < 0 {
		return fmt.Errorf("parter: Config.Instance must be >= 0 when set, got %d", *cfg.Instance)
	}
	return nil
}
