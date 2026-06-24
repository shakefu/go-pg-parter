package parter

import (
	"strings"
	"testing"
)

// metadata.yass.yaml parter.PartitionStatus INPUT: the four text values.
func TestPartitionStatusValues(t *testing.T) {
	cases := []struct {
		s    PartitionStatus
		want string
	}{
		{StatusPending, "pending"},
		{StatusCreated, "created"},
		{StatusDetached, "detached"},
		{StatusFailed, "failed"},
	}
	for _, c := range cases {
		if string(c.s) != c.want {
			t.Errorf("status = %q, want %q", string(c.s), c.want)
		}
	}
}

// metadata.yass.yaml parter.ManagedPartitions SIDE-EFFECT: CREATE SCHEMA.
func TestCreateSchemaSQL(t *testing.T) {
	got := createSchemaSQL("parter")
	want := "CREATE SCHEMA IF NOT EXISTS parter"
	if got != want {
		t.Errorf("createSchemaSQL = %q, want %q", got, want)
	}
	if !strings.Contains(createSchemaSQL("custom"), "custom") {
		t.Error("createSchemaSQL did not template the schema name")
	}
}

// CONTRACT §4 / metadata.yass.yaml parter.ManagedPartitions INPUT: the
// rendered CREATE TABLE contains every column definition and the PK verbatim.
func TestCreateTableSQLColumns(t *testing.T) {
	got := createTableSQL("parter")

	if !strings.HasPrefix(got, "CREATE TABLE IF NOT EXISTS parter.managed_partitions (") {
		t.Errorf("createTableSQL prefix wrong:\n%s", got)
	}

	mustContain := []string{
		"table_name TEXT NOT NULL",
		"partition_name TEXT NOT NULL",
		"range_start TIMESTAMPTZ NOT NULL",
		"range_end TIMESTAMPTZ NOT NULL",
		"status TEXT NOT NULL DEFAULT 'pending'",
		"created_at TIMESTAMPTZ",
		"fail_count INT NOT NULL DEFAULT 0",
		"PRIMARY KEY (table_name, partition_name)",
	}
	for _, frag := range mustContain {
		if !strings.Contains(got, frag) {
			t.Errorf("createTableSQL missing %q:\n%s", frag, got)
		}
	}
	// The retention redesign removed the crash-recovery timestamp column; the
	// rendered DDL must declare ONLY the columns above plus the PK. We assert by
	// counting commas: 7 columns + 1 PK clause => 7 top-level commas.
	if n := strings.Count(got, ",\n"); n != 7 {
		t.Errorf("createTableSQL has %d column/PK separators, want 7 (no recovery column):\n%s", n, got)
	}
	// schema templating
	if !strings.Contains(createTableSQL("custom"), "custom.managed_partitions") {
		t.Error("createTableSQL did not template the schema name")
	}
}

// CONTRACT §4 / metadata.yass.yaml parter.SchemaVersioning SIDE-EFFECT: the
// single idempotent ADD COLUMN IF NOT EXISTS migration (fail_count only),
// verbatim. The recovery-timestamp migration was removed with the retention
// redesign, leaving exactly one migration.
func TestMigrationSQLs(t *testing.T) {
	got := migrationSQLs("parter")
	if len(got) != 1 {
		t.Fatalf("migrationSQLs returned %d statements, want 1", len(got))
	}
	want := []string{
		"ALTER TABLE parter.managed_partitions ADD COLUMN IF NOT EXISTS fail_count INT NOT NULL DEFAULT 0",
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("migration[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
