package parter

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PartitionStatus is the value of the status column on managed_partitions.
//
// metadata.yass.yaml parter.PartitionStatus.
type PartitionStatus string

const (
	// StatusPending: needs to be created and attached.
	StatusPending PartitionStatus = "pending"
	// StatusCreated: exists and is attached to the parent.
	StatusCreated PartitionStatus = "created"
	// StatusDetached: detached from the parent and awaiting DROP TABLE.
	StatusDetached PartitionStatus = "detached"
	// StatusFailed: creation failed repeatedly and the row is parked, requiring
	// manual intervention.
	StatusFailed PartitionStatus = "failed"
)

// createSchemaSQL renders the CREATE SCHEMA statement for the metadata schema.
//
// CONTRACT §4 / metadata.yass.yaml parter.ManagedPartitions SIDE-EFFECT.
func createSchemaSQL(schema string) string {
	return fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema)
}

// createTableSQL renders the CREATE TABLE statement for managed_partitions in
// the given schema, with all columns and the composite primary key verbatim.
//
// CONTRACT §4 / metadata.yass.yaml parter.ManagedPartitions INPUT.
func createTableSQL(schema string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.managed_partitions (
    table_name TEXT NOT NULL,
    partition_name TEXT NOT NULL,
    range_start TIMESTAMPTZ NOT NULL,
    range_end TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ,
    fail_count INT NOT NULL DEFAULT 0,
    PRIMARY KEY (table_name, partition_name)
)`, schema)
}

// migrationSQLs renders the idempotent additive schema-versioning migrations
// that upgrade tables created by older library versions in place.
//
// CONTRACT §4 / metadata.yass.yaml parter.SchemaVersioning SIDE-EFFECT.
func migrationSQLs(schema string) []string {
	return []string{
		fmt.Sprintf("ALTER TABLE %s.managed_partitions ADD COLUMN IF NOT EXISTS fail_count INT NOT NULL DEFAULT 0", schema),
	}
}

// createSchema creates the metadata schema if it does not exist.
func createSchema(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	_, err := pool.Exec(ctx, createSchemaSQL(schema))
	return err
}

// createTable creates the managed_partitions table if it does not exist.
func createTable(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	_, err := pool.Exec(ctx, createTableSQL(schema))
	return err
}

// applyMigrations runs the idempotent additive schema-versioning migrations.
// It is a no-op on a freshly created table and upgrades older tables in place.
func applyMigrations(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	for _, sql := range migrationSQLs(schema) {
		if _, err := pool.Exec(ctx, sql); err != nil {
			return err
		}
	}
	return nil
}
