package jetstreamd

import (
	"time"

	"github.com/bluesky-social/jetstream/internal/leader"
)

// MigrationConfig is JETSTREAM_MIGRATION_*: the live migration of a local
// archive to disaggregated storage (specs/notes/2026-10-09-local-to-disagg-
// migration.md). Only StandbyBackoff applies in disaggregated mode; the rest
// configure the migrator that runs inside a local-mode process.
type MigrationConfig struct {
	// StandbyBackoff is how long a disaggregated pod waits after finding a
	// catalog a migration still owns before it tries for the lease again.
	StandbyBackoff time.Duration
}

// DefaultMigrationConfig returns every migration knob at its default.
func DefaultMigrationConfig() MigrationConfig {
	return MigrationConfig{
		StandbyBackoff: leader.DefaultStandbyBackoff,
	}
}
