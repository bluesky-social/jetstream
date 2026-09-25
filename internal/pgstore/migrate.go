package pgstore

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"slices"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/jackc/pgx/v5"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// SchemaVersion is the schema this binary expects: the number of the last
// embedded migration.
const SchemaVersion = 1

// ErrInitialized is returned by Initialize when the archive table already
// exists.
var ErrInitialized = errors.New("pgstore: database already holds an archive")

// ErrNotInitialized is returned by CheckVersions on a database without the
// schema.
var ErrNotInitialized = errors.New("pgstore: database holds no archive; run `jetstream storage init`")

// ErrVersionMismatch is returned by CheckVersions when the archive's schema
// or format version is not the compiled one. There is no automatic
// migration on serve (design §8).
var ErrVersionMismatch = errors.New("pgstore: archive version mismatch")

func migrations() ([]string, error) {
	names, err := fs.Glob(migrationFS, "migrations/*.sql")
	if err != nil {
		return nil, err
	}
	slices.Sort(names)
	if len(names) != SchemaVersion {
		return nil, fmt.Errorf("pgstore: %d embedded migrations, SchemaVersion is %d", len(names), SchemaVersion)
	}
	out := make([]string, len(names))
	for i, n := range names {
		b, err := migrationFS.ReadFile(n)
		if err != nil {
			return nil, err
		}
		out[i] = string(b)
	}
	return out, nil
}

// Initialize applies every migration to an empty database and inserts the
// archive row, in one transaction: a failure leaves the database empty. It
// refuses a database that already has the archive table (design §15.1 step
// 1). It creates no segment rows; `storage init` does that through a
// catalog script.
func (s *Store) Initialize(ctx context.Context, archiveID [16]byte) error {
	sqls, err := migrations()
	if err != nil {
		return err
	}
	return pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		// Serializes concurrent inits: the loser's check sees the winner's
		// table once it commits.
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(7467816)"); err != nil {
			return fmt.Errorf("pgstore: init lock: %w", err)
		}
		var exists bool
		if err := tx.QueryRow(ctx, "SELECT to_regclass('archive') IS NOT NULL").Scan(&exists); err != nil {
			return fmt.Errorf("pgstore: check for archive table: %w", err)
		}
		if exists {
			return ErrInitialized
		}
		for i, q := range sqls {
			// No arguments: pgx sends it as one simple-protocol query, which
			// may hold many statements.
			if _, err := tx.Exec(ctx, q); err != nil {
				return fmt.Errorf("pgstore: migration %d: %w", i+1, err)
			}
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO archive (id, archive_id, format_version, schema_version) VALUES (1, $1, $2, $3)`,
			archiveID, catalog.FormatVersion, SchemaVersion)
		if err != nil {
			return fmt.Errorf("pgstore: insert archive row: %w", err)
		}
		return nil
	})
}

// CheckVersions refuses an archive whose schema or format version is not
// the one compiled into this binary (design §8, §15.2). It returns the
// archive row.
func (s *Store) CheckVersions(ctx context.Context) (catalog.ArchiveRow, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, "SELECT to_regclass('archive') IS NOT NULL").Scan(&exists); err != nil {
		return catalog.ArchiveRow{}, fmt.Errorf("pgstore: check for archive table: %w", err)
	}
	if !exists {
		return catalog.ArchiveRow{}, ErrNotInitialized
	}
	a, err := scanArchive(s.pool.QueryRow(ctx, archiveSQL))
	if errors.Is(err, pgx.ErrNoRows) {
		return catalog.ArchiveRow{}, ErrNotInitialized
	}
	if err != nil {
		return catalog.ArchiveRow{}, fmt.Errorf("pgstore: read archive row: %w", err)
	}
	if a.SchemaVersion != SchemaVersion || a.FormatVersion != catalog.FormatVersion {
		return a, fmt.Errorf("%w: archive has schema %d format %d, binary expects schema %d format %d",
			ErrVersionMismatch, a.SchemaVersion, a.FormatVersion, SchemaVersion, catalog.FormatVersion)
	}
	return a, nil
}
