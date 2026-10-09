package pgstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// The migration control tables carry operator requests to a migrator and
// its status back (migration plan §6.7). They sit outside the versioned
// schema: only a migration uses them, an archive that never migrated never
// has them, and creating them needs no SchemaVersion bump, which would
// stop every existing archive from starting. Nothing in them is fenced or
// read by a disaggregated pod: a request is advice that the migrator, the
// only fenced writer, acts on or refuses.
//
// A migrator claims a request before acting on it, so an operator who
// stops waiting can withdraw a request no migrator has seen, and knows
// that one it cannot withdraw is being acted on.
const migrationControlDDL = `
CREATE TABLE IF NOT EXISTS migration_requests (
	request_id   bigserial   PRIMARY KEY,
	action       text        NOT NULL,
	requested_at timestamptz NOT NULL DEFAULT now(),
	claimed_at   timestamptz,
	acked_at     timestamptz,
	result       text
);
CREATE TABLE IF NOT EXISTS migration_status (
	id         int         PRIMARY KEY CHECK (id = 1),
	updated_at timestamptz NOT NULL,
	status     bytea       NOT NULL
);`

// MigrationRequest is one operator request to the migrator.
type MigrationRequest struct {
	ID          int64
	Action      string
	RequestedAt time.Time
	// ClaimedAt is set once a migrator took the request to act on.
	ClaimedAt time.Time
	// AckedAt and Result are set once the request was answered.
	AckedAt time.Time
	Result  string
}

const migrationRequestColumns = `request_id, action, requested_at, claimed_at, acked_at, result`

// EnsureMigrationControl creates the migration control tables if they are
// missing.
func (s *Store) EnsureMigrationControl(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, migrationControlDDL); err != nil {
		return fmt.Errorf("pgstore: create migration control tables: %w", err)
	}
	return nil
}

// RequestMigration records an operator request and returns its ID.
func (s *Store) RequestMigration(ctx context.Context, action string) (int64, error) {
	var id int64
	if err := s.pool.QueryRow(ctx, `INSERT INTO migration_requests (action) VALUES ($1) RETURNING request_id`, action).Scan(&id); err != nil {
		return 0, fmt.Errorf("pgstore: record migration request: %w", err)
	}
	return id, nil
}

// ClaimMigrationRequest claims the oldest request that is neither claimed
// nor answered, and returns it.
func (s *Store) ClaimMigrationRequest(ctx context.Context) (MigrationRequest, bool, error) {
	r, err := scanMigrationRequest(s.pool.QueryRow(ctx, `UPDATE migration_requests SET claimed_at = now()
		WHERE request_id = (
			SELECT request_id FROM migration_requests
			WHERE claimed_at IS NULL AND acked_at IS NULL
			ORDER BY request_id LIMIT 1 FOR UPDATE SKIP LOCKED)
		RETURNING `+migrationRequestColumns))
	if errors.Is(err, pgx.ErrNoRows) {
		return MigrationRequest{}, false, nil
	}
	if err != nil {
		return MigrationRequest{}, false, fmt.Errorf("pgstore: claim a migration request: %w", err)
	}
	return r, true, nil
}

// WithdrawMigrationRequest answers a request no migrator has claimed with
// result, and reports whether it did.
func (s *Store) WithdrawMigrationRequest(ctx context.Context, id int64, result string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE migration_requests SET acked_at = now(), result = $2
		WHERE request_id = $1 AND claimed_at IS NULL AND acked_at IS NULL`, id, result)
	if err != nil {
		return false, fmt.Errorf("pgstore: withdraw migration request %d: %w", id, err)
	}
	return tag.RowsAffected() == 1, nil
}

// AbandonMigrationRequests answers every unanswered request with result,
// claimed or not.
func (s *Store) AbandonMigrationRequests(ctx context.Context, result string) error {
	if _, err := s.pool.Exec(ctx, `UPDATE migration_requests SET acked_at = now(), result = $1
		WHERE acked_at IS NULL`, result); err != nil {
		return fmt.Errorf("pgstore: abandon migration requests: %w", err)
	}
	return nil
}

// MigrationRequestByID returns one request.
func (s *Store) MigrationRequestByID(ctx context.Context, id int64) (MigrationRequest, error) {
	r, err := scanMigrationRequest(s.pool.QueryRow(ctx, `SELECT `+migrationRequestColumns+`
		FROM migration_requests WHERE request_id = $1`, id))
	if err != nil {
		return MigrationRequest{}, fmt.Errorf("pgstore: read migration request %d: %w", id, err)
	}
	return r, nil
}

// AckMigrationRequest answers a request. Answering one already answered
// does nothing.
func (s *Store) AckMigrationRequest(ctx context.Context, id int64, result string) error {
	if _, err := s.pool.Exec(ctx, `UPDATE migration_requests SET acked_at = now(), result = $2
		WHERE request_id = $1 AND acked_at IS NULL`, id, result); err != nil {
		return fmt.Errorf("pgstore: answer migration request %d: %w", id, err)
	}
	return nil
}

// SetMigrationStatus publishes the migrator's status document.
func (s *Store) SetMigrationStatus(ctx context.Context, status []byte) error {
	if _, err := s.pool.Exec(ctx, `INSERT INTO migration_status (id, updated_at, status) VALUES (1, now(), $1)
		ON CONFLICT (id) DO UPDATE SET updated_at = EXCLUDED.updated_at, status = EXCLUDED.status`, nonNil(status)); err != nil {
		return fmt.Errorf("pgstore: publish migration status: %w", err)
	}
	return nil
}

// MigrationStatus reads the migrator's last status document.
func (s *Store) MigrationStatus(ctx context.Context) ([]byte, time.Time, bool, error) {
	var status []byte
	var at time.Time
	err := s.pool.QueryRow(ctx, `SELECT status, updated_at FROM migration_status WHERE id = 1`).Scan(&status, &at)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, time.Time{}, false, nil
	}
	if err != nil {
		return nil, time.Time{}, false, fmt.Errorf("pgstore: read migration status: %w", err)
	}
	return status, at, true, nil
}

func scanMigrationRequest(row pgx.Row) (MigrationRequest, error) {
	var r MigrationRequest
	var claimed, acked *time.Time
	var result *string
	if err := row.Scan(&r.ID, &r.Action, &r.RequestedAt, &claimed, &acked, &result); err != nil {
		return MigrationRequest{}, err
	}
	if claimed != nil {
		r.ClaimedAt = *claimed
	}
	if acked != nil {
		r.AckedAt = *acked
	}
	if result != nil {
		r.Result = *result
	}
	return r, nil
}
