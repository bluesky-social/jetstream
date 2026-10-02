// Package pgfixture is the PostgreSQL scaffolding cmd/storagebench needs
// outside a test: scratch databases and the server's WAL position. It lives
// under pgstore because only the storage backends may import pgx.
package pgfixture

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/bluesky-social/jetstream/internal/pgstore"
	"github.com/jackc/pgx/v5"
)

// CreateDatabase creates an empty database jst_<random> on the server
// adminURL names, whose role needs CREATEDB. It returns the database's URL
// and a drop func that removes it, disconnecting any sessions left on it.
// Errors never contain adminURL.
func CreateDatabase(ctx context.Context, adminURL string) (string, func(context.Context) error, error) {
	u, err := url.Parse(adminURL)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return "", nil, errors.New("pgfixture: the admin URL must be a postgres:// URL")
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	name := "jst_" + hex.EncodeToString(b[:])
	if err := exec(ctx, adminURL, "CREATE DATABASE "+name); err != nil {
		return "", nil, fmt.Errorf("pgfixture: create %s: %w", name, err)
	}
	drop := func(ctx context.Context) error {
		if err := exec(ctx, adminURL, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			return fmt.Errorf("pgfixture: drop %s: %w", name, err)
		}
		return nil
	}
	u.Path = "/" + name
	return u.String(), drop, nil
}

func exec(ctx context.Context, rawURL, stmt string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cfg, err := pgx.ParseConfig(rawURL)
	if err != nil {
		return errors.New("cannot parse the connection URL")
	}
	c, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close(context.Background()) }()
	_, err = c.Exec(ctx, stmt)
	return err
}

// WALPosition returns the server's current WAL insert position in bytes.
// The WAL is shared by every database on the server, so a difference
// between two readings is attributable to one workload only when nothing
// else writes.
func WALPosition(ctx context.Context, s *pgstore.Store) (uint64, error) {
	var pos int64
	if err := s.Pool().QueryRow(ctx, `SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint`).Scan(&pos); err != nil {
		return 0, fmt.Errorf("pgfixture: read WAL position: %w", err)
	}
	return uint64(pos), nil
}

// DatabaseBytes returns the size of the database s is connected to.
func DatabaseBytes(ctx context.Context, s *pgstore.Store) (int64, error) {
	var n int64
	if err := s.Pool().QueryRow(ctx, `SELECT pg_database_size(current_database())`).Scan(&n); err != nil {
		return 0, fmt.Errorf("pgfixture: read database size: %w", err)
	}
	return n, nil
}

// MarkObjectsAvailable makes every uploading object available, for a
// fixture that inserts object rows it never uploads. It returns how many
// rows changed. It updates in chunks, so a catalog of millions of objects
// stays inside the pool's statement timeout.
func MarkObjectsAvailable(ctx context.Context, s *pgstore.Store) (int64, error) {
	var total int64
	for {
		tag, err := s.Pool().Exec(ctx, `UPDATE objects SET state = 'available'
			WHERE object_id IN (SELECT object_id FROM objects WHERE state = 'uploading' LIMIT 100000)`)
		if err != nil {
			return total, fmt.Errorf("pgfixture: mark objects available: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return total, nil
		}
		total += tag.RowsAffected()
	}
}
