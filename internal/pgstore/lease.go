package pgstore

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/jackc/pgx/v5"
	"github.com/jcalabro/atmos/streaming"
)

// Lease is the writer lease over the archive row (design §6.2). Each
// statement is its own autocommit transaction. It lives here rather than in
// internal/leader because pgstore's catalog.DB already imports leader
// (through catalog), and the lease and the fence share the archive row.
type Lease struct {
	s      *Store
	holder [16]byte
	epoch  atomic.Uint64
}

var _ leader.Locker = (*Lease)(nil)

// NewLease returns a lease with a random holder ID. Each process picks one
// at startup.
func (s *Store) NewLease() *Lease {
	l := &Lease{s: s}
	_, _ = rand.Read(l.holder[:])
	return l
}

// Holder returns the lease's holder ID.
func (l *Lease) Holder() [16]byte { return l.holder }

// Epoch implements leader.Locker: the epoch of the last successful Acquire.
func (l *Lease) Epoch() uint64 { return l.epoch.Load() }

// Acquire implements streaming.DistributedLocker.
func (l *Lease) Acquire(ctx context.Context, lease time.Duration) error {
	var epoch uint64
	err := l.s.pool.QueryRow(ctx,
		`UPDATE archive
		 SET writer_epoch = writer_epoch + 1,
		     holder_id = $1,
		     lease_expires_at = now() + $2::bigint * interval '1 microsecond'
		 WHERE id = 1
		   AND (holder_id IS NULL OR lease_expires_at <= now())
		 RETURNING writer_epoch`, l.holder, lease.Microseconds()).Scan(&epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return streaming.ErrLockHeld
	}
	if err != nil {
		return fmt.Errorf("pgstore: lease acquire: %w", err)
	}
	l.epoch.Store(epoch)
	return nil
}

// Renew implements streaming.DistributedLocker.
func (l *Lease) Renew(ctx context.Context, lease time.Duration) error {
	tag, err := l.s.pool.Exec(ctx,
		`UPDATE archive
		 SET lease_expires_at = now() + $3::bigint * interval '1 microsecond'
		 WHERE id = 1 AND writer_epoch = $1 AND holder_id = $2
		   AND lease_expires_at > now()`, l.epoch.Load(), l.holder, lease.Microseconds())
	if err != nil {
		return fmt.Errorf("pgstore: lease renew: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return streaming.ErrNotHolder
	}
	return nil
}

// Release implements streaming.DistributedLocker.
func (l *Lease) Release(ctx context.Context) error {
	tag, err := l.s.pool.Exec(ctx,
		`UPDATE archive SET holder_id = NULL, lease_expires_at = now()
		 WHERE id = 1 AND writer_epoch = $1 AND holder_id = $2`, l.epoch.Load(), l.holder)
	if err != nil {
		return fmt.Errorf("pgstore: lease release: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return streaming.ErrNotHolder
	}
	return nil
}
