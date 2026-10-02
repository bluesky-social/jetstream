package storagefake

import (
	"context"
	"crypto/rand"
	"time"

	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/jcalabro/atmos/streaming"
)

// Lease is the writer lease over the archive row (design §6.2). Each
// statement is its own autocommit transaction that takes the archive row
// lock, so an Acquire waits for an in-flight leader transaction and every
// later fence by the old epoch fails.
type Lease struct {
	db     *DB
	cl     *Client
	holder [16]byte
	epoch  uint64
}

var _ leader.Locker = (*Lease)(nil)

// NewLease returns a lease with a random holder ID, as each process picks
// one at startup.
func (db *DB) NewLease() *Lease { return db.newLease(nil) }

func (db *DB) newLease(cl *Client) *Lease {
	l := &Lease{db: db, cl: cl}
	_, _ = rand.Read(l.holder[:])
	return l
}

// Holder returns the lease's holder ID.
func (l *Lease) Holder() [16]byte { return l.holder }

// Epoch implements leader.Locker.
func (l *Lease) Epoch() uint64 { return l.epoch }

// update runs one autocommit lease statement. apply mutates the archive row
// and reports whether a row matched.
func (l *Lease) update(ctx context.Context, name string, apply func(s *state, now time.Time) bool) (bool, error) {
	if err := l.db.yield(ctx, l.cl, "lease/"+name); err != nil {
		return false, err
	}
	if err := l.db.lockArchive(ctx); err != nil {
		return false, err
	}
	defer l.db.unlockArchive()
	next := l.db.current().child()
	if !apply(next, l.db.now()) {
		return false, nil
	}
	l.db.publish(next, nil, false)
	return true, nil
}

// Acquire implements streaming.DistributedLocker.
func (l *Lease) Acquire(ctx context.Context, lease time.Duration) error {
	var epoch uint64
	ok, err := l.update(ctx, "acquire", func(s *state, now time.Time) bool {
		a := &s.archive
		if a.HolderID != ([16]byte{}) && a.LeaseExpiresAt.After(now) {
			return false
		}
		a.WriterEpoch++
		a.HolderID = l.holder
		a.LeaseExpiresAt = now.Add(lease)
		epoch = a.WriterEpoch
		return true
	})
	if err != nil {
		return err
	}
	if !ok {
		return streaming.ErrLockHeld
	}
	l.epoch = epoch
	return nil
}

// Renew implements streaming.DistributedLocker.
func (l *Lease) Renew(ctx context.Context, lease time.Duration) error {
	ok, err := l.update(ctx, "renew", func(s *state, now time.Time) bool {
		a := &s.archive
		if a.WriterEpoch != l.epoch || a.HolderID != l.holder || !a.LeaseExpiresAt.After(now) {
			return false
		}
		a.LeaseExpiresAt = now.Add(lease)
		return true
	})
	if err != nil {
		return err
	}
	if !ok {
		return streaming.ErrNotHolder
	}
	return nil
}

// Release implements streaming.DistributedLocker.
func (l *Lease) Release(ctx context.Context) error {
	ok, err := l.update(ctx, "release", func(s *state, now time.Time) bool {
		a := &s.archive
		if a.WriterEpoch != l.epoch || a.HolderID != l.holder {
			return false
		}
		a.HolderID = [16]byte{}
		a.LeaseExpiresAt = now
		return true
	})
	if err != nil {
		return err
	}
	if !ok {
		return streaming.ErrNotHolder
	}
	return nil
}
