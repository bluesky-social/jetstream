package jetstreamd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/bluesky-social/jetstream/internal/migrate"
	"github.com/bluesky-social/jetstream/internal/pgstore"
	"github.com/cockroachdb/pebble/vfs"
)

// MigrationOperator is the operator's side of the migration control
// channel: `jetstream migrate`.
type MigrationOperator interface {
	// Request records a request for the migrator and returns its ID.
	Request(ctx context.Context, action string) (int64, error)
	// Result returns a request's answer, once it has one, and whether a
	// migrator claimed it.
	Result(ctx context.Context, id int64) (result string, answered, claimed bool, err error)
	// Withdraw answers a request no migrator has claimed, and reports
	// whether it did.
	Withdraw(ctx context.Context, id int64, result string) (bool, error)
	// Status returns the migrator's last published status.
	Status(ctx context.Context) ([]byte, time.Time, bool, error)
}

type pgMigrationOperator struct{ pg *pgstore.Store }

func (o pgMigrationOperator) Request(ctx context.Context, action string) (int64, error) {
	return o.pg.RequestMigration(ctx, action)
}

func (o pgMigrationOperator) Result(ctx context.Context, id int64) (string, bool, bool, error) {
	r, err := o.pg.MigrationRequestByID(ctx, id)
	return r.Result, !r.AckedAt.IsZero(), !r.ClaimedAt.IsZero(), err
}

func (o pgMigrationOperator) Withdraw(ctx context.Context, id int64, result string) (bool, error) {
	return o.pg.WithdrawMigrationRequest(ctx, id, result)
}

func (o pgMigrationOperator) Status(ctx context.Context) ([]byte, time.Time, bool, error) {
	return o.pg.MigrationStatus(ctx)
}

// MigrationReport is `jetstream migrate status`: what the catalog says, and
// what the migrator last published.
type MigrationReport struct {
	State           catalog.MigrationState `json:"state"`
	NextSeq         uint64                 `json:"catalog_next_seq"`
	HandoffSeq      uint64                 `json:"handoff_seq,omitempty"`
	Vacancies       int                    `json:"vacancies"`
	Migrator        *migrate.Status        `json:"migrator,omitempty"`
	MigratorUpdated time.Time              `json:"migrator_updated_at,omitzero"`
}

// withMigrationBackend opens cfg's backend for one operator command.
func withMigrationBackend(ctx context.Context, cfg StorageConfig, fn func(*StorageBackend) error) error {
	switch {
	case cfg.PG.URL == "":
		return errors.New("migrate: JETSTREAM_PG_URL is required")
	case cfg.S3.Region == "":
		return errors.New("migrate: JETSTREAM_S3_REGION is required")
	case cfg.S3.Bucket == "":
		return errors.New("migrate: JETSTREAM_S3_BUCKET is required")
	}
	b, err := openBackend(ctx, cfg, nil, nil)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	defer b.Close()
	return fn(b)
}

// MigrationStatus is `jetstream migrate status`.
func MigrationStatus(ctx context.Context, cfg StorageConfig) (MigrationReport, error) {
	var rep MigrationReport
	err := withMigrationBackend(ctx, cfg, func(b *StorageBackend) error {
		var err error
		rep, err = b.MigrationReport(ctx)
		return err
	})
	return rep, err
}

// MigrationReport reads the migration's state from b.
func (b *StorageBackend) MigrationReport(ctx context.Context) (MigrationReport, error) {
	if _, err := b.Archive(ctx); err != nil {
		return MigrationReport{}, fmt.Errorf("migrate: %w", err)
	}
	keys := [][]byte{[]byte(catalog.MigrationStateKey), []byte(catalog.MainSeqKey), []byte(catalog.MigrationHandoffSeqKey), []byte(catalog.VacanciesKey)}
	vals, err := b.MetaStore(nil).GetMany(ctx, keys)
	if err != nil {
		return MigrationReport{}, fmt.Errorf("migrate: read the catalog: %w", err)
	}
	var rep MigrationReport
	if vals[0] != nil {
		if rep.State, err = catalog.ParseMigrationState(vals[0]); err != nil {
			return MigrationReport{}, err
		}
	}
	if rep.NextSeq, err = catalog.DecodeSeq(catalog.MainSeqKey, vals[1], vals[1] != nil); err != nil {
		return MigrationReport{}, err
	}
	if vals[2] != nil {
		if rep.HandoffSeq, err = catalog.DecodeSeq(catalog.MigrationHandoffSeqKey, vals[2], true); err != nil {
			return MigrationReport{}, err
		}
	}
	gaps, err := catalog.DecodeVacancies(vals[3], vals[3] != nil)
	if err != nil {
		return MigrationReport{}, err
	}
	rep.Vacancies = gaps.Count()
	if b.MigrationOperator == nil {
		return rep, nil
	}
	op, err := b.MigrationOperator(ctx)
	if err != nil {
		return MigrationReport{}, fmt.Errorf("migrate: %w", err)
	}
	doc, at, ok, err := op.Status(ctx)
	if err != nil {
		return MigrationReport{}, fmt.Errorf("migrate: %w", err)
	}
	if ok {
		var st migrate.Status
		if err := json.Unmarshal(doc, &st); err != nil {
			return MigrationReport{}, fmt.Errorf("migrate: decode the migrator's status: %w", err)
		}
		rep.Migrator, rep.MigratorUpdated = &st, at
	}
	return rep, nil
}

// RequestMigration is `jetstream migrate handoff` and `abort`: it records
// the request and waits up to wait for the migrator's answer.
func RequestMigration(ctx context.Context, cfg StorageConfig, action string, wait time.Duration) (string, error) {
	var result string
	err := withMigrationBackend(ctx, cfg, func(b *StorageBackend) error {
		var err error
		result, err = b.RequestMigration(ctx, action, wait)
		return err
	})
	return result, err
}

// RequestMigration records action for the migrator and waits for its
// answer.
func (b *StorageBackend) RequestMigration(ctx context.Context, action string, wait time.Duration) (string, error) {
	if action != migrate.ActionHandoff && action != migrate.ActionAbort {
		return "", fmt.Errorf("migrate: unknown action %q", action)
	}
	if b.MigrationOperator == nil {
		return "", errors.New("migrate: the storage backend has no control channel")
	}
	op, err := b.MigrationOperator(ctx)
	if err != nil {
		return "", fmt.Errorf("migrate: %w", err)
	}
	id, err := op.Request(ctx, action)
	if err != nil {
		return "", fmt.Errorf("migrate: %w", err)
	}
	if result, answered, err := awaitAnswer(ctx, op, id, wait); err != nil || answered {
		return result, err
	}
	// Out of time. A request no migrator claimed is withdrawn, so it never
	// runs after the operator stopped watching; a claimed one is underway.
	fctx, fcancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer fcancel()
	withdrawn, err := op.Withdraw(fctx, id, "withdrawn: no migrator claimed it in time")
	switch {
	case err != nil:
		return "", fmt.Errorf("migrate: request %d has no answer, and withdrawing it failed, so a migrator may still act on it: %w", id, err)
	case withdrawn:
		return "", fmt.Errorf("migrate: no migrator claimed request %d within %s, so it was withdrawn; is the migrator running? (`jetstream migrate status`)", id, wait)
	}
	if result, answered, _, err := op.Result(fctx, id); err == nil && answered {
		return result, nil
	}
	return "", fmt.Errorf("migrate: the migrator is acting on request %d and has not answered within %s; follow it with `jetstream migrate status`", id, wait)
}

// requestPollInterval bounds how often `jetstream migrate` polls for an
// answer; polling starts faster, since refusals come back at once.
const requestPollInterval = 500 * time.Millisecond

// awaitAnswer polls request id until it is answered or wait runs out.
func awaitAnswer(ctx context.Context, op MigrationOperator, id int64, wait time.Duration) (string, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	interval := 20 * time.Millisecond
	for {
		result, answered, _, err := op.Result(ctx, id)
		switch {
		case err == nil && answered:
			return result, true, nil
		case err != nil && ctx.Err() == nil:
			return "", false, fmt.Errorf("migrate: %w", err)
		}
		select {
		case <-ctx.Done():
			return "", false, nil
		case <-time.After(interval):
		}
		interval = min(interval*2, requestPollInterval)
	}
}

// DefaultReclaimMargin is the seqs `migrate reclaim` leaves between the
// catalog's last seq and the first one local ingest assigns again.
const DefaultReclaimMargin = 1_000_000_000

// ReclaimResult is what `jetstream migrate reclaim` did.
type ReclaimResult struct {
	// CatalogNext is the catalog's seq/next when it was reverted.
	CatalogNext uint64
	// LocalNext is the local seq/next, and ResumeSeq the first seq local
	// ingest assigns when it restarts: [LocalNext, ResumeSeq) becomes a
	// vacancy.
	LocalNext, ResumeSeq uint64
}

// ReclaimLocal is the emergency rollback after a handoff (migration plan
// §8): it sets the catalog to reverted, so no pod may lead it again, and
// makes the stopped local archive resume past every seq the catalog
// assigned. The disaggregated pods must be stopped first: reclaim takes
// their lease and refuses while one holds it.
func ReclaimLocal(ctx context.Context, cfg StorageConfig, dataDir string, fsys vfs.FS, margin uint64) (ReclaimResult, error) {
	var res ReclaimResult
	err := withMigrationBackend(ctx, cfg, func(b *StorageBackend) error {
		var err error
		res, err = b.ReclaimLocal(ctx, cfg.Leader.Lease, dataDir, fsys, margin)
		return err
	})
	return res, err
}

// ReclaimLocal is ReclaimLocal over b.
func (b *StorageBackend) ReclaimLocal(ctx context.Context, lease time.Duration, dataDir string, fsys vfs.FS, margin uint64) (ReclaimResult, error) {
	if dataDir == "" {
		return ReclaimResult{}, errors.New("migrate reclaim: --data-dir is required")
	}
	if margin == 0 {
		margin = DefaultReclaimMargin
	}
	// Open the local store first: its lock proves no local process runs.
	st, err := pebblestore.Open(dataDir, nil, pebblestore.WithFS(fsys))
	if err != nil {
		return ReclaimResult{}, fmt.Errorf("migrate reclaim: open the local metadata (is the local process stopped?): %w", err)
	}
	defer func() { _ = st.Close() }()

	locker := b.NewLease()
	if err := locker.Acquire(ctx, lease); err != nil {
		return ReclaimResult{}, fmt.Errorf("migrate reclaim: take the writer lease (stop every disaggregated pod first): %w", err)
	}
	defer func() { _ = locker.Release(context.WithoutCancel(ctx)) }()
	sess := catalog.NewSession(catalog.SessionConfig{DB: b.DB, Epoch: locker.Epoch()})
	state, err := sess.ReadMigrationState(ctx)
	if err != nil {
		return ReclaimResult{}, fmt.Errorf("migrate reclaim: %w", err)
	}
	switch state {
	case catalog.MigrationDone:
		if _, err := sess.SetMigrationState(ctx, state, catalog.MigrationReverted, nil); err != nil {
			return ReclaimResult{}, fmt.Errorf("migrate reclaim: %w", err)
		}
	case catalog.MigrationReverted:
	default:
		return ReclaimResult{}, fmt.Errorf("migrate reclaim: migration/state is %q; reclaim only undoes a finished handoff (abort an unfinished one)", state)
	}
	// Under the lease and with the state reverted, no writer can move the
	// seq key again.
	v, err := b.MetaStore(nil).Get(ctx, []byte(catalog.MainSeqKey))
	if err != nil && !errors.Is(err, metastore.ErrNotFound) {
		return ReclaimResult{}, fmt.Errorf("migrate reclaim: read the catalog: %w", err)
	}
	var res ReclaimResult
	if res.CatalogNext, err = catalog.DecodeSeq(catalog.MainSeqKey, v, err == nil); err != nil {
		return ReclaimResult{}, err
	}

	lv, err := st.Get(ctx, []byte(catalog.MainSeqKey))
	if err != nil && !errors.Is(err, metastore.ErrNotFound) {
		return ReclaimResult{}, fmt.Errorf("migrate reclaim: read local seq/next: %w", err)
	}
	if res.LocalNext, err = catalog.DecodeSeq(catalog.MainSeqKey, lv, err == nil); err != nil {
		return ReclaimResult{}, err
	}
	res.ResumeSeq = max(res.CatalogNext, res.LocalNext) + margin
	// The seq lease registers [seq/next, seq/max_reserved) as a vacancy
	// when the next session starts, exactly as after an unclean stop.
	b2 := st.NewBatch()
	b2.Set([]byte(ingest.SeqReservedKey), catalog.EncodeSeq(res.ResumeSeq))
	b2.Delete([]byte(migrate.HandoffGuardKey))
	b2.Delete([]byte(migrate.CompactionPausedKey))
	if err := b2.Commit(ctx); err != nil {
		return ReclaimResult{}, fmt.Errorf("migrate reclaim: write local state: %w", err)
	}
	return res, nil
}
