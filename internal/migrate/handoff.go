package migrate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore"
)

// criticalKeys must be byte-identical in the catalog at handoff: the new
// leader resumes the firehose from the cursor, its compaction from the
// watermark, and its lifecycle from the phase (migration plan §6.6).
var criticalKeys = []string{catalog.RelayCursorKey, "compaction/seq", lifecycle.PhaseKey, "backfill/counts"}

// handoff runs the migration plan §6.7 steps. PostgreSQL is the
// coordinator of a two-phase commit and the local guard its prepared
// record: up to H6 every failure reverts to local ingest, and once H6
// commits the local process never ingests again.
//
// It returns the operator's answer. A refusal, or a failure that reverted,
// returns a nil error and the session carries on tailing. An error means
// the session cannot tell what it left behind; it ends, and the next
// session settles it under a new lease.
func (m *Migrator) handoff(ctx context.Context, sess *catalog.Session, tr *tracker, pausedAt time.Time) (string, error) {
	p, err := m.look(ctx)
	if err != nil {
		return "refused: " + err.Error(), nil
	}
	if p.next > tr.next && p.next-tr.next > m.cfg.HandoffMaxLagSeqs {
		return m.refuse("the replica trails local ingest by %d seqs; wait for it to catch up", p.next-tr.next)
	}
	if age := time.Since(pausedAt); age > m.cfg.MaxCompactionPause {
		return m.refuse("local compaction has been paused for %s, past %s; abort the migration, let compaction run, and start again", age.Round(time.Minute), m.cfg.MaxCompactionPause)
	}
	// Hold the metadata copy for the whole handoff: a resync running now
	// finishes first, and none starts until the handoff is over.
	m.setStep("handoff: waiting for the metadata copy")
	m.meta.mu.Lock()
	defer m.meta.mu.Unlock()
	if last, _ := m.meta.lastResyncAt(); last.IsZero() || time.Since(last) > m.cfg.HandoffMaxDiffAge {
		return m.refuse("no full metadata resync finished in the last %s", m.cfg.HandoffMaxDiffAge)
	}

	// H1.
	m.setStep("handoff: handing_off")
	if _, err := sess.SetMigrationState(ctx, catalog.MigrationTailing, catalog.MigrationHandingOff, nil); err != nil {
		return "failed: " + err.Error(), err
	}
	m.setState(catalog.MigrationHandingOff)
	if err := m.crash(ctx, crashpoint.AfterMigrationHandingOff); err != nil {
		return "failed: " + err.Error(), err
	}

	// H2.
	m.setStep("handoff: stopping local ingest")
	stopCtx, cancel := context.WithTimeout(ctx, m.cfg.HandoffTimeout)
	err = m.cfg.Ingest.Stop(stopCtx)
	cancel()
	if err != nil {
		return m.revert(ctx, sess, "stop local ingest: %v", err)
	}
	// Until H5 the done commit has not been sent, so it cannot land: any
	// exit restarts local ingest at once rather than waiting for a new
	// session to win the lease, which a catalog outage could hold off.
	// The catalog may still say handing_off; the next session reverts it.
	prepared := false
	defer func() {
		if !prepared {
			m.cfg.Ingest.Start()
		}
	}()
	if err := m.crash(ctx, crashpoint.AfterMigrationIngestStopped); err != nil {
		return "failed: " + err.Error(), err
	}

	// H3: nothing writes locally now, so what is shipped is final.
	m.setStep("handoff: shipping the last blocks and metadata")
	for {
		done, err := m.ship(ctx, sess, tr, true)
		if err != nil {
			return "failed: " + err.Error(), err
		}
		if done {
			break
		}
	}
	if p, err = m.look(ctx); err != nil {
		return "failed: " + err.Error(), err
	}
	if tr.next != p.next {
		// The source registered a vacancy at its tip and wrote nothing
		// after it, so no block can carry it to the catalog yet.
		return m.revert(ctx, sess, "the source's seq/next %d is past its last block (%d): a vacancy with no block after it; retry once local ingest has written an event", p.next, tr.next)
	}
	if _, err := m.meta.flushLocked(ctx, sess, true); errors.Is(err, errResyncNeeded) {
		_, err = m.meta.resyncLocked(ctx, sess, true, false)
		if err != nil {
			return "failed: " + err.Error(), err
		}
	} else if err != nil {
		return "failed: " + err.Error(), err
	}
	if err := m.copyKey(ctx, sess, catalog.RelayCursorKey); err != nil {
		return "failed: " + err.Error(), err
	}
	if m.cfg.HandoffFullVerify {
		m.setStep("handoff: comparing all metadata")
		diffs, err := m.meta.resyncLocked(ctx, sess, true, true)
		if err != nil {
			return "failed: " + err.Error(), err
		}
		if diffs > 0 {
			return m.revert(ctx, sess, "the full metadata comparison found %d differences after the final copy", diffs)
		}
	}
	if err := m.crash(ctx, crashpoint.AfterMigrationFinalShip); err != nil {
		return "failed: " + err.Error(), err
	}

	// H4.
	m.setStep("handoff: verifying")
	if err := m.verifyHandoff(ctx, p); err != nil {
		return m.revert(ctx, sess, "%v", err)
	}

	// H5. From here the outcome is the catalog's, and only a session that
	// reads it restarts local ingest.
	prepared = true
	if err := writeGuard(ctx, m.cfg.Meta, GuardPending); err != nil {
		return "failed: " + err.Error(), err
	}
	if err := m.crash(ctx, crashpoint.AfterMigrationGuardPending); err != nil {
		return "failed: " + err.Error(), err
	}

	// H6: the decision.
	m.setStep("handoff: committing")
	if _, err := sess.SetMigrationState(ctx, catalog.MigrationHandingOff, catalog.MigrationDone, []metastore.Op{
		{Kind: metastore.OpSet, Key: []byte(catalog.MigrationHandoffSeqKey), Value: catalog.EncodeSeq(p.next)},
	}); err != nil {
		// The commit may have landed anyway. Only done is a final answer
		// outside the lease; anything else is the next session's to settle.
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), m.cfg.HandoffTimeout)
		st, rerr := m.remoteState(rctx)
		cancel()
		if rerr != nil || st != catalog.MigrationDone {
			return "failed: " + err.Error(), err
		}
	}
	if err := m.crash(ctx, crashpoint.AfterMigrationDone); err != nil {
		return "failed: " + err.Error(), err
	}

	// H7.
	if err := writeGuard(ctx, m.cfg.Meta, GuardDone); err != nil {
		// The catalog says done, so local ingest stays stopped until a
		// restart reads it and writes the guard.
		m.log.Error("handoff committed but the local guard could not record it; a restart will", "err", err)
	}
	m.cfg.Metrics.handoff("done")
	m.log.Info("handoff committed; the disaggregated pods own the archive", "handoff_seq", p.next)
	return "done", m.finish(catalog.MigrationDone)
}

func (m *Migrator) refuse(format string, args ...any) (string, error) {
	m.cfg.Metrics.handoff("refused")
	return "refused: " + fmt.Sprintf(format, args...), nil
}

// revert undoes H1 and restarts local ingest. Called only before H5, so
// no guard needs clearing.
func (m *Migrator) revert(ctx context.Context, sess *catalog.Session, format string, args ...any) (string, error) {
	reason := fmt.Sprintf(format, args...)
	m.log.Warn("handoff reverted; local ingest resumes", "reason", reason)
	m.cfg.Metrics.handoff("reverted")
	if _, err := sess.SetMigrationState(ctx, catalog.MigrationHandingOff, catalog.MigrationTailing, nil); err != nil {
		// The next session reverts it and restarts ingest.
		return "reverted: " + reason, err
	}
	m.setState(catalog.MigrationTailing)
	m.cfg.Ingest.Start()
	return "reverted: " + reason, nil
}

// copyKey copies one key's current local value, or its absence.
func (m *Migrator) copyKey(ctx context.Context, sess *catalog.Session, key string) error {
	v, err := m.cfg.Meta.Get(ctx, []byte(key))
	op := metastore.Op{Kind: metastore.OpSet, Key: []byte(key), Value: v}
	switch {
	case errors.Is(err, metastore.ErrNotFound):
		op = metastore.Op{Kind: metastore.OpDelete, Key: []byte(key)}
	case err != nil:
		return fmt.Errorf("migrate: read local %s: %w", key, err)
	}
	if _, err := sess.ImportMeta(ctx, []metastore.Op{op}); err != nil {
		return fmt.Errorf("migrate: copy %s: %w", key, err)
	}
	return nil
}

// verifyHandoff is H4: with local ingest stopped, the catalog's seq key,
// vacancies, and critical keys must equal the source's exactly.
func (m *Migrator) verifyHandoff(ctx context.Context, p progress) error {
	keys := append([]string{catalog.MainSeqKey, catalog.VacanciesKey}, criticalKeys...)
	raw := make([][]byte, len(keys))
	for i, k := range keys {
		raw[i] = []byte(k)
	}
	remote, err := m.cfg.RemoteMeta.GetMany(ctx, raw)
	if err != nil {
		return fmt.Errorf("read the catalog: %w", err)
	}
	next, err := catalog.DecodeSeq(catalog.MainSeqKey, remote[0], remote[0] != nil)
	if err != nil {
		return err
	}
	if next != p.next {
		return fmt.Errorf("the catalog's seq/next is %d; the source's is %d", next, p.next)
	}
	gaps, err := catalog.DecodeVacancies(remote[1], remote[1] != nil)
	if err != nil {
		return err
	}
	if !slices.Equal(gaps.Ranges(), p.gaps.Ranges()) {
		return fmt.Errorf("the catalog's vacancies %v are not the source's %v", gaps.Ranges(), p.gaps.Ranges())
	}
	local, err := m.cfg.Meta.GetMany(ctx, raw[2:])
	if err != nil {
		return fmt.Errorf("read local metadata: %w", err)
	}
	for i, k := range criticalKeys {
		if (local[i] == nil) != (remote[i+2] == nil) || !bytes.Equal(local[i], remote[i+2]) {
			return fmt.Errorf("the catalog's %s differs from the source's", k)
		}
	}
	return nil
}
