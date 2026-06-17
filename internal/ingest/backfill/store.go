// Package backfill: store.go implements the atmos backfill.Store
// interface against pebble. Keys live at repo/<did>; values are the
// JSON-encoded RepoStatus from status.go.
//
// All callbacks the engine fires (OnDiscover, OnUpdate, OnComplete,
// OnFail) write with pebble.Sync to satisfy atmos's durability
// contract: the engine treats a successful return as durable.
//
// Whole-row read-modify-write paths preserve fields a future PR may
// add to RepoStatus (e.g. RecordCount). They serialize through countsMu
// with aggregate writes and deferred completion staging so a stale
// callback cannot overwrite a just-committed completion row.
package backfill

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/crashpoint"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
)

// Store implements atmosbackfill.Store against the shared pebble
// metadata store. Construct via NewStore.
type Store struct {
	db                 *store.Store
	metrics            *Metrics
	afterComplete      func(context.Context, atmos.DID) error
	afterCompleteError func(error)
	crashInjector      crashpoint.Injector
	countsMu           sync.Mutex
	completions        *completionBatcher
}

// Compile-time guarantee that Store satisfies the atmos contract.
var _ atmosbackfill.Store = (*Store)(nil)

// NewStore constructs a Store backed by the shared metadata pebble db.
// metrics may be nil; callbacks are no-ops in that case.
func NewStore(db *store.Store, metrics *Metrics) *Store {
	return &Store{db: db, metrics: metrics}
}

// SetCompletionBatcher defers OnComplete writes into writer durable metadata
// batches. It is intended for construction-time wiring before the backfill
// engine starts.
func (s *Store) SetCompletionBatcher(b *completionBatcher) {
	s.completions = b
}

// Lookup reads repo/<did> and projects the on-disk RepoStatus into
// atmos's StoreEntry shape. A missing row returns StateUnknown — that's
// how atmos tells the engine to fire OnDiscover.
func (s *Store) Lookup(_ context.Context, did atmos.DID) (atmosbackfill.StoreEntry, error) {
	val, closer, err := s.db.Get(repoKey(did))
	if errors.Is(err, store.ErrNotFound) {
		return atmosbackfill.StoreEntry{State: atmosbackfill.StateUnknown}, nil
	}
	if err != nil {
		return atmosbackfill.StoreEntry{}, fmt.Errorf("backfill: lookup %s: %w", did, err)
	}
	defer func() { _ = closer.Close() }()

	rs, err := decodeRepoStatus(val)
	if err != nil {
		return atmosbackfill.StoreEntry{}, fmt.Errorf("backfill: lookup %s: %w", did, err)
	}

	var st atmosbackfill.State
	switch rs.Backfill.Status {
	case StatusNotStarted:
		st = atmosbackfill.StateDiscovered
	case StatusComplete:
		st = atmosbackfill.StateComplete
	case StatusFailed:
		st = atmosbackfill.StateFailed
	case StatusUnavailable:
		// Terminal, non-retryable: atmos has no dedicated state for
		// "exists but unfetchable", so we project to StateComplete,
		// which is the only Lookup result that makes the engine skip
		// re-dispatch (engine.reconcile). The distinct lifecycle is
		// preserved on disk via Backfill.Status for diagnostics.
		st = atmosbackfill.StateComplete
	default:
		return atmosbackfill.StoreEntry{}, fmt.Errorf("backfill: lookup %s: unknown status %q", did, rs.Backfill.Status)
	}

	return atmosbackfill.StoreEntry{State: st, Active: rs.Active}, nil
}

// putRepoStatus writes the value durably. Used by all write paths.
func (s *Store) putRepoStatus(did atmos.DID, rs *RepoStatus) error {
	enc, err := encodeRepoStatus(rs)
	if err != nil {
		return err
	}
	if err := s.db.Set(repoKey(did), enc, store.SyncWrites); err != nil {
		return fmt.Errorf("backfill: write repo/%s: %w", did, err)
	}
	return nil
}

func (s *Store) putRepoStatusAndCounts(
	did atmos.DID,
	rs *RepoStatus,
	hadRow bool,
	old Status,
	updateHost func(*HostStatus),
) error {
	enc, err := encodeRepoStatus(rs)
	if err != nil {
		return err
	}

	s.countsMu.Lock()
	defer s.countsMu.Unlock()

	counts, ok, err := LoadCounts(s.db)
	if err != nil {
		return err
	}
	if !ok {
		counts, err = CountStatuses(s.db)
		if err != nil {
			return err
		}
	}
	applyCountTransition(&counts, hadRow, old, rs.Backfill.Status)
	countsEnc, err := encodeCounts(counts)
	if err != nil {
		return err
	}

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.Set(repoKey(did), enc, nil); err != nil {
		return fmt.Errorf("backfill: stage repo/%s: %w", did, err)
	}
	if err := batch.Set([]byte(countsKey), countsEnc, nil); err != nil {
		return fmt.Errorf("backfill: stage counts: %w", err)
	}
	if rs.Host != "" {
		hs, _, err := loadHostStatus(s.db, rs.Host)
		if err != nil {
			return err
		}
		applyHostStatusTransition(hs, hadRow, rs.Active, old, rs.Backfill.Status)
		if updateHost != nil {
			updateHost(hs)
		}
		if err := stageHostStatus(batch, hs); err != nil {
			return err
		}
	}
	if err := s.db.Commit(batch, store.SyncWrites); err != nil {
		return fmt.Errorf("backfill: write repo/%s and counts: %w", did, err)
	}
	return nil
}

func (s *Store) updateRepoStatusAndCounts(
	did atmos.DID,
	mutate func(*RepoStatus, bool, Status) (func(*HostStatus), error),
) error {
	s.countsMu.Lock()
	defer s.countsMu.Unlock()

	rs, err := s.readRepoStatus(did)
	if err != nil {
		return err
	}
	hadRow := rs != nil
	old := Status("")
	if rs == nil {
		rs = &RepoStatus{}
	} else {
		old = rs.Backfill.Status
	}
	updateHost, err := mutate(rs, hadRow, old)
	if err != nil {
		return err
	}

	counts, ok, err := LoadCounts(s.db)
	if err != nil {
		return err
	}
	if !ok {
		counts, err = CountStatuses(s.db)
		if err != nil {
			return err
		}
	}
	applyCountTransition(&counts, hadRow, old, rs.Backfill.Status)
	countsEnc, err := encodeCounts(counts)
	if err != nil {
		return err
	}
	enc, err := encodeRepoStatus(rs)
	if err != nil {
		return err
	}

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.Set(repoKey(did), enc, nil); err != nil {
		return fmt.Errorf("backfill: stage repo/%s: %w", did, err)
	}
	if err := batch.Set([]byte(countsKey), countsEnc, nil); err != nil {
		return fmt.Errorf("backfill: stage counts: %w", err)
	}
	if rs.Host != "" {
		hs, _, err := loadHostStatus(s.db, rs.Host)
		if err != nil {
			return err
		}
		applyHostStatusTransition(hs, hadRow, rs.Active, old, rs.Backfill.Status)
		if updateHost != nil {
			updateHost(hs)
		}
		if err := stageHostStatus(batch, hs); err != nil {
			return err
		}
	}
	if err := s.db.Commit(batch, store.SyncWrites); err != nil {
		return fmt.Errorf("backfill: write repo/%s and counts: %w", did, err)
	}
	return nil
}

func (s *Store) stageCompleteBatch(ctx context.Context, batch *pebble.Batch, completions []queuedCompletion) (func(error), error) {
	s.countsMu.Lock()
	locked := true
	unlock := func(error) {
		if locked {
			locked = false
			s.countsMu.Unlock()
		}
	}
	fail := func(err error) (func(error), error) {
		unlock(err)
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if len(completions) == 0 {
		unlock(nil)
		return nil, nil
	}

	counts, ok, err := LoadCounts(s.db)
	if err != nil {
		return fail(err)
	}
	if !ok {
		counts, err = CountStatuses(s.db)
		if err != nil {
			return fail(err)
		}
	}

	type stagedRepoStatus struct {
		status *RepoStatus
		hadRow bool
	}
	repoCache := make(map[atmos.DID]stagedRepoStatus)
	hostCache := make(map[string]*HostStatus)
	for _, c := range completions {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		if c.commit == nil {
			return fail(fmt.Errorf("backfill: stage complete %s: nil commit", c.did))
		}

		cached, ok := repoCache[c.did]
		rs := cached.status
		hadRow := cached.hadRow
		if !ok {
			var err error
			rs, err = s.readRepoStatus(c.did)
			if err != nil {
				return fail(err)
			}
			hadRow = rs != nil
			if rs == nil {
				rs = &RepoStatus{}
			}
		}
		old := rs.Backfill.Status
		if !hadRow {
			old = Status("")
		}
		rs.Backfill.Status = StatusComplete
		rs.Backfill.Rev = c.commit.Rev
		rs.Backfill.CompletedAt = c.completed
		rs.Backfill.LastError = ""
		rs.Rev = c.commit.Rev
		rs.UpdatedAt = c.completed
		rs.LastAttemptedAt = c.completed
		applyCountTransition(&counts, hadRow, old, StatusComplete)

		enc, err := encodeRepoStatus(rs)
		if err != nil {
			return fail(err)
		}
		if err := batch.Set(repoKey(c.did), enc, nil); err != nil {
			return fail(fmt.Errorf("backfill: stage repo/%s: %w", c.did, err))
		}
		repoCache[c.did] = stagedRepoStatus{status: rs, hadRow: true}
		if rs.Host != "" {
			hs := hostCache[rs.Host]
			if hs == nil {
				var err error
				hs, _, err = loadHostStatus(s.db, rs.Host)
				if err != nil {
					return fail(err)
				}
				hostCache[rs.Host] = hs
			}
			applyHostStatusTransition(hs, hadRow, rs.Active, old, StatusComplete)
			hs.LastAttemptedAt = c.completed
		}
	}
	countsEnc, err := encodeCounts(counts)
	if err != nil {
		return fail(err)
	}
	if err := batch.Set([]byte(countsKey), countsEnc, nil); err != nil {
		return fail(fmt.Errorf("backfill: stage counts: %w", err))
	}
	for _, hs := range hostCache {
		if err := stageHostStatus(batch, hs); err != nil {
			return fail(err)
		}
	}
	return func(commitErr error) {
		unlock(commitErr)
		if commitErr != nil {
			return
		}
		for _, c := range completions {
			if err := s.simulateCrash(ctx, crashpoint.AfterRepoComplete); err != nil {
				if s.afterCompleteError != nil {
					s.afterCompleteError(fmt.Errorf("backfill: after repo complete crashpoint %s: %w", c.did, err))
				}
				continue
			}
			if s.afterComplete != nil {
				if err := s.afterComplete(ctx, c.did); err != nil {
					err = fmt.Errorf("backfill: after complete hook %s: %w", c.did, err)
					if s.afterCompleteError != nil {
						s.afterCompleteError(err)
					}
				}
			}
		}
	}, nil
}

func applyCountTransition(c *Counts, hadRow bool, old, next Status) {
	if !hadRow {
		c.Total++
	}
	if hadRow && old == next {
		return
	}
	if p := countBucket(c, old); p != nil && *p > 0 {
		*p--
	}
	if p := countBucket(c, next); p != nil {
		*p++
	}
}

func countBucket(c *Counts, st Status) *uint64 {
	switch st {
	case StatusNotStarted:
		return &c.Discovered
	case StatusComplete:
		return &c.Complete
	case StatusFailed:
		return &c.Failed
	case StatusUnavailable:
		return &c.Unavailable
	default:
		return nil
	}
}

func applyHostStatusTransition(h *HostStatus, hadRow bool, active bool, old, next Status) {
	if !hadRow {
		h.Total++
		if active {
			h.Active++
		}
		incrementStatus(h, next)
		return
	}
	if old == next {
		return
	}
	decrementStatus(h, old)
	incrementStatus(h, next)
}

func applyHostActiveTransition(h *HostStatus, oldActive, nextActive bool) {
	if oldActive == nextActive {
		return
	}
	if nextActive {
		h.Active++
		return
	}
	if h.Active > 0 {
		h.Active--
	}
}

// readRepoStatus is the RMW helper for OnUpdate/OnComplete/OnFail.
// It returns (nil, nil) when the row doesn't exist so callers can
// decide whether absence is an error in their context.
func (s *Store) readRepoStatus(did atmos.DID) (*RepoStatus, error) {
	val, closer, err := s.db.Get(repoKey(did))
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("backfill: read repo/%s: %w", did, err)
	}
	defer func() { _ = closer.Close() }()
	return decodeRepoStatus(val)
}

func (s *Store) updateRepoActive(did atmos.DID, active bool) error {
	s.countsMu.Lock()
	defer s.countsMu.Unlock()

	rs, err := s.readRepoStatus(did)
	if err != nil {
		return err
	}
	if rs == nil {
		return fmt.Errorf("backfill: on_update %s: missing row (atmos invariant violation)", did)
	}
	oldActive := rs.Active
	rs.Active = active

	enc, err := encodeRepoStatus(rs)
	if err != nil {
		return err
	}

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.Set(repoKey(did), enc, nil); err != nil {
		return fmt.Errorf("backfill: stage repo/%s: %w", did, err)
	}
	if rs.Host != "" && oldActive != rs.Active {
		hs, _, err := loadHostStatus(s.db, rs.Host)
		if err != nil {
			return err
		}
		applyHostActiveTransition(hs, oldActive, rs.Active)
		if err := stageHostStatus(batch, hs); err != nil {
			return err
		}
	}
	if err := s.db.Commit(batch, store.SyncWrites); err != nil {
		return fmt.Errorf("backfill: write repo/%s and host active: %w", did, err)
	}
	return nil
}

func (s *Store) recordIdentityResolution(_ context.Context, did atmos.DID, resolution IdentityResolution) error {
	normalizedHost, _, err := normalizeHostStatusKey(resolution.Host)
	if err != nil {
		return fmt.Errorf("backfill: record identity resolution %s: %w", did, err)
	}

	s.countsMu.Lock()
	defer s.countsMu.Unlock()

	rs, err := s.readRepoStatus(did)
	if err != nil {
		return err
	}
	hadRow := rs != nil
	if rs == nil {
		rs = &RepoStatus{
			Backfill: RepoBackfillStatus{Status: StatusNotStarted},
		}
	}
	oldStatus := rs.Backfill.Status
	oldHost := rs.Host
	if oldHost != "" {
		oldHost, _, err = normalizeHostStatusKey(oldHost)
		if err != nil {
			return fmt.Errorf("backfill: record identity resolution %s: existing host %q: %w", did, rs.Host, err)
		}
	}
	oldActive := rs.Active
	oldHandle := rs.Handle

	rs.Handle = resolution.Handle
	rs.PDS = resolution.PDS
	rs.Host = normalizedHost

	enc, err := encodeRepoStatus(rs)
	if err != nil {
		return err
	}

	var countsEnc []byte
	if !hadRow {
		counts, ok, err := LoadCounts(s.db)
		if err != nil {
			return err
		}
		if !ok {
			counts, err = CountStatuses(s.db)
			if err != nil {
				return err
			}
		}
		applyCountTransition(&counts, false, "", rs.Backfill.Status)
		countsEnc, err = encodeCounts(counts)
		if err != nil {
			return err
		}
	}

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.Set(repoKey(did), enc, nil); err != nil {
		return fmt.Errorf("backfill: stage repo/%s: %w", did, err)
	}
	if len(countsEnc) > 0 {
		if err := batch.Set([]byte(countsKey), countsEnc, nil); err != nil {
			return fmt.Errorf("backfill: stage counts: %w", err)
		}
	}
	if handleIndexChanged(oldHandle, resolution.Handle) {
		if err := stageHandleIndexDeleteIfMatches(s.db, batch, oldHandle, did); err != nil {
			return err
		}
	}
	if err := stageHandleIndexSet(batch, resolution.Handle, did); err != nil {
		return err
	}
	if oldHost != normalizedHost {
		if oldHost != "" {
			oldHS, _, err := loadHostStatus(s.db, oldHost)
			if err != nil {
				return err
			}
			if oldHS.Total > 0 {
				oldHS.Total--
			}
			if oldActive && oldHS.Active > 0 {
				oldHS.Active--
			}
			decrementStatus(oldHS, oldStatus)
			if err := stageHostStatus(batch, oldHS); err != nil {
				return err
			}
		}

		newHS, _, err := loadHostStatus(s.db, normalizedHost)
		if err != nil {
			return err
		}
		newHS.Total++
		if rs.Active {
			newHS.Active++
		}
		incrementStatus(newHS, rs.Backfill.Status)
		if err := stageHostStatus(batch, newHS); err != nil {
			return err
		}
	}
	if err := s.db.Commit(batch, store.SyncWrites); err != nil {
		return fmt.Errorf("backfill: write identity resolution %s: %w", did, err)
	}
	return nil
}

func handleIndexChanged(a, b string) bool {
	ak, aok := normalizeHandleIndexKey(a)
	bk, bok := normalizeHandleIndexKey(b)
	if aok != bok {
		return true
	}
	if !aok {
		return false
	}
	return string(ak) != string(bk)
}

// OnDiscover writes a fresh RepoStatus at status=not_started for a
// DID the engine has never seen. atmos guarantees this fires at most
// once per DID per Lookup-StateUnknown path.
func (s *Store) OnDiscover(_ context.Context, entry atmossync.ListReposEntry) error {
	rs := &RepoStatus{
		Backfill: RepoBackfillStatus{
			Status:    StatusNotStarted,
			StartedAt: timeNow(),
		},
		Active: entry.Active,
	}
	if err := s.putRepoStatusAndCounts(entry.DID, rs, false, "", nil); err != nil {
		return err
	}
	s.metrics.incDiscovered()
	return nil
}

// OnUpdate flips the Active flag on an existing row. The lifecycle
// Status is preserved — atmos fires OnUpdate only when the
// listRepos.Active value differs from what the Store last saw, and
// it never changes the Status as a side effect.
func (s *Store) OnUpdate(_ context.Context, entry atmossync.ListReposEntry) error {
	if err := s.updateRepoActive(entry.DID, entry.Active); err != nil {
		return err
	}
	s.metrics.incActiveFlips()
	return nil
}

// OnComplete records a successful repo download. The commit's rev is
// stored in both Backfill.Rev (the rev at end of initial download
// per DESIGN.md §3.5) and the top-level Rev (the latest known rev).
// They're equal here because initial backfill is the only thing
// updating Rev in this PR; steady-state ingest will diverge them.
//
// We RMW rather than write fresh so a future field on RepoStatus
// (RecordCount, TotalBytes) added between OnDiscover and OnComplete
// survives. The read, aggregate transition, and durable write must
// stay serialized with deferred completion staging via countsMu.
func (s *Store) OnComplete(ctx context.Context, did atmos.DID, commit *repo.Commit) error {
	if s.completions != nil {
		return s.completions.QueueComplete(ctx, did, commit)
	}

	now := timeNow()
	if err := s.updateRepoStatusAndCounts(did, func(rs *RepoStatus, _ bool, _ Status) (func(*HostStatus), error) {
		rs.Backfill.Status = StatusComplete
		rs.Backfill.Rev = commit.Rev
		rs.Backfill.CompletedAt = now
		rs.Backfill.LastError = ""
		rs.Rev = commit.Rev
		rs.UpdatedAt = now
		rs.LastAttemptedAt = now
		return func(hs *HostStatus) {
			hs.LastAttemptedAt = now
		}, nil
	}); err != nil {
		return err
	}
	if err := s.simulateCrash(ctx, crashpoint.AfterRepoComplete); err != nil {
		return err
	}
	if s.afterComplete != nil {
		if err := s.afterComplete(ctx, did); err != nil {
			err = fmt.Errorf("backfill: after complete hook %s: %w", did, err)
			if s.afterCompleteError != nil {
				s.afterCompleteError(err)
			}
			return err
		}
	}
	s.metrics.incCompleted()
	return nil
}

func (s *Store) simulateCrash(ctx context.Context, point crashpoint.Point) error {
	if s.crashInjector == nil {
		return nil
	}
	return s.crashInjector.SimulateCrash(ctx, point)
}

// OnFail records a failed repo download. atmos passes the total
// attempt count for the current Run (initial + retries). We overwrite
// rather than accumulate across Runs — DESIGN.md §6.3 calls out
// resetting Attempts on failover as an acceptable cosmetic regression.
//
// CompletedAt and Backfill.Rev from a prior successful Run are
// preserved. This is defensive — within this PR the engine never
// retries a StateComplete DID — but it keeps a hypothetical future
// "complete then later failed" trail intact.
func (s *Store) OnFail(ctx context.Context, did atmos.DID, failErr error, attempts int) error {
	if errors.Is(failErr, errIdentityDiagnosticsPersistence) {
		s.metrics.incOnFailErrors()
		return failErr
	}
	if err := ctx.Err(); err != nil {
		s.metrics.incOnFailErrors()
		return err
	}

	now := timeNow()
	if isRepoNotFoundError(failErr) {
		return s.updateRepoStatusAndCounts(did, func(rs *RepoStatus, _ bool, _ Status) (func(*HostStatus), error) {
			rs.Backfill.Status = StatusComplete
			rs.Backfill.LastError = ""
			rs.Backfill.Attempts = 0
			rs.Backfill.CompletedAt = now
			rs.LastAttemptedAt = now
			return func(hs *HostStatus) {
				hs.LastAttemptedAt = now
			}, nil
		})
	}
	if isRepoUnavailableError(failErr) {
		// The account exists but its repo is deactivated/suspended/
		// taken down. This is a terminal upstream state, not a download
		// failure: record it as unavailable so the engine stops
		// retrying (Lookup -> StateComplete) and dashboards don't count
		// it as a failed host. Clear LastError/Attempts so a row that
		// previously failed for another reason doesn't carry a stale
		// diagnostic into its terminal state.
		return s.updateRepoStatusAndCounts(did, func(rs *RepoStatus, _ bool, _ Status) (func(*HostStatus), error) {
			rs.Backfill.Status = StatusUnavailable
			rs.Backfill.LastError = ""
			rs.Backfill.Attempts = 0
			rs.LastAttemptedAt = now
			return func(hs *HostStatus) {
				hs.LastAttemptedAt = now
			}, nil
		})
	}

	errMsg := ""
	if failErr != nil {
		errMsg = failErr.Error()
	}
	errMsg = truncateErrorString(errMsg)
	errClass := classifyBackfillError(failErr)
	if err := s.updateRepoStatusAndCounts(did, func(rs *RepoStatus, _ bool, _ Status) (func(*HostStatus), error) {
		rs.Backfill.Status = StatusFailed
		rs.Backfill.LastError = errMsg
		rs.Backfill.Attempts = attempts
		rs.LastAttemptedAt = now
		return func(hs *HostStatus) {
			hs.LastAttemptedAt = now
			hs.addErrorSample(HostErrorSample{
				DID:         did,
				AttemptedAt: now,
				Class:       errClass,
				Error:       errMsg,
			})
		}, nil
	}); err != nil {
		s.metrics.incOnFailErrors()
		return err
	}
	s.metrics.incFailed()
	return nil
}

// timeNow is a package var so tests can pin wall-clock values.
// Production callers don't override this.
var timeNow = func() time.Time { return time.Now().UTC() }
