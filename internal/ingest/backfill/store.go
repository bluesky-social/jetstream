// store.go implements the atmos backfill.Store
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

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/store"
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
	rosterMu           sync.Mutex
	completions        *completionBatcher
	runMu              sync.Mutex
	discoveredThisRun  map[atmos.DID]struct{}
}

type atmosStoreAdapter struct{ *Store }

func (a atmosStoreAdapter) OnDiscover(ctx context.Context, host string, entry atmossync.ListReposEntry) error {
	return a.onDiscover(ctx, host, entry)
}

func (a atmosStoreAdapter) OnUpdate(ctx context.Context, host string, entry atmossync.ListReposEntry) error {
	return a.onUpdate(ctx, host, entry)
}

var _ atmosbackfill.Store = atmosStoreAdapter{}

func (s *Store) AtmosStore() atmosbackfill.Store { return atmosStoreAdapter{s} }

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
func (s *Store) Lookup(ctx context.Context, did atmos.DID) (atmosbackfill.StoreEntry, error) {
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
		if s.discoveredInThisRun(did) {
			st = atmosbackfill.StateDiscovered
			break
		}
		// A pre-existing not_started row means this download may be replaying
		// after a crash. Re-downloading through the bootstrap writer would
		// assign low seqs; stale account/sync tombstones from bootstrap-live can
		// then merge above those rows and erase them (#262). Defer it to the
		// post-merge pending retry pass instead, where the whole-repo
		// replacement lands above the captured live tail.
		if err := s.deferInterruptedBootstrapRepo(ctx, did); err != nil {
			return atmosbackfill.StoreEntry{}, err
		}
		st = atmosbackfill.StateComplete
	case StatusPending:
		// Pending rows are handled only by the explicit post-merge retry pass.
		// Do not dispatch them through atmos bootstrap: a live first sighting
		// is not evidence that Jetstream owns a re-download, and a crash-
		// recovery pending row must land above the captured live tail.
		st = atmosbackfill.StateComplete
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

func (s *Store) markDiscoveredThisRun(did atmos.DID) {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	if s.discoveredThisRun == nil {
		s.discoveredThisRun = make(map[atmos.DID]struct{})
	}
	s.discoveredThisRun[did] = struct{}{}
}

func (s *Store) discoveredInThisRun(did atmos.DID) bool {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	_, ok := s.discoveredThisRun[did]
	return ok
}

func (s *Store) deferInterruptedBootstrapRepo(ctx context.Context, did atmos.DID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.updateRepoStatusAndCounts(did, func(rs *RepoStatus, _ bool, old Status) (func(*HostStatus), error) {
		if old != StatusNotStarted {
			return nil, nil
		}
		rs.Backfill.Status = StatusPending
		rs.Backfill.LastError = ""
		rs.Backfill.Attempts = 0
		rs.Backfill.RetryCount = 0
		rs.Backfill.NextAttemptAt = time.Time{}
		return nil, nil
	})
}

// putRepoStatus writes the value durably. It is a test-only setter that
// writes the repo/<did> row in isolation, skipping the counts and host
// aggregate maintenance that production write paths perform; production
// code goes through putRepoStatusAndCounts (and the RMW helpers built on
// it) instead.
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
	rosterHostname string,
) error {
	s.countsMu.Lock()
	defer s.countsMu.Unlock()
	s.rosterMu.Lock()
	defer s.rosterMu.Unlock()
	return s.putRepoStatusAndCountsLocked(did, rs, hadRow, old, updateHost, rosterHostname)
}

// putRepoStatusAndCountsLocked is putRepoStatusAndCounts's body with the
// caller already holding countsMu.
func (s *Store) putRepoStatusAndCountsLocked(
	did atmos.DID,
	rs *RepoStatus,
	hadRow bool,
	old Status,
	updateHost func(*HostStatus),
	rosterHostname string,
) error {
	enc, err := encodeRepoStatus(rs)
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
		// putRepoStatusAndCounts is the create path (OnDiscover): the
		// caller passes a freshly built rs, so any host present is a
		// first sighting for that bucket.
		applyHostStatusTransition(hs, firstInHostBucket(hadRow, "", rs.Host), rs.Active, old, rs.Backfill.Status)
		if updateHost != nil {
			updateHost(hs)
		}
		if err := stageHostStatus(batch, hs); err != nil {
			return err
		}
	}
	if rosterHostname != "" {
		roster, _, err := s.loadPDSHost(rosterHostname)
		if err != nil {
			return err
		}
		roster.ActualAccounts++
		roster.UpdatedAt = timeNow()
		rosterEnc, err := encodePDSHost(roster)
		if err != nil {
			return err
		}
		if err := batch.Set(pdsHostKey(rosterHostname), rosterEnc, nil); err != nil {
			return fmt.Errorf("backfill: stage pdshost/%s: %w", rosterHostname, err)
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
	oldHost := ""
	oldActive := false
	if rs == nil {
		rs = &RepoStatus{}
	} else {
		old = rs.Backfill.Status
		oldHost = rs.Host
		oldActive = rs.Active
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
	// A steady-state retry can re-attribute a DID to a different host than
	// its prior terminal transition recorded (the relay can 302 to a
	// different PDS on a later attempt). When that happens we must decrement
	// the stale bucket — otherwise the DID is counted under both hosts.
	// Mirrors the host-move handling in recordIdentityResolution. (The
	// bootstrap path never moves a host post-terminal, so this is a no-op
	// there.)
	if oldHost != "" && oldHost != rs.Host {
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
		decrementStatus(oldHS, old)
		if err := stageHostStatus(batch, oldHS); err != nil {
			return err
		}
	}
	if rs.Host != "" {
		hs, _, err := loadHostStatus(s.db, rs.Host)
		if err != nil {
			return err
		}
		applyHostStatusTransition(hs, firstInHostBucket(hadRow, oldHost, rs.Host), rs.Active, old, rs.Backfill.Status)
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

func (s *Store) stageDurableBatch(ctx context.Context, batch *pebble.Batch, completions []queuedCompletion, cursors []queuedHostCursor) (func(error), error) {
	s.countsMu.Lock()
	s.rosterMu.Lock()
	locked := true
	unlock := func(error) {
		if locked {
			locked = false
			s.rosterMu.Unlock()
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
	if len(completions) == 0 && len(cursors) == 0 {
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
		oldHost := rs.Host
		rs.Backfill.Status = StatusComplete
		rs.Backfill.Rev = c.commit.Rev
		rs.Backfill.CompletedAt = c.completed
		rs.Backfill.LastError = ""
		rs.Backfill.Attempts = 0
		rs.Backfill.RetryCount = 0
		rs.Backfill.NextAttemptAt = time.Time{}
		rs.Rev = c.commit.Rev
		rs.UpdatedAt = c.completed
		rs.LastAttemptedAt = c.completed
		// Record the host the CAR was downloaded from (post-redirect),
		// replacing the identity-resolution side effect. Preserve any
		// existing bucket when the transport surfaced no host.
		//
		// With discovery-time PDS stamping, oldHost can differ from the
		// completing host (cross-host dedup during a migration window):
		// decrement the stale bucket so the DID is not counted twice.
		// Mirrors updateRepoStatusAndCounts' host-move handling.
		if bucket, ok := hostBucketFromAuthority(c.host); ok {
			if oldHost != "" && oldHost != bucket {
				oldHS := hostCache[oldHost]
				if oldHS == nil {
					oldHS, _, err = loadHostStatus(s.db, oldHost)
					if err != nil {
						return fail(err)
					}
					hostCache[oldHost] = oldHS
				}
				if oldHS.Total > 0 {
					oldHS.Total--
				}
				if rs.Active && oldHS.Active > 0 {
					oldHS.Active--
				}
				decrementStatus(oldHS, old)
			}
			rs.Host = bucket
		}
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
			applyHostStatusTransition(hs, firstInHostBucket(hadRow, oldHost, rs.Host), rs.Active, old, StatusComplete)
			hs.LastAttemptedAt = c.completed
		}
	}
	for _, hs := range hostCache {
		if err := stageHostStatus(batch, hs); err != nil {
			return fail(err)
		}
	}
	for _, cursor := range cursors {
		host, _, err := s.loadPDSHost(cursor.host)
		if err != nil {
			return fail(err)
		}
		oldState := atmosbackfill.HostState(host.State)
		host.State = string(cursor.state)
		switch cursor.state {
		case atmosbackfill.HostStateRunning:
			host.ListReposCursor = cursor.cursor
			if cursor.cursor != "" {
				host.LastNonEmptyCursor = cursor.cursor
			}
		case atmosbackfill.HostStateDrained:
			host.Enumerated = true
			host.ListReposCursor = ""
			host.LastNonEmptyCursor = cursor.lastNonEmpty
			host.Attempts = 0
			host.LastError = ""
			host.NextAttemptAt = time.Time{}
		case atmosbackfill.HostStateExhausted:
			// An exhausted host resumes its partial crawl from the last
			// durable checkpoint on the next run (matches
			// updateHostStateDirect). cursor.cursor is non-empty only when
			// the batcher folded a still-pending Running checkpoint into
			// this record (queueHostState); otherwise preserve the on-disk
			// value rather than erasing the resume point.
			if cursor.cursor != "" {
				host.ListReposCursor = cursor.cursor
			}
			host.Enumerated = false
			host.Attempts = cursor.attempts
			host.LastError = cursor.lastError
		}
		host.UpdatedAt = timeNow()
		applyHostCountTransition(&counts, oldState, cursor.state)
		enc, err := encodePDSHost(host)
		if err != nil {
			return fail(err)
		}
		if err := batch.Set(pdsHostKey(cursor.host), enc, nil); err != nil {
			return fail(fmt.Errorf("backfill: stage pdshost/%s cursor: %w", cursor.host, err))
		}
	}
	countsEnc, err := encodeCounts(counts)
	if err != nil {
		return fail(err)
	}
	if err := batch.Set([]byte(countsKey), countsEnc, nil); err != nil {
		return fail(fmt.Errorf("backfill: stage counts: %w", err))
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

func applyHostCountTransition(counts *Counts, old, next atmosbackfill.HostState) {
	if old == next {
		return
	}
	switch old {
	case atmosbackfill.HostStateDrained:
		if counts.HostsDrained > 0 {
			counts.HostsDrained--
		}
	case atmosbackfill.HostStateExhausted:
		if counts.HostsExhausted > 0 {
			counts.HostsExhausted--
		}
	}
	switch next {
	case atmosbackfill.HostStateDrained:
		counts.HostsDrained++
	case atmosbackfill.HostStateExhausted:
		counts.HostsExhausted++
	}
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
	case StatusPending:
		return &c.Pending
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

// applyHostStatusTransition folds one repo's status change into its host
// aggregate. firstInBucket is true when this DID is being counted under
// this host for the first time — which, now that the backfill path learns
// a DID's host only at its terminal OnComplete/OnFail (no identity
// resolution at discovery), is the common case: the repo row already
// exists but was never attributed to any host. On a first sighting we
// add the DID to the bucket (Total++, Active++, increment its status);
// otherwise it is a status move within the same bucket.
//
// A DID changing host buckets (old non-empty host != new host) is not
// expected on the backfill path — host is assigned once at the terminal
// transition and never rewritten — so the stale-bucket decrement is
// intentionally not handled here.
func applyHostStatusTransition(h *HostStatus, firstInBucket bool, active bool, old, next Status) {
	if firstInBucket {
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

// firstInHostBucket reports whether a DID is being counted under bucket
// for the first time, given whether its repo row already existed and the
// host it was previously attributed to (empty if none). True when the row
// is new, or when it had no host / a different host than bucket.
func firstInHostBucket(hadRow bool, oldHostBucket, bucket string) bool {
	return !hadRow || oldHostBucket != bucket
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

func (s *Store) updateRepoHostActive(did atmos.DID, pds string, active bool) error {
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
	oldHost := rs.Host
	rs.Active = active
	// A hostless update (legacy selected-repo path) flips Active only; it
	// must not erase a discovery- or identity-derived PDS stamp — losing it
	// silently downgrades future retries to the relay fallback.
	if pds != "" {
		rs.PDS = pds
		if newHost, ok := hostBucketFromAuthority(pds); ok {
			rs.Host = newHost
		}
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
	if oldHost != "" && oldHost != rs.Host {
		hs, _, err := loadHostStatus(s.db, oldHost)
		if err != nil {
			return err
		}
		if hs.Total > 0 {
			hs.Total--
		}
		if oldActive && hs.Active > 0 {
			hs.Active--
		}
		decrementStatus(hs, rs.Backfill.Status)
		if err := stageHostStatus(batch, hs); err != nil {
			return err
		}
	}
	if rs.Host != "" {
		hs, _, err := loadHostStatus(s.db, rs.Host)
		if err != nil {
			return err
		}
		if oldHost != rs.Host {
			hs.Total++
			if rs.Active {
				hs.Active++
			}
			incrementStatus(hs, rs.Backfill.Status)
		} else {
			applyHostActiveTransition(hs, oldActive, rs.Active)
		}
		if err := stageHostStatus(batch, hs); err != nil {
			return err
		}
	}
	if err := s.db.Commit(batch, store.SyncWrites); err != nil {
		return fmt.Errorf("backfill: write repo/%s and host active: %w", did, err)
	}
	return nil
}

func (s *Store) updateRepoActive(did atmos.DID, active bool) error {
	return s.updateRepoHostActive(did, "", active)
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
func (s *Store) onDiscover(_ context.Context, host string, entry atmossync.ListReposEntry) error {
	bucket, _ := hostBucketFromAuthority(host)
	rs := &RepoStatus{
		Backfill: RepoBackfillStatus{
			Status:    StatusNotStarted,
			StartedAt: timeNow(),
		},
		PDS: host, Host: bucket, Active: entry.Active,
	}
	if err := s.putRepoStatusAndCounts(entry.DID, rs, false, "", nil, host); err != nil {
		return err
	}
	s.markDiscoveredThisRun(entry.DID)
	s.metrics.incDiscovered()
	return nil
}

// OnDiscover is retained for Jetstream's selected-repo and focused store
// paths, which do not have a listHosts mapping. Fleet callers use AtmosStore.
func (s *Store) OnDiscover(ctx context.Context, entry atmossync.ListReposEntry) error {
	return s.onDiscover(ctx, "", entry)
}

// OnDiscoverForRetry records a merge-discovery-only marker for an unknown DID.
// Steady-state retry owns the eventual direct-PDS download.
func (s *Store) OnDiscoverForRetry(_ context.Context, host string, entry atmossync.ListReposEntry) error {
	bucket, _ := hostBucketFromAuthority(host)
	rs := &RepoStatus{
		Backfill: RepoBackfillStatus{Status: StatusFailed, LastError: "discovered post-bootstrap; queued for retry"},
		PDS:      host, Host: bucket, Active: entry.Active,
	}
	return s.putRepoStatusAndCounts(entry.DID, rs, false, "", nil, host)
}

// HostDiscoveryCursor reopens a drained/exhausted host at its last non-empty
// bootstrap cursor so merge discovery scans only tail pages.
func (s *Store) HostDiscoveryCursor(ctx context.Context, hostname string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	s.rosterMu.Lock()
	defer s.rosterMu.Unlock()
	host, _, err := s.loadPDSHost(hostname)
	if err != nil {
		return "", false, err
	}
	return host.LastNonEmptyCursor, false, nil
}

// OnUpdate flips the Active flag on an existing row. The lifecycle
// Status is preserved — atmos fires OnUpdate only when the
// listRepos.Active value differs from what the Store last saw, and
// it never changes the Status as a side effect.
func (s *Store) onUpdate(_ context.Context, host string, entry atmossync.ListReposEntry) error {
	if err := s.updateRepoHostActive(entry.DID, host, entry.Active); err != nil {
		return err
	}
	s.metrics.incActiveFlips()
	return nil
}

func (s *Store) OnUpdate(ctx context.Context, entry atmossync.ListReposEntry) error {
	return s.onUpdate(ctx, "", entry)
}

func (s *Store) loadPDSHost(hostname string) (*PDSHost, bool, error) {
	val, closer, err := s.db.Get(pdsHostKey(hostname))
	if errors.Is(err, store.ErrNotFound) {
		return &PDSHost{Hostname: hostname}, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("backfill: load pdshost/%s: %w", hostname, err)
	}
	defer func() { _ = closer.Close() }()
	host, err := decodePDSHost(val)
	if err != nil {
		return nil, false, fmt.Errorf("backfill: load pdshost/%s: %w", hostname, err)
	}
	return host, true, nil
}

func (s *Store) savePDSHost(host *PDSHost) error {
	enc, err := encodePDSHost(host)
	if err != nil {
		return err
	}
	if err := s.db.Set(pdsHostKey(host.Hostname), enc, store.SyncWrites); err != nil {
		return fmt.Errorf("backfill: write pdshost/%s: %w", host.Hostname, err)
	}
	return nil
}

func (s *Store) OnHost(ctx context.Context, info atmosbackfill.HostInfo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.rosterMu.Lock()
	defer s.rosterMu.Unlock()
	host, exists, err := s.loadPDSHost(info.Hostname)
	if err != nil {
		return err
	}
	now := timeNow()
	if !exists {
		host.FirstSeenAt = now
		host.State = string(atmosbackfill.HostStatePending)
	}
	host.RelayStatus = info.RelayStatus
	host.RelayAccounts = info.RelayAccounts
	host.Seq = info.Seq
	host.UpdatedAt = now
	return s.savePDSHost(host)
}

func (s *Store) HostCursor(ctx context.Context, hostname string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	s.rosterMu.Lock()
	defer s.rosterMu.Unlock()
	host, _, err := s.loadPDSHost(hostname)
	if err != nil {
		return "", false, err
	}
	return host.ListReposCursor, host.Enumerated, nil
}

func (s *Store) SaveHostCursor(ctx context.Context, hostname, cursor string) error {
	if s.completions != nil {
		return s.completions.QueueHostCursor(ctx, hostname, cursor)
	}
	return s.saveHostCursorDirect(ctx, hostname, cursor)
}

func (s *Store) saveHostCursorDirect(ctx context.Context, hostname, cursor string) error {
	return s.updateHostStateDirect(ctx, hostname, func(host *PDSHost) atmosbackfill.HostState {
		host.ListReposCursor = cursor
		if cursor != "" {
			host.LastNonEmptyCursor = cursor
		}
		return atmosbackfill.HostStateRunning
	})
}

func (s *Store) OnHostDrained(ctx context.Context, hostname, lastNonEmptyCursor string) error {
	if s.completions != nil {
		return s.completions.QueueHostDrained(ctx, hostname, lastNonEmptyCursor)
	}
	return s.updateHostStateDirect(ctx, hostname, func(host *PDSHost) atmosbackfill.HostState {
		host.Enumerated = true
		host.ListReposCursor = ""
		host.LastNonEmptyCursor = lastNonEmptyCursor
		host.Attempts = 0
		host.LastError = ""
		host.NextAttemptAt = time.Time{}
		return atmosbackfill.HostStateDrained
	})
}

func (s *Store) OnHostExhausted(ctx context.Context, hostname string, cause error, attempts int) error {
	if s.completions != nil {
		return s.completions.QueueHostExhausted(ctx, hostname, cause, attempts)
	}
	return s.updateHostStateDirect(ctx, hostname, func(host *PDSHost) atmosbackfill.HostState {
		host.Enumerated = false
		host.Attempts = attempts
		if cause != nil {
			host.LastError = truncateErrorString(cause.Error())
		}
		return atmosbackfill.HostStateExhausted
	})
}

func (s *Store) updateHostStateDirect(ctx context.Context, hostname string, mutate func(*PDSHost) atmosbackfill.HostState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.countsMu.Lock()
	defer s.countsMu.Unlock()
	s.rosterMu.Lock()
	defer s.rosterMu.Unlock()

	host, _, err := s.loadPDSHost(hostname)
	if err != nil {
		return err
	}
	oldState := atmosbackfill.HostState(host.State)
	nextState := mutate(host)
	host.State = string(nextState)
	host.UpdatedAt = timeNow()

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
	applyHostCountTransition(&counts, oldState, nextState)
	hostEnc, err := encodePDSHost(host)
	if err != nil {
		return err
	}
	countsEnc, err := encodeCounts(counts)
	if err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.Set(pdsHostKey(hostname), hostEnc, nil); err != nil {
		return fmt.Errorf("backfill: stage pdshost/%s state: %w", hostname, err)
	}
	if err := batch.Set([]byte(countsKey), countsEnc, nil); err != nil {
		return fmt.Errorf("backfill: stage counts: %w", err)
	}
	if err := s.db.Commit(batch, store.SyncWrites); err != nil {
		return fmt.Errorf("backfill: commit pdshost/%s state: %w", hostname, err)
	}
	return nil
}

// OnComplete records a successful repo download. The commit's rev is
// stored in both Backfill.Rev (the rev at end of initial download
// per docs/README.md §3.5) and the top-level Rev (the latest known rev).
// They're equal here because initial backfill is the only thing
// updating Rev in this PR; steady-state ingest will diverge them.
//
// We RMW rather than write fresh so a future field on RepoStatus
// (RecordCount, TotalBytes) added between OnDiscover and OnComplete
// survives. The read, aggregate transition, and durable write must
// stay serialized with deferred completion staging via countsMu.
func (s *Store) OnComplete(ctx context.Context, did atmos.DID, host string, commit *repo.Commit) error {
	if s.completions != nil {
		return s.completions.QueueComplete(ctx, did, host, commit)
	}

	now := timeNow()
	bucket, hasBucket := hostBucketFromAuthority(host)
	if err := s.updateRepoStatusAndCounts(did, func(rs *RepoStatus, _ bool, _ Status) (func(*HostStatus), error) {
		rs.Backfill.Status = StatusComplete
		rs.Backfill.Rev = commit.Rev
		rs.Backfill.CompletedAt = now
		rs.Backfill.LastError = ""
		rs.Backfill.Attempts = 0
		rs.Backfill.RetryCount = 0
		rs.Backfill.NextAttemptAt = time.Time{}
		rs.Rev = commit.Rev
		rs.UpdatedAt = now
		rs.LastAttemptedAt = now
		// Record the host the CAR was downloaded from (post-redirect),
		// replacing the identity-resolution side effect that used to
		// populate this. Preserve any existing bucket if the transport
		// did not surface a host. rs.PDS is deliberately untouched: the
		// selected path stamps an identity-derived endpoint URL there, and
		// the retry runner repairs stale stamps explicitly (restampPDS)
		// only when a relay fallback proved the old route dead.
		if hasBucket {
			rs.Host = bucket
		}
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
// rather than accumulate across Runs; resetting Attempts on failover
// is an acceptable cosmetic regression.
//
// CompletedAt and Backfill.Rev from a prior successful Run are
// preserved. This is defensive — within this PR the engine never
// retries a StateComplete DID — but it keeps a hypothetical future
// "complete then later failed" trail intact.
func (s *Store) OnFail(ctx context.Context, did atmos.DID, host string, failErr error, attempts int) error {
	if err := ctx.Err(); err != nil {
		s.metrics.incOnFailErrors()
		return err
	}

	now := timeNow()
	// host is the server the failing request was sent to (post-redirect),
	// or "" when no response was received (e.g. a dial failure). Record it
	// so per-host attribution survives without identity resolution;
	// preserve any existing bucket when host is empty.
	bucket, hasBucket := hostBucketFromAuthority(host)
	setHost := func(rs *RepoStatus) {
		if hasBucket {
			rs.Host = bucket
		}
	}
	if isRepoNotFoundError(failErr) {
		return s.updateRepoStatusAndCounts(did, func(rs *RepoStatus, _ bool, _ Status) (func(*HostStatus), error) {
			rs.Backfill.Status = StatusComplete
			rs.Backfill.LastError = ""
			rs.Backfill.Attempts = 0
			rs.Backfill.RetryCount = 0
			rs.Backfill.NextAttemptAt = time.Time{}
			rs.Backfill.CompletedAt = now
			rs.LastAttemptedAt = now
			setHost(rs)
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
			rs.Backfill.RetryCount = 0
			rs.Backfill.NextAttemptAt = time.Time{}
			rs.LastAttemptedAt = now
			setHost(rs)
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
		setHost(rs)
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

// RecordRetryFailure records one steady-state failed-repo retry attempt and
// persists when that DID is next eligible. Unlike OnFail, this is intentionally
// not part of the atmos bootstrap Store contract: periodic steady-state retry
// has its own long-lived backoff state that must survive process restarts.
func (s *Store) RecordRetryFailure(ctx context.Context, did atmos.DID, host string, failErr error, nextAttemptAt time.Time) error {
	if err := ctx.Err(); err != nil {
		s.metrics.incOnFailErrors()
		return err
	}

	now := timeNow()
	bucket, hasBucket := hostBucketFromAuthority(host)
	setHost := func(rs *RepoStatus) {
		if hasBucket {
			rs.Host = bucket
		}
	}

	if isRepoNotFoundError(failErr) || isRepoUnavailableError(failErr) {
		return s.OnFail(ctx, did, host, failErr, 1)
	}

	errMsg := ""
	if failErr != nil {
		errMsg = failErr.Error()
	}
	errMsg = truncateErrorString(errMsg)
	errClass := classifyBackfillError(failErr)
	recorded := false
	if err := s.updateRepoStatusAndCounts(did, func(rs *RepoStatus, hadRow bool, old Status) (func(*HostStatus), error) {
		if !hadRow {
			return nil, fmt.Errorf("backfill: retry failure %s: missing row", did)
		}
		// Guard against a concurrent terminal transition (e.g. a completion
		// that raced this attempt): only record a failure for a row still
		// selected by a retry pass.
		if !isRetryFailureRecordableStatus(old) {
			return nil, nil
		}
		rs.Backfill.Status = StatusFailed
		rs.Backfill.LastError = errMsg
		rs.Backfill.Attempts++
		rs.Backfill.RetryCount++
		rs.Backfill.NextAttemptAt = nextAttemptAt.UTC()
		rs.LastAttemptedAt = now
		setHost(rs)
		recorded = true
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
	if recorded {
		s.metrics.incRetryFailed()
	}
	return nil
}

// DeferRetryAttempt persists host-level backpressure for a due retry candidate
// that was not actually attempted because another repo on the same host
// received a rate-limit response. It intentionally does not increment Attempts,
// RetryCount, LastAttemptedAt, or host error samples — the repo keeps its
// current status and is simply rescheduled past the host's parked-until instant.
func (s *Store) DeferRetryAttempt(ctx context.Context, did atmos.DID, nextAttemptAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.countsMu.Lock()
	defer s.countsMu.Unlock()

	rs, err := s.readRepoStatus(did)
	if err != nil {
		return err
	}
	if rs == nil {
		return fmt.Errorf("backfill: defer retry %s: missing row", did)
	}
	if !isRetryFailureRecordableStatus(rs.Backfill.Status) {
		return nil
	}
	next := nextAttemptAt.UTC()
	if !rs.Backfill.NextAttemptAt.IsZero() && rs.Backfill.NextAttemptAt.After(next) {
		return nil
	}
	rs.Backfill.NextAttemptAt = next

	enc, err := encodeRepoStatus(rs)
	if err != nil {
		return err
	}
	if err := s.db.Set(repoKey(did), enc, store.SyncWrites); err != nil {
		return fmt.Errorf("backfill: defer retry repo/%s: %w", did, err)
	}
	return nil
}

// timeNow is a package var so tests can pin wall-clock values.
// Production callers don't override this.
var timeNow = func() time.Time { return time.Now().UTC() }
