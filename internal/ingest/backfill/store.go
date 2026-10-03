// store.go implements the atmos backfill.Store
// interface against the metadata store. Keys live at repo/<did>; values are the
// JSON-encoded RepoStatus from status.go.
//
// All callbacks the engine fires (OnDiscover, OnUpdate, OnComplete,
// OnFail) commit durably to satisfy atmos's durability
// contract: the engine treats a successful return as durable.
//
// atmos's Store takes its enumeration callbacks a page at a time, so the
// engine reconciles a listRepos page and records a listHosts page in a
// constant number of metadata round trips. In disaggregated mode each round
// trip crosses the network to PostgreSQL and each commit is a fenced catalog
// transaction, so per-entry callbacks would bound a large PDS's enumeration
// at a few repos a second. Concurrent page writes from different hosts also share one
// transaction (groupCommitter), and every locked read-modify-write reads
// through a prefetched metaView rather than one key at a time.
//
// Whole-row read-modify-write paths preserve fields a future PR may
// add to RepoStatus (e.g. RecordCount). They stage under countsMu with
// aggregate writes and deferred completion staging, reading every write
// staged ahead of them, so a stale callback cannot overwrite a
// just-completed row. Commits run after the lock is released, in staging
// order (commitPipe), so no writer waits out another's commit to stage.

package backfill

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
)

// Store implements atmosbackfill.Store against the shared metadata
// store. Construct via NewStore.
type Store struct {
	db                 metastore.Store
	metrics            *Metrics
	afterComplete      func(context.Context, atmos.DID) error
	afterCompleteError func(error)
	crashInjector      crashpoint.Injector
	countsMu           chanMutex
	rosterMu           chanMutex
	pipe               commitPipe
	group              groupCommitter
	// hookOpen is set while a durable-batch hook's staged write awaits its
	// commit (stageDurableBatch).
	hookOpen          atomic.Bool
	completions       *completionBatcher
	runMu             sync.Mutex
	discoveredThisRun map[atmos.DID]struct{}
}

// atmosStoreAdapter is the atmos backfill.Store view of Store. atmos's
// enumeration callbacks take a page at a time; Store's own same-named
// methods are the per-item forms Jetstream's other callers use.
type atmosStoreAdapter struct{ *Store }

func (a atmosStoreAdapter) Lookup(ctx context.Context, dids []atmos.DID) ([]atmosbackfill.StoreEntry, error) {
	return a.LookupBatch(ctx, dids)
}

func (a atmosStoreAdapter) OnDiscover(ctx context.Context, host string, entries []atmossync.ListReposEntry) error {
	return a.reconcileBatch(ctx, host, entries, nil, discoverBootstrap)
}

func (a atmosStoreAdapter) OnUpdate(ctx context.Context, host string, entries []atmossync.ListReposEntry) error {
	return a.reconcileBatch(ctx, host, nil, entries, discoverBootstrap)
}

func (a atmosStoreAdapter) OnHost(ctx context.Context, hosts []atmosbackfill.HostInfo) error {
	return a.recordHosts(ctx, hosts)
}

func (a atmosStoreAdapter) HostCursor(ctx context.Context, hostnames []string) ([]atmosbackfill.HostCursorState, error) {
	return a.hostCursors(ctx, hostnames, bootstrapHostCursor)
}

var _ atmosbackfill.Store = atmosStoreAdapter{}

func (s *Store) AtmosStore() atmosbackfill.Store { return atmosStoreAdapter{s} }

// NewStore constructs a Store backed by the shared metadata store.
// metrics may be nil; callbacks are no-ops in that case. Call SeedCounts
// before the first write.
func NewStore(db metastore.Store, metrics *Metrics) *Store {
	return &Store{db: db, metrics: metrics, countsMu: newChanMutex(), rosterMu: newChanMutex()}
}

// chanMutex is a mutex built on a channel. countsMu and rosterMu are held
// across metadata reads, and in disaggregated mode a read is a catalog
// round trip. testing/synctest counts a goroutine waiting on a channel as
// durably blocked but not one waiting on a sync.Mutex, so the layer 3
// oracle's seeded scheduler, which admits the next catalog call only once
// every goroutine is durably blocked, would wedge on a sync.Mutex here.
type chanMutex chan struct{}

func newChanMutex() chanMutex { return make(chanMutex, 1) }

func (m chanMutex) Lock() { m <- struct{}{} }

func (m chanMutex) Unlock() {
	select {
	case <-m:
	default:
		panic("backfill: unlock of unlocked chanMutex")
	}
}

// lockedView returns a view for a read-modify-write under countsMu: it
// sees every write staged ahead of it, committed or not (commitPipe). Pass
// the reader to commitPipe.stage.
func (s *Store) lockedView() (*metaView, *pipeReader) {
	r := &pipeReader{Store: s.db, p: &s.pipe}
	return newMetaView(r), r
}

// writeLocked is the shape of every read-modify-write of the shared rows:
// stage under countsMu (and rosterMu with roster) through a lockedView, then
// commit after releasing the locks, in staging order. keys are prefetched
// in one read. A commit error is wrapped with what.
func (s *Store) writeLocked(
	ctx context.Context,
	roster bool,
	what string,
	keys [][]byte,
	stage func(view *metaView, batch metastore.Batch) error,
) error {
	t, ops, err := s.stageLocked(ctx, roster, keys, stage)
	if err != nil || t == nil {
		return err
	}
	if err := t.commit(func() error { return applyOps(ctx, s.db, ops) }); err != nil {
		return fmt.Errorf("backfill: %s: %w", what, err)
	}
	return nil
}

// stageLocked runs stage under the locks and stages its ops in the pipe. It
// returns no ticket when stage wrote nothing.
func (s *Store) stageLocked(
	ctx context.Context,
	roster bool,
	keys [][]byte,
	stage func(view *metaView, batch metastore.Batch) error,
) (*pipeTicket, []metastore.Op, error) {
	s.countsMu.Lock()
	defer s.countsMu.Unlock()
	if roster {
		s.rosterMu.Lock()
		defer s.rosterMu.Unlock()
	}
	view, reader := s.lockedView()
	if err := view.prefetch(ctx, keys); err != nil {
		return nil, nil, err
	}
	local := metastore.NewOpBatch(nil)
	if err := stage(view, view.batch(local)); err != nil {
		return nil, nil, err
	}
	if local.Len() == 0 {
		return nil, nil, nil
	}
	t, err := s.pipe.stage(reader, local.Ops())
	if err != nil {
		return nil, nil, err
	}
	return t, local.Ops(), nil
}

// errCountsNotSeeded means a counts-maintaining write ran before SeedCounts.
// It is an internal error, not a condition to repair in place: tallying repo/
// mid-run races writers in other Store instances, and the metadata iterator
// is not a snapshot, so the tally could be torn and would then be saved
// permanently.
var errCountsNotSeeded = errors.New("backfill: internal error: backfill/counts missing; SeedCounts must run before any repo/ write")

// SeedCounts writes backfill/counts from a full repo/ tally when the row is
// missing (a data dir that predates the counts row). Every entry point that
// writes repo/ rows calls it before its first writer starts, so the tally
// sees a quiescent keyspace. Idempotent: once the row exists it is only
// maintained incrementally.
func (s *Store) SeedCounts(ctx context.Context) error {
	s.countsMu.Lock()
	defer s.countsMu.Unlock()
	_, ok, err := LoadCounts(s.db)
	if err != nil || ok {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	counts, err := CountStatuses(s.db)
	if err != nil {
		return err
	}
	return SaveCounts(s.db, counts)
}

// loadCountsFrom reads backfill/counts for an incremental update through db,
// normally a lockedView under countsMu.
func loadCountsFrom(db metastore.Store) (Counts, error) {
	counts, ok, err := LoadCounts(db)
	if err != nil {
		return Counts{}, err
	}
	if !ok {
		return Counts{}, errCountsNotSeeded
	}
	return counts, nil
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
	val, err := s.db.Get(context.Background(), repoKey(did))
	if errors.Is(err, metastore.ErrNotFound) {
		return atmosbackfill.StoreEntry{State: atmosbackfill.StateUnknown}, nil
	}
	if err != nil {
		return atmosbackfill.StoreEntry{}, fmt.Errorf("backfill: lookup %s: %w", did, err)
	}
	entry, interrupted, err := s.lookupEntry(did, val)
	if err != nil {
		return atmosbackfill.StoreEntry{}, err
	}
	if interrupted {
		if err := s.deferInterruptedBootstrapRepos(ctx, []atmos.DID{did}); err != nil {
			return atmosbackfill.StoreEntry{}, err
		}
	}
	return entry, nil
}

// LookupBatch is Lookup for a listRepos page in one metadata read, deferring
// every interrupted row it finds in one write. It backs atmos's Store.Lookup.
func (s *Store) LookupBatch(ctx context.Context, dids []atmos.DID) ([]atmosbackfill.StoreEntry, error) {
	keys := make([][]byte, len(dids))
	for i, did := range dids {
		keys[i] = repoKey(did)
	}
	vals, err := s.db.GetMany(context.Background(), keys)
	if err != nil {
		return nil, fmt.Errorf("backfill: lookup batch (%d dids): %w", len(dids), err)
	}
	out := make([]atmosbackfill.StoreEntry, len(dids))
	var interrupted []atmos.DID
	for i, did := range dids {
		if vals[i] == nil {
			out[i] = atmosbackfill.StoreEntry{State: atmosbackfill.StateUnknown}
			continue
		}
		entry, deferRow, err := s.lookupEntry(did, vals[i])
		if err != nil {
			return nil, err
		}
		if deferRow {
			interrupted = append(interrupted, did)
		}
		out[i] = entry
	}
	if len(interrupted) > 0 {
		if err := s.deferInterruptedBootstrapRepos(ctx, interrupted); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// lookupEntry projects a stored RepoStatus into atmos's StoreEntry.
// interrupted reports a not_started row from an earlier run, which the caller
// must defer (deferInterruptedBootstrapRepos) before returning the entry.
func (s *Store) lookupEntry(did atmos.DID, val []byte) (atmosbackfill.StoreEntry, bool, error) {
	rs, err := decodeRepoStatus(val)
	if err != nil {
		return atmosbackfill.StoreEntry{}, false, fmt.Errorf("backfill: lookup %s: %w", did, err)
	}

	var st atmosbackfill.State
	interrupted := false
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
		interrupted = true
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
		return atmosbackfill.StoreEntry{}, false, fmt.Errorf("backfill: lookup %s: unknown status %q", did, rs.Backfill.Status)
	}

	return atmosbackfill.StoreEntry{State: st, Active: rs.Active}, interrupted, nil
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

func (s *Store) deferInterruptedBootstrapRepos(ctx context.Context, dids []atmos.DID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.updateRepoStatusesAndCounts(dids, func(rs *RepoStatus, _ bool, old Status) (func(*HostStatus), error) {
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
// code goes through reconcileBatch and updateRepoStatusesAndCounts instead.
func (s *Store) putRepoStatus(did atmos.DID, rs *RepoStatus) error {
	enc, err := encodeRepoStatus(rs)
	if err != nil {
		return err
	}
	if err := s.db.Set(context.Background(), repoKey(did), enc); err != nil {
		return fmt.Errorf("backfill: write repo/%s: %w", did, err)
	}
	return nil
}

// updateRepoStatusAndCounts read-modify-writes one repo row and the
// aggregates its status change moves, in one commit.
func (s *Store) updateRepoStatusAndCounts(
	did atmos.DID,
	mutate func(*RepoStatus, bool, Status) (func(*HostStatus), error),
) error {
	return s.updateRepoStatusesAndCounts([]atmos.DID{did}, mutate)
}

// updateRepoStatusesAndCounts is updateRepoStatusAndCounts for several DIDs
// in one commit. Each DID's update reads the aggregates the previous one
// staged, so the result equals one commit per DID in order.
func (s *Store) updateRepoStatusesAndCounts(
	dids []atmos.DID,
	mutate func(*RepoStatus, bool, Status) (func(*HostStatus), error),
) error {
	ctx := context.Background()
	keys := [][]byte{[]byte(countsKey)}
	for _, did := range dids {
		keys = append(keys, repoKey(did))
	}
	what := fmt.Sprintf("write %d repo rows and counts", len(dids))
	if len(dids) == 1 {
		what = fmt.Sprintf("write repo/%s and counts", dids[0])
	}
	return s.writeLocked(ctx, false, what, keys, func(view *metaView, batch metastore.Batch) error {
		for _, did := range dids {
			if err := s.stageRepoStatusUpdate(ctx, view, batch, did, mutate); err != nil {
				return err
			}
		}
		return nil
	})
}

// stageRepoStatusUpdate stages one repo row's read-modify-write and the
// aggregate transitions it implies, reading through view.
func (s *Store) stageRepoStatusUpdate(
	ctx context.Context,
	view *metaView,
	batch metastore.Batch,
	did atmos.DID,
	mutate func(*RepoStatus, bool, Status) (func(*HostStatus), error),
) error {
	rs, err := readRepoStatusFrom(view, did)
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
	if err := prefetchHostStatuses(ctx, view, oldHost, rs.Host); err != nil {
		return err
	}

	counts, err := loadCountsFrom(view)
	if err != nil {
		return err
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

	batch.Set(repoKey(did), enc)
	batch.Set([]byte(countsKey), countsEnc)
	// A steady-state retry can re-attribute a DID to a different host than
	// its prior terminal transition recorded (the relay can 302 to a
	// different PDS on a later attempt). When that happens we must decrement
	// the stale bucket — otherwise the DID is counted under both hosts.
	// Mirrors the host-move handling in recordIdentityResolution. (The
	// bootstrap path never moves a host post-terminal, so this is a no-op
	// there.)
	if oldHost != "" && oldHost != rs.Host {
		oldHS, _, err := loadHostStatus(view, oldHost)
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
		hs, _, err := loadHostStatus(view, rs.Host)
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
	return nil
}

// prefetchRepoHosts loads, in one read, the host aggregates that the
// entries' existing repo rows are attributed to.
func prefetchRepoHosts(ctx context.Context, view *metaView, entries []atmossync.ListReposEntry) error {
	hosts := make([]string, 0, len(entries))
	for _, entry := range entries {
		rs, err := readRepoStatusFrom(view, entry.DID)
		if err != nil {
			return err
		}
		if rs != nil {
			hosts = append(hosts, rs.Host)
		}
	}
	return prefetchHostStatuses(ctx, view, hosts...)
}

// prefetchHostStatuses loads the aggregates for the given hosts in one read.
// Empty and unnormalizable hosts are skipped; loadHostStatus reports the
// latter when the caller reaches it.
func prefetchHostStatuses(ctx context.Context, view *metaView, hosts ...string) error {
	keys := make([][]byte, 0, len(hosts))
	for _, host := range hosts {
		if host == "" {
			continue
		}
		if _, key, err := normalizeHostStatusKey(host); err == nil {
			keys = append(keys, key)
		}
	}
	return view.prefetch(ctx, keys)
}

func (s *Store) stageDurableBatch(ctx context.Context, batch metastore.Batch, completions []queuedCompletion, cursors []queuedHostCursor) (func(error), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(completions) == 0 && len(cursors) == 0 {
		return nil, nil
	}
	// The staged write waits for every write staged ahead of it, so a second
	// hook call before this one's commit would wait on itself. The segment
	// writers call the hook once per commit; hot mode's multi-batch group
	// commit would not, so refuse it rather than hang.
	if !s.hookOpen.CompareAndSwap(false, true) {
		return nil, errHookReentered
	}
	var t *pipeTicket
	var ops []metastore.Op
	for attempt := 1; ; attempt++ {
		var err error
		t, ops, err = s.stageCompletionsLocked(ctx, completions, cursors)
		if err == nil {
			if err = t.wait(); err != nil {
				t.finish(err)
			}
		}
		if err == nil {
			break
		}
		// A write these rows build on failed, so they build on rows that
		// never landed. Stage again from the store rather than fail the
		// segment writer over another write's failure.
		if _, ok := errors.AsType[*stagedOnFailureError](err); !ok || attempt == maxHookRestages {
			s.hookOpen.Store(false)
			return nil, err
		}
	}
	for _, op := range ops {
		switch op.Kind {
		case metastore.OpSet:
			batch.Set(op.Key, op.Value)
		case metastore.OpDelete:
			batch.Delete(op.Key)
		}
	}
	return func(commitErr error) {
		t.finish(commitErr)
		s.hookOpen.Store(false)
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

// errHookReentered is a durable-batch hook call made before the previous
// call's batch finished (stageDurableBatch).
var errHookReentered = errors.New("backfill: internal error: durable batch hook called again before the previous batch finished; it supports one batch per commit")

// maxHookRestages bounds how many times stageDurableBatch stages again
// after writes staged ahead of it fail. Each retry needs another write's
// commit to fail, so the bound only matters when the store fails every
// commit, and then the segment writer's own commit fails too.
const maxHookRestages = 8

// stageCompletionsLocked stages queued completions and host cursors under
// countsMu and rosterMu, reading through the pipe, and returns the staged
// write's ticket.
func (s *Store) stageCompletionsLocked(ctx context.Context, completions []queuedCompletion, cursors []queuedHostCursor) (*pipeTicket, []metastore.Op, error) {
	s.countsMu.Lock()
	defer s.countsMu.Unlock()
	s.rosterMu.Lock()
	defer s.rosterMu.Unlock()
	// Staged rows go to a plain batch, not the view's: as with the writer's
	// batch they used to go to directly, later reads see the store and the
	// caches below, not earlier staging.
	local := metastore.NewOpBatch(nil)

	// Read everything the staging below touches in two round trips — the
	// rows named up front, then the host buckets those rows were attributed
	// to — rather than one per completion while holding countsMu.
	view, reader := s.lockedView()
	keys := [][]byte{[]byte(countsKey)}
	for _, c := range completions {
		keys = append(keys, repoKey(c.did))
	}
	for _, cursor := range cursors {
		keys = append(keys, pdsHostKey(cursor.host))
	}
	if err := view.prefetch(context.Background(), keys); err != nil {
		return nil, nil, err
	}
	hosts := make([]string, 0, 2*len(completions))
	for _, c := range completions {
		rs, err := readRepoStatusFrom(view, c.did)
		if err != nil {
			return nil, nil, err
		}
		if rs != nil {
			hosts = append(hosts, rs.Host)
		}
		if bucket, ok := hostBucketFromAuthority(c.host); ok {
			hosts = append(hosts, bucket)
		}
	}
	if err := prefetchHostStatuses(context.Background(), view, hosts...); err != nil {
		return nil, nil, err
	}

	counts, err := loadCountsFrom(view)
	if err != nil {
		return nil, nil, err
	}

	type stagedRepoStatus struct {
		status *RepoStatus
		hadRow bool
	}
	repoCache := make(map[atmos.DID]stagedRepoStatus)
	hostCache := make(map[string]*HostStatus)
	for _, c := range completions {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if c.commit == nil {
			return nil, nil, fmt.Errorf("backfill: stage complete %s: nil commit", c.did)
		}

		cached, ok := repoCache[c.did]
		rs := cached.status
		hadRow := cached.hadRow
		if !ok {
			var err error
			rs, err = readRepoStatusFrom(view, c.did)
			if err != nil {
				return nil, nil, err
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
					oldHS, _, err = loadHostStatus(view, oldHost)
					if err != nil {
						return nil, nil, err
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
			return nil, nil, err
		}
		local.Set(repoKey(c.did), enc)
		repoCache[c.did] = stagedRepoStatus{status: rs, hadRow: true}
		if rs.Host != "" {
			hs := hostCache[rs.Host]
			if hs == nil {
				var err error
				hs, _, err = loadHostStatus(view, rs.Host)
				if err != nil {
					return nil, nil, err
				}
				hostCache[rs.Host] = hs
			}
			applyHostStatusTransition(hs, firstInHostBucket(hadRow, oldHost, rs.Host), rs.Active, old, StatusComplete)
			hs.LastAttemptedAt = c.completed
		}
	}
	for _, hs := range hostCache {
		if err := stageHostStatus(local, hs); err != nil {
			return nil, nil, err
		}
	}
	for _, cursor := range cursors {
		host, _, err := loadPDSHostFrom(view, cursor.host)
		if err != nil {
			return nil, nil, err
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
			return nil, nil, err
		}
		local.Set(pdsHostKey(cursor.host), enc)
	}
	countsEnc, err := encodeCounts(counts)
	if err != nil {
		return nil, nil, err
	}
	local.Set([]byte(countsKey), countsEnc)
	t, err := s.pipe.stage(reader, local.Ops())
	if err != nil {
		return nil, nil, err
	}
	return t, local.Ops(), nil
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
	return readRepoStatusFrom(s.db, did)
}

// readRepoStatusFrom is readRepoStatus reading through db, normally a
// metaView.
func readRepoStatusFrom(db metastore.Store, did atmos.DID) (*RepoStatus, error) {
	val, err := db.Get(context.Background(), repoKey(did))
	if errors.Is(err, metastore.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("backfill: read repo/%s: %w", did, err)
	}
	return decodeRepoStatus(val)
}

func (s *Store) updateRepoHostActive(did atmos.DID, pds string, active bool) error {
	ctx := context.Background()
	return s.writeLocked(ctx, false, fmt.Sprintf("write repo/%s and host active", did), [][]byte{repoKey(did)},
		func(view *metaView, batch metastore.Batch) error {
			return stageRepoHostActive(ctx, view, batch, did, pds, active)
		})
}

// stageRepoHostActive stages an Active flip (and, given a pds, a PDS
// re-stamp) for an existing repo row, reading through view.
func stageRepoHostActive(ctx context.Context, view *metaView, batch metastore.Batch, did atmos.DID, pds string, active bool) error {
	rs, err := readRepoStatusFrom(view, did)
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
	if err := prefetchHostStatuses(ctx, view, oldHost, rs.Host); err != nil {
		return err
	}

	enc, err := encodeRepoStatus(rs)
	if err != nil {
		return err
	}

	batch.Set(repoKey(did), enc)
	if oldHost != "" && oldHost != rs.Host {
		hs, _, err := loadHostStatus(view, oldHost)
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
		hs, _, err := loadHostStatus(view, rs.Host)
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

	return s.writeLocked(context.Background(), false, fmt.Sprintf("write identity resolution %s", did), [][]byte{repoKey(did)},
		func(view *metaView, batch metastore.Batch) error {
			rs, err := readRepoStatusFrom(view, did)
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
				counts, err := loadCountsFrom(view)
				if err != nil {
					return err
				}
				applyCountTransition(&counts, false, "", rs.Backfill.Status)
				countsEnc, err = encodeCounts(counts)
				if err != nil {
					return err
				}
			}

			batch.Set(repoKey(did), enc)
			if len(countsEnc) > 0 {
				batch.Set([]byte(countsKey), countsEnc)
			}
			if handleIndexChanged(oldHandle, resolution.Handle) {
				if err := stageHandleIndexDeleteIfMatches(view, batch, oldHandle, did); err != nil {
					return err
				}
			}
			if err := stageHandleIndexSet(batch, resolution.Handle, did); err != nil {
				return err
			}
			if oldHost != normalizedHost {
				if oldHost != "" {
					oldHS, _, err := loadHostStatus(view, oldHost)
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

				newHS, _, err := loadHostStatus(view, normalizedHost)
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
			return nil
		})
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

// discoverMode selects the row a reconcile writes for a DID it discovers.
type discoverMode int

const (
	// discoverBootstrap records a not_started row this run downloads.
	discoverBootstrap discoverMode = iota
	// discoverForRetry records a failed marker that steady-state retry
	// downloads (merge discovery, after bootstrap's downloads are done).
	discoverForRetry
)

func (m discoverMode) newRow(host, bucket string, entry atmossync.ListReposEntry, now time.Time) *RepoStatus {
	rs := &RepoStatus{PDS: host, Host: bucket, Active: entry.Active}
	switch m {
	case discoverForRetry:
		rs.Backfill = RepoBackfillStatus{Status: StatusFailed, LastError: "discovered post-bootstrap; queued for retry"}
	default:
		rs.Backfill = RepoBackfillStatus{Status: StatusNotStarted, StartedAt: now}
	}
	return rs
}

// onDiscover writes a fresh RepoStatus at status=not_started for a DID the
// engine has never seen. atmos guarantees discovery fires at most once per
// DID per Lookup-StateUnknown path.
func (s *Store) onDiscover(ctx context.Context, host string, entry atmossync.ListReposEntry) error {
	return s.reconcileBatch(ctx, host, []atmossync.ListReposEntry{entry}, nil, discoverBootstrap)
}

// OnDiscover is retained for Jetstream's selected-repo and focused store
// paths, which do not have a listHosts mapping. Fleet callers use AtmosStore.
func (s *Store) OnDiscover(ctx context.Context, entry atmossync.ListReposEntry) error {
	return s.onDiscover(ctx, "", entry)
}

// OnDiscoverForRetry records merge-discovery-only markers for unknown DIDs,
// all listed by host, in one write. Steady-state retry owns the eventual
// direct-PDS downloads.
func (s *Store) OnDiscoverForRetry(ctx context.Context, host string, entries []atmossync.ListReposEntry) error {
	return s.reconcileBatch(ctx, host, entries, nil, discoverForRetry)
}

// reconcileBatch records one listRepos page's reconcile outcome in one
// group-committed transaction: a new row per discovered DID, with the counts,
// host aggregate, and roster moves they imply applied once for the page, then
// the Active flips. It returns once the write is durable.
func (s *Store) reconcileBatch(ctx context.Context, host string, discovered, updated []atmossync.ListReposEntry, mode discoverMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(discovered) == 0 && len(updated) == 0 {
		return nil
	}
	bucket, _ := hostBucketFromAuthority(host)
	keys := [][]byte{[]byte(countsKey)}
	if bucket != "" {
		if _, key, err := normalizeHostStatusKey(bucket); err == nil {
			keys = append(keys, key)
		}
	}
	if host != "" {
		keys = append(keys, pdsHostKey(host))
	}
	for _, entry := range updated {
		keys = append(keys, repoKey(entry.DID))
	}
	return s.commitGrouped(&groupWrite{
		keys: keys,
		apply: func(ctx context.Context, view *metaView, batch metastore.Batch) error {
			if err := stageDiscovered(view, batch, host, bucket, discovered, mode); err != nil {
				return err
			}
			if err := prefetchRepoHosts(ctx, view, updated); err != nil {
				return err
			}
			for _, entry := range updated {
				if err := stageRepoHostActive(ctx, view, batch, entry.DID, host, entry.Active); err != nil {
					return err
				}
			}
			return nil
		},
		committed: func() {
			for _, entry := range discovered {
				if mode == discoverBootstrap {
					s.markDiscoveredThisRun(entry.DID)
					s.metrics.incDiscovered()
				}
			}
			for range updated {
				s.metrics.incActiveFlips()
			}
		},
	})
}

// stageDiscovered stages a fresh row for every entry, which atmos guarantees
// has none, and moves the counts row, the host aggregate, and the roster
// row's ActualAccounts once for all of them.
func stageDiscovered(view *metaView, batch metastore.Batch, host, bucket string, entries []atmossync.ListReposEntry, mode discoverMode) error {
	if len(entries) == 0 {
		return nil
	}
	counts, err := loadCountsFrom(view)
	if err != nil {
		return err
	}
	var hs *HostStatus
	if bucket != "" {
		if hs, _, err = loadHostStatus(view, bucket); err != nil {
			return err
		}
	}
	var roster *PDSHost
	if host != "" {
		if roster, _, err = loadPDSHostFrom(view, host); err != nil {
			return err
		}
	}
	now := timeNow()
	for _, entry := range entries {
		rs := mode.newRow(host, bucket, entry, now)
		enc, err := encodeRepoStatus(rs)
		if err != nil {
			return err
		}
		batch.Set(repoKey(entry.DID), enc)
		applyCountTransition(&counts, false, "", rs.Backfill.Status)
		if hs != nil {
			applyHostStatusTransition(hs, firstInHostBucket(false, "", bucket), rs.Active, "", rs.Backfill.Status)
		}
		if roster != nil {
			roster.ActualAccounts++
		}
	}
	countsEnc, err := encodeCounts(counts)
	if err != nil {
		return err
	}
	batch.Set([]byte(countsKey), countsEnc)
	if hs != nil {
		if err := stageHostStatus(batch, hs); err != nil {
			return err
		}
	}
	if roster != nil {
		roster.UpdatedAt = now
		rosterEnc, err := encodePDSHost(roster)
		if err != nil {
			return err
		}
		batch.Set(pdsHostKey(host), rosterEnc)
	}
	return nil
}

// commitGrouped commits w in a group transaction (see groupCommitter).
func (s *Store) commitGrouped(w *groupWrite) error {
	return s.group.commit(w, s.commitGroup)
}

// commitGroup runs one group's transaction: under the aggregate locks every
// member's apply expects, one prefetch for every member's keys and each apply
// in queue order against a shared view; then, after the locks are released,
// one commit (commitPipe).
func (s *Store) commitGroup(group []*groupWrite) error {
	ctx := context.Background()
	var keys [][]byte
	for _, w := range group {
		keys = append(keys, w.keys...)
	}
	ops := 0
	err := s.writeLocked(ctx, true, fmt.Sprintf("group commit of %d writes", len(group)), keys,
		func(view *metaView, batch metastore.Batch) error {
			for _, w := range group {
				if err := w.apply(ctx, view, batch); err != nil {
					return err
				}
			}
			ops = batch.Len()
			return nil
		})
	if err == nil && ops > 0 {
		s.metrics.observeGroupCommit(len(group), ops)
	}
	return err
}

// HostDiscoveryCursor reopens a drained/exhausted host at its last non-empty
// bootstrap cursor so merge discovery scans only tail pages.
func (s *Store) HostDiscoveryCursor(ctx context.Context, hostname string) (string, bool, error) {
	states, err := s.HostDiscoveryCursors(ctx, []string{hostname})
	if err != nil {
		return "", false, err
	}
	return states[0].Cursor, states[0].Drained, nil
}

// HostDiscoveryCursors is HostDiscoveryCursor for many hosts in one read.
func (s *Store) HostDiscoveryCursors(ctx context.Context, hostnames []string) ([]atmosbackfill.HostCursorState, error) {
	return s.hostCursors(ctx, hostnames, func(host *PDSHost) atmosbackfill.HostCursorState {
		return atmosbackfill.HostCursorState{Cursor: host.LastNonEmptyCursor}
	})
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
	return loadPDSHostFrom(s.db, hostname)
}

// loadPDSHostFrom is loadPDSHost reading through db, normally a metaView.
func loadPDSHostFrom(db metastore.Store, hostname string) (*PDSHost, bool, error) {
	val, err := db.Get(context.Background(), pdsHostKey(hostname))
	if errors.Is(err, metastore.ErrNotFound) {
		return &PDSHost{Hostname: hostname}, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("backfill: load pdshost/%s: %w", hostname, err)
	}
	host, err := decodePDSHost(val)
	if err != nil {
		return nil, false, fmt.Errorf("backfill: load pdshost/%s: %w", hostname, err)
	}
	return host, true, nil
}

// OnHost upserts one host's relay metadata.
func (s *Store) OnHost(ctx context.Context, info atmosbackfill.HostInfo) error {
	return s.recordHosts(ctx, []atmosbackfill.HostInfo{info})
}

// HostCursor returns the host's resume cursor and whether a prior run fully
// enumerated it.
func (s *Store) HostCursor(ctx context.Context, hostname string) (string, bool, error) {
	states, err := s.hostCursors(ctx, []string{hostname}, bootstrapHostCursor)
	if err != nil {
		return "", false, err
	}
	return states[0].Cursor, states[0].Drained, nil
}

// bootstrapHostCursor is the bootstrap projection of a roster row: resume
// from the last checkpoint, and skip only a host a prior run fully
// enumerated.
func bootstrapHostCursor(host *PDSHost) atmosbackfill.HostCursorState {
	return atmosbackfill.HostCursorState{Cursor: host.ListReposCursor, Drained: host.Enumerated}
}

// recordHosts upserts each host's relay metadata in one group-committed
// transaction.
func (s *Store) recordHosts(ctx context.Context, hosts []atmosbackfill.HostInfo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(hosts) == 0 {
		return nil
	}
	keys := make([][]byte, len(hosts))
	for i, info := range hosts {
		keys[i] = pdsHostKey(info.Hostname)
	}
	return s.commitGrouped(&groupWrite{
		keys: keys,
		apply: func(_ context.Context, view *metaView, batch metastore.Batch) error {
			now := timeNow()
			for _, info := range hosts {
				host, exists, err := loadPDSHostFrom(view, info.Hostname)
				if err != nil {
					return err
				}
				if !exists {
					host.FirstSeenAt = now
					host.State = string(atmosbackfill.HostStatePending)
				}
				host.RelayStatus = info.RelayStatus
				host.RelayAccounts = info.RelayAccounts
				host.Seq = info.Seq
				host.UpdatedAt = now
				enc, err := encodePDSHost(host)
				if err != nil {
					return err
				}
				batch.Set(pdsHostKey(info.Hostname), enc)
			}
			return nil
		},
	})
}

// hostCursors reads every host's roster row in one round trip and returns
// project's answer for each, in order. An unseen host projects from an empty
// row.
func (s *Store) hostCursors(ctx context.Context, hostnames []string, project func(*PDSHost) atmosbackfill.HostCursorState) ([]atmosbackfill.HostCursorState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.rosterMu.Lock()
	defer s.rosterMu.Unlock()
	view := newMetaView(s.db)
	keys := make([][]byte, len(hostnames))
	for i, hostname := range hostnames {
		keys[i] = pdsHostKey(hostname)
	}
	if err := view.prefetch(context.Background(), keys); err != nil {
		return nil, err
	}
	out := make([]atmosbackfill.HostCursorState, len(hostnames))
	for i, hostname := range hostnames {
		host, _, err := loadPDSHostFrom(view, hostname)
		if err != nil {
			return nil, err
		}
		out[i] = project(host)
	}
	return out, nil
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
	keys := [][]byte{pdsHostKey(hostname), []byte(countsKey)}
	return s.writeLocked(ctx, true, fmt.Sprintf("commit pdshost/%s state", hostname), keys,
		func(view *metaView, batch metastore.Batch) error {
			host, _, err := loadPDSHostFrom(view, hostname)
			if err != nil {
				return err
			}
			oldState := atmosbackfill.HostState(host.State)
			nextState := mutate(host)
			host.State = string(nextState)
			host.UpdatedAt = timeNow()

			counts, err := loadCountsFrom(view)
			if err != nil {
				return err
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
			batch.Set(pdsHostKey(hostname), hostEnc)
			batch.Set([]byte(countsKey), countsEnc)
			return nil
		})
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

	return s.writeLocked(ctx, false, fmt.Sprintf("defer retry repo/%s", did), [][]byte{repoKey(did)},
		func(view *metaView, batch metastore.Batch) error {
			rs, err := readRepoStatusFrom(view, did)
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
			batch.Set(repoKey(did), enc)
			return nil
		})
}

// timeNow is a package var so tests can pin wall-clock values.
// Production callers don't override this.
var timeNow = func() time.Time { return time.Now().UTC() }
