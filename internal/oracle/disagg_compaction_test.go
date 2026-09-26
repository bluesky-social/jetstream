package oracle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/catalog/follower"
	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
	"github.com/bluesky-social/jetstream/segment"
)

// The layer 3 oracle's compaction and GC checks (plan S4.4). Every pod runs
// delete compaction and GC with short fake-clock intervals, so main's sealed
// segments are rewritten under the readers and replaced objects are
// collected while the waves run.
//
// Compaction removes rows, so the catalog alone no longer holds the whole
// model. The harness keeps the archive union instead: every main row it has
// ever read, by seq. The leader's OnBeforeCompactionPass hook reads the
// catalog before each pass rewrites anything, so no row can be dropped
// before the union holds it, and a seq read twice must hold the same row
// both times. The union is dense and is what the model checks run on.
//
// Against the union, a stream or an archive is checked with the drop rule
// (disaggDroppable): a create, update, or resync create goes once a later
// record or DID tombstone in (floor, W] supersedes it, where floor is the
// watermark merge starts from. A reader that served a row before a pass
// dropped it keeps it, so a stream may miss exactly the droppable rows;
// once the watermark reaches main's last seq, a fresh download must miss
// all of them and nothing else (the assertCompacted analog), and every
// stream must fold to what the union folds to.
//
// A held reader follows each sealed segment's generations and reads a
// replaced one again just before GC_DELAY runs out since it stopped being
// current: an old generation keeps serving until then (design §13). At the
// end, once GC has had time, every object row is available and referenced
// and the object store holds exactly their keys.

const (
	disaggCompactionInterval = 2 * time.Second
	// disaggTombstoneCap splits a pass into several chunks, so the
	// between-chunk crashpoint has somewhere to fire.
	disaggTombstoneCap = 4
	disaggMaxViewAge   = 4 * time.Second
	disaggMaxResponse  = 10 * time.Second
	disaggGCMargin     = time.Second
	// disaggGCDelay is the smallest delay the pods accept: it must exceed
	// the view age, the response cutoff, and the margin together.
	disaggGCDelay     = disaggMaxViewAge + disaggMaxResponse + disaggGCMargin + time.Second
	disaggGCOrphanAge = 8 * time.Second
	disaggGCInterval  = time.Second
	// disaggLeakTimeout bounds, on the fake clock, how long GC may take to
	// collect every unreferenced object once the catalog stops changing.
	disaggLeakTimeout = 2 * time.Minute
	disaggHeldPoll    = 50 * time.Millisecond
)

// Compaction and GC faults: the leader killed after a rewrite's upload and
// before its publish, after a publish and before the watermark moves,
// between chunks, and after each GC step.
const (
	dfCrashCompactUpload  = disaggFault("crash:" + crashpoint.AfterCompactionUploadBeforePublish)
	dfCrashCompactRewrite = disaggFault("crash:" + crashpoint.AfterCompactionRewriteBeforeWatermark)
	dfCrashCompactChunk   = disaggFault("crash:" + crashpoint.AfterCompactionChunkWatermark)
	dfCrashGCMark         = disaggFault("crash:" + crashpoint.AfterGCMarkBeforeClaim)
	dfCrashGCClaim        = disaggFault("crash:" + crashpoint.AfterGCClaimBeforeDelete)
	dfCrashGCDelete       = disaggFault("crash:" + crashpoint.AfterGCDeleteBeforeForget)
)

var disaggCompactionFaults = []disaggFault{
	dfCrashCompactUpload, dfCrashCompactRewrite, dfCrashCompactChunk,
	dfCrashGCMark, dfCrashGCClaim, dfCrashGCDelete,
}

// compactionOptions turns on compaction and GC for a pod.
func (h *disaggHarness) compactionOptions(opts *jetstreamd.Options) {
	opts.CompactionInterval = disaggCompactionInterval
	opts.CompactionTombstoneCap = disaggTombstoneCap
	opts.CompactionRewriteWorkers = 2
	// A small budget splits rewrites into several reservations.
	opts.Storage.CompactionMemoryBytes = 16 << 10
	opts.Storage.MaxViewAge = disaggMaxViewAge
	opts.Storage.MaxArchiveResponseDuration = disaggMaxResponse
	opts.Storage.GC = jetstreamd.GCConfig{Interval: disaggGCInterval, Delay: disaggGCDelay, OrphanAge: disaggGCOrphanAge}
	opts.GCDelayMargin = disaggGCMargin
	opts.OnBeforeCompactionPass = h.notePass
}

// notePass runs on the leader's goroutine before a compaction pass rewrites
// anything: it records the pass's target and takes every row the pass may
// drop into the archive union. It reports with Errorf, since it is not on
// the test goroutine.
func (h *disaggHarness) notePass(target uint64) {
	h.archiveMu.Lock()
	h.passes++
	first := h.passes == 1
	h.passTarget = max(h.passTarget, target)
	floor := h.floor
	h.archiveMu.Unlock()
	if first {
		w, err := h.watermark()
		switch {
		case err != nil:
			h.t.Errorf("before the first compaction pass: read the watermark: %v", err)
		case w != floor:
			h.t.Errorf("the first compaction pass starts from watermark %d; merge started with main at seq %d", w, floor)
		}
	}
	if _, err := h.captureArchive(); err != nil {
		h.t.Errorf("before the compaction pass to %d: %v", target, err)
	}
}

// passBound is the highest target a compaction pass has announced: no row
// the drop rule keeps below it can have been dropped.
func (h *disaggHarness) passBound() uint64 {
	h.archiveMu.Lock()
	defer h.archiveMu.Unlock()
	return h.passTarget
}

func (h *disaggHarness) compactionFloor() uint64 {
	h.archiveMu.Lock()
	defer h.archiveMu.Unlock()
	return h.floor
}

// watermark reads the compaction watermark W.
func (h *disaggHarness) watermark() (uint64, error) {
	w, _, err := metastore.GetVersionedUint64LE(h.ctx, h.db.MetaStore(nil), "compaction/seq", 0x01)
	return w, err
}

func (h *disaggHarness) mustWatermark() uint64 {
	w, err := h.watermark()
	require.NoError(h.t, err, "read the compaction watermark")
	return w
}

// captureArchive reads the catalog and adds main's rows to the archive
// union. It returns what it read.
func (h *disaggHarness) captureArchive() (map[catalog.Namespace][]segment.Event, error) {
	got, err := h.readCatalogErr()
	if err != nil {
		return nil, err
	}
	h.archiveMu.Lock()
	defer h.archiveMu.Unlock()
	if h.archive == nil {
		h.archive = map[uint64]segment.Event{}
	}
	for _, ev := range got[catalog.Main] {
		old, ok := h.archive[ev.Seq]
		if !ok {
			h.archive[ev.Seq] = ev
			continue
		}
		if disaggRowDigest(old) != disaggRowDigest(ev) {
			return nil, fmt.Errorf("main's seq %d changed: it held %s and now holds %s",
				ev.Seq, disaggRowString(old), disaggRowString(ev))
		}
	}
	return got, nil
}

// archiveNow captures the catalog and returns the archive union in seq
// order, and what the capture read. The union must be dense from 1.
// Captures are cached while the catalog is unchanged.
func (h *disaggHarness) archiveNow() ([]segment.Event, map[catalog.Namespace][]segment.Event) {
	key := h.archiveKey()
	if key == h.archiveCacheKey && h.archiveCache != nil {
		return h.archiveCache, h.archiveCacheRead
	}
	got, err := h.captureArchive()
	if err != nil {
		h.failf("capture the archive: %v", err)
	}
	h.archiveMu.Lock()
	evs := make([]segment.Event, 0, len(h.archive))
	for _, seq := range slices.Sorted(maps.Keys(h.archive)) {
		evs = append(evs, h.archive[seq])
	}
	h.archiveMu.Unlock()
	h.requireDense(evs, "the archive union")
	h.archiveCacheKey, h.archiveCache, h.archiveCacheRead = key, evs, got
	return evs, got
}

// archiveKey changes whenever main's content can have: a new seq, or a
// segment's generation replaced.
func (h *disaggHarness) archiveKey() string {
	snap, err := h.db.Snapshot()
	require.NoError(h.t, err)
	var b strings.Builder
	b.WriteString(h.catalogMark())
	for _, s := range snap.Segments {
		fmt.Fprintf(&b, " %s/%d:%d:%d:%d", s.Namespace, s.Index, s.State, s.GenerationID, s.Revision)
	}
	return b.String()
}

// droppable applies the drop rule to evs with the watermark at bound.
func (h *disaggHarness) droppable(evs []segment.Event, bound uint64) map[uint64]bool {
	drop, err := disaggDroppable(evs, h.compactionFloor(), bound)
	if err != nil {
		h.failf("the drop rule: %v", err)
	}
	return drop
}

// disaggDroppable returns the seqs of the rows compaction drops once its
// watermark reaches bound, having started from floor: each create, update,
// or resync create that a record tombstone (a delete or an update of the
// same record) or a DID tombstone (a sync, or an account deletion) with a
// seq in (floor, bound] follows. It is segment.Tombstones' rule, written
// again from the design, and monotone in bound.
func disaggDroppable(evs []segment.Event, floor, bound uint64) (map[uint64]bool, error) {
	records := map[RecordKey]uint64{}
	dids := map[string]uint64{}
	for _, ev := range evs {
		if ev.Seq <= floor || ev.Seq > bound {
			continue
		}
		switch ev.Kind {
		case segment.KindDelete, segment.KindUpdate:
			k := RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}
			records[k] = max(records[k], ev.Seq)
		case segment.KindSync:
			dids[ev.DID] = max(dids[ev.DID], ev.Seq)
		case segment.KindAccount:
			deleted, err := oracleAccountDeleted(ev.Payload)
			if err != nil {
				return nil, fmt.Errorf("account payload at seq %d: %w", ev.Seq, err)
			}
			if deleted {
				dids[ev.DID] = max(dids[ev.DID], ev.Seq)
			}
		}
	}
	out := map[uint64]bool{}
	for _, ev := range evs {
		if !ev.Kind.IsMaterialization() {
			continue
		}
		if dids[ev.DID] > ev.Seq || records[RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}] > ev.Seq {
			out[ev.Seq] = true
		}
	}
	return out, nil
}

// settleCompaction waits, with no traffic, until the watermark reaches
// main's last seq: every pass after the last row has run.
func (h *disaggHarness) settleCompaction() {
	deadline := time.Now().Add(disaggConvergeTimeout)
	for {
		h.reap()
		h.checkPods()
		last := h.mainNext() - 1
		w := h.mustWatermark()
		if w > last {
			h.failf("the compaction watermark %d is past main's last seq %d", w, last)
		}
		if w == last {
			return
		}
		if time.Now().After(deadline) {
			h.failf("compaction did not reach main's last seq %d after %s: W=%d", last, disaggConvergeTimeout, w)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (h *disaggHarness) passCount() int {
	h.archiveMu.Lock()
	defer h.archiveMu.Unlock()
	return h.passes
}

// disaggV1Last is the last seq of evs a v1 subscriber must reach: the last
// row v1 carries that compaction may not have dropped.
func disaggV1Last(evs []segment.Event, drop map[uint64]bool) uint64 {
	for i := len(evs) - 1; i >= 0; i-- {
		ev := evs[i]
		if ev.Kind != segment.KindSync && !ev.Kind.IsResyncReplacement() && !drop[ev.Seq] {
			return ev.Seq
		}
	}
	return 0
}

func disaggKeys(evs []segment.Event) []disaggKey {
	out := make([]disaggKey, len(evs))
	for i, ev := range evs {
		out[i] = disaggKeyOf(observedFromSegment(ev))
	}
	return out
}

// requireServed checks a v2 stream against the archive union evs: seqs
// increase, each event is the union's row at its seq, every seq skipped is
// one compaction may drop, and the stream reaches the last row. With exact
// set the stream must also skip every droppable row: it was read after the
// watermark reached main's last seq.
func (h *disaggHarness) requireServed(evs []segment.Event, drop map[uint64]bool, got []ObservedEvent, exact bool, what string) {
	var last uint64
	for _, ev := range got {
		if ev.Seq <= last || ev.Seq > uint64(len(evs)) {
			h.failf("%s: seq %d after seq %d, with %d rows archived", what, ev.Seq, last, len(evs))
		}
		for s := last + 1; s < ev.Seq; s++ {
			if !drop[s] {
				h.failf("%s: skipped seq %d, which compaction does not drop: %s", what, s, disaggRowString(evs[s-1]))
			}
		}
		want := evs[ev.Seq-1]
		if disaggKeyOf(ev) != disaggKeyOf(observedFromSegment(want)) || ev.WitnessedAt != want.WitnessedAt ||
			(ev.Kind.IsMaterialization() && !bytes.Equal(ev.Payload, want.Payload)) {
			h.failf("%s: seq %d is %s at %d; the archive holds %s", what, ev.Seq,
				disaggKeyAt([]disaggKey{disaggKeyOf(ev)}, 0), ev.WitnessedAt, disaggRowString(want))
		}
		if exact && drop[ev.Seq] {
			h.failf("%s: served seq %d, which compaction must have dropped: %s", what, ev.Seq, disaggRowString(want))
		}
		last = ev.Seq
	}
	if last != uint64(len(evs)) {
		h.failf("%s: ends at seq %d; main's last seq is %d", what, last, len(evs))
	}
}

// requireV1 checks a v1 stream against proj, the v1 projection of the
// archive union: it is proj in order less rows compaction may drop.
func (h *disaggHarness) requireV1(proj []disaggKey, drop map[uint64]bool, got []disaggKey, what string) {
	j := 0
	for _, k := range got {
		for j < len(proj) && proj[j].Seq != k.Seq {
			if !drop[proj[j].Seq] {
				h.failf("%s: skipped %s, which compaction does not drop", what, disaggKeyAt(proj, j))
			}
			j++
		}
		if j == len(proj) {
			h.failf("%s: %s is out of order or not in the archive", what, disaggKeyAt([]disaggKey{k}, 0))
		}
		if proj[j] != k {
			h.failf("%s: got %s; the archive holds %s", what, disaggKeyAt([]disaggKey{k}, 0), disaggKeyAt(proj, j))
		}
		j++
	}
	for ; j < len(proj); j++ {
		if !drop[proj[j].Seq] {
			h.failf("%s: missing %s, which compaction does not drop", what, disaggKeyAt(proj, j))
		}
	}
}

// checkLeaks waits for GC to collect every object no generation, active
// block, or hot batch references, then requires the object store to hold
// exactly the objects rows' keys.
func (h *disaggHarness) checkLeaks() {
	deadline := time.Now().Add(disaggLeakTimeout)
	for {
		why := h.leakPending()
		if why == "" {
			return
		}
		h.reap()
		h.checkPods()
		if time.Now().After(deadline) {
			h.failf("objects not collected after %s: %s", disaggLeakTimeout, why)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// leakPending says why the objects table and the object store are not yet
// exactly the referenced objects, or "". It lists the store first: GC
// deletes a key before it forgets the row.
func (h *disaggHarness) leakPending() string {
	var keys []string
	for _, k := range h.blob.Keys() {
		if strings.HasPrefix(k, h.objects) {
			keys = append(keys, k)
		}
	}
	rows := h.db.AllObjects()
	snap, err := h.db.Snapshot()
	require.NoError(h.t, err)
	archiveID := h.db.Archive().ArchiveID
	byKey := map[string]uint64{}
	for _, id := range slices.Sorted(maps.Keys(rows)) {
		row := rows[id]
		if row.State != catalog.ObjectAvailable {
			return fmt.Sprintf("object %d is %s", id, row.State)
		}
		byKey[objstore.Key(archiveID, row.Key)] = id
	}
	for _, k := range keys {
		if _, ok := byKey[k]; !ok {
			return "key " + k + " has no objects row"
		}
		delete(byKey, k)
	}
	for _, k := range slices.Sorted(maps.Keys(byKey)) {
		return fmt.Sprintf("object %d's key %s is missing from the store", byKey[k], k)
	}
	ref := map[uint64]bool{}
	for _, id := range snap.ReferencedObjects() {
		ref[id] = true
	}
	for _, id := range slices.Sorted(maps.Keys(rows)) {
		if !ref[id] {
			return fmt.Sprintf("object %d is unreferenced", id)
		}
	}
	return ""
}

// disaggHeld reads replaced generations as a reader holding an old view
// would. Its goroutine owns gens and retired; the rest is shared.
type disaggHeld struct {
	done chan struct{}

	// gens is each sealed main segment's generation as last seen current.
	gens map[uint64]*disaggHeldGen
	// retired are replaced generations whose late read is due.
	retired []*disaggHeldGen

	mu      sync.Mutex
	errs    []string
	pending int
	late    int
	// missed counts late reads that failed after GC could have claimed.
	missed int
	// rows is every row a generation held, by seq, as digests.
	rows map[uint64][32]byte
	read int
}

type disaggHeldGen struct {
	idx, gen    uint64
	parts       catalog.GenerationParts
	digest      [32]byte
	lastCurrent time.Time
	lateAt      time.Time
}

func (hr *disaggHeld) fail(format string, args ...any) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.errs = append(hr.errs, fmt.Sprintf(format, args...))
}

// startHeldReader starts the held reader. It reads through its own
// follower's object reader, which serves an object the mirror no longer
// holds from its catalog row until GC claims it, as every pod's does.
func (h *disaggHarness) startHeldReader() {
	f, err := follower.New(follower.Config{
		DB:              h.db.Observer("oracle-held"),
		Blob:            h.blob.Unfaulted(),
		ArchiveID:       h.db.Archive().ArchiveID,
		Logger:          slog.New(slog.DiscardHandler),
		Metrics:         follower.NewMetrics(prometheus.NewRegistry()),
		ReadConcurrency: 4,
	})
	require.NoError(h.t, err)
	hr := &disaggHeld{done: make(chan struct{}), gens: map[uint64]*disaggHeldGen{}, rows: map[uint64][32]byte{}}
	h.held = hr
	ctx, cancel := context.WithCancel(h.ctx)
	go func() {
		defer close(hr.done)
		for ctx.Err() == nil {
			h.heldPoll(ctx, hr, f)
			disaggSleep(ctx, disaggHeldPoll)
		}
	}()
	h.t.Cleanup(func() { cancel(); <-hr.done })
}

func (h *disaggHarness) heldPoll(ctx context.Context, hr *disaggHeld, f *follower.Follower) {
	now := time.Now()
	snap, err := h.db.Snapshot()
	if err != nil {
		hr.fail("snapshot: %v", err)
		return
	}
	cur := map[uint64]uint64{}
	for _, s := range snap.Segments {
		if s.Namespace == catalog.Main && s.State == catalog.Sealed {
			cur[s.Index] = s.GenerationID
		}
	}
	// A generation no longer current was unreferenced after its last poll
	// as current, so GC may not claim its objects until GC_DELAY after
	// that: read it at once, and again just before then.
	for _, idx := range slices.Sorted(maps.Keys(hr.gens)) {
		g := hr.gens[idx]
		if cur[idx] == g.gen {
			g.lastCurrent = now
			continue
		}
		delete(hr.gens, idx)
		g.lateAt = g.lastCurrent.Add(disaggGCDelay - time.Second)
		h.heldRead(ctx, hr, f, g, "when replaced")
		hr.retired = append(hr.retired, g)
		hr.mu.Lock()
		hr.pending++
		hr.mu.Unlock()
	}
	hr.retired = slices.DeleteFunc(hr.retired, func(g *disaggHeldGen) bool {
		if now.Before(g.lateAt) || ctx.Err() != nil {
			return false
		}
		ok := h.heldRead(ctx, hr, f, g, "late")
		hr.mu.Lock()
		hr.pending--
		if ok {
			hr.late++
		}
		hr.mu.Unlock()
		return true
	})
	refreshed := false
	for _, idx := range slices.Sorted(maps.Keys(cur)) {
		if _, ok := hr.gens[idx]; ok || ctx.Err() != nil {
			continue
		}
		if !refreshed {
			if err := f.Refresh(ctx); err != nil {
				if ctx.Err() == nil {
					hr.fail("refresh: %v", err)
				}
				return
			}
			refreshed = true
		}
		p, ok, err := f.GenerationParts(ctx, idx)
		if err != nil || !ok || p.Generation != cur[idx] {
			// Replaced again since the snapshot; the next poll sees the new
			// one.
			continue
		}
		g := &disaggHeldGen{idx: idx, gen: p.Generation, parts: p, lastCurrent: now}
		rows, digest, err := heldDecode(ctx, f, g)
		if err != nil {
			if ctx.Err() == nil {
				hr.fail("segment %d generation %d, read while current: %v", idx, g.gen, err)
			}
			continue
		}
		g.digest = digest
		hr.mu.Lock()
		hr.read++
		for seq, d := range rows {
			if old, ok := hr.rows[seq]; ok && old != d {
				hr.errs = append(hr.errs, fmt.Sprintf("seq %d differs between generations (segment %d generation %d)", seq, idx, g.gen))
			}
			hr.rows[seq] = d
		}
		hr.mu.Unlock()
		hr.gens[idx] = g
	}
}

// heldRead reads a replaced generation from the parts taken while it was
// current, and requires the same rows. It reports whether the read
// succeeded. A failure after GC_DELAY has run out is GC's right.
func (h *disaggHarness) heldRead(ctx context.Context, hr *disaggHeld, f *follower.Follower, g *disaggHeldGen, what string) bool {
	_, digest, err := heldDecode(ctx, f, g)
	switch {
	case ctx.Err() != nil:
		return false
	case err != nil && !time.Now().Before(g.lastCurrent.Add(disaggGCDelay)):
		hr.mu.Lock()
		hr.missed++
		hr.mu.Unlock()
		return false
	case err != nil:
		hr.fail("segment %d generation %d, read %s, %s after it was last current: %v",
			g.idx, g.gen, what, time.Since(g.lastCurrent), err)
		return false
	case digest != g.digest:
		hr.fail("segment %d generation %d, read %s: its rows changed", g.idx, g.gen, what)
		return false
	}
	return true
}

func heldDecode(ctx context.Context, f *follower.Follower, g *disaggHeldGen) (map[uint64][32]byte, [32]byte, error) {
	file, err := xrpcapi.ObjectOpener{Gens: disaggFixedParts{g.parts}, Objects: f.Objects()}.OpenSegment(ctx, g.idx)
	if err != nil {
		return nil, [32]byte{}, err
	}
	r, err := segment.OpenReaderAt(file, file.Size(), segment.ReaderOptions{})
	if err != nil {
		return nil, [32]byte{}, err
	}
	defer func() { _ = r.Close() }()
	rows := map[uint64][32]byte{}
	d := sha256.New()
	for i := range r.Blocks() {
		evs, err := r.DecodeBlock(i)
		if err != nil {
			return nil, [32]byte{}, fmt.Errorf("block %d: %w", i, err)
		}
		for _, ev := range evs {
			rd := disaggRowDigest(ev)
			rows[ev.Seq] = rd
			_, _ = d.Write(rd[:])
		}
	}
	return rows, [32]byte(d.Sum(nil)), nil
}

// disaggFixedParts resolves every segment to one generation's parts: a
// reader holding the view it took them from.
type disaggFixedParts struct{ p catalog.GenerationParts }

func (f disaggFixedParts) GenerationParts(context.Context, uint64) (catalog.GenerationParts, bool, error) {
	return f.p, true, nil
}

// checkHeld waits for every late read to be due and done, then requires
// that none failed, that at least one ran, and that every row any
// generation held is the archive union's row at its seq.
func (h *disaggHarness) checkHeld(evs []segment.Event) {
	hr := h.held
	deadline := time.Now().Add(disaggGCDelay + disaggConvergeTimeout)
	for {
		hr.mu.Lock()
		errs, pending := slices.Clone(hr.errs), hr.pending
		hr.mu.Unlock()
		if len(errs) > 0 {
			h.failf("held reader:\n  %s", strings.Join(errs, "\n  "))
		}
		if pending == 0 {
			break
		}
		h.reap()
		h.checkPods()
		if time.Now().After(deadline) {
			h.failf("held reader: %d late reads still pending after %s", pending, disaggGCDelay+disaggConvergeTimeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
	hr.mu.Lock()
	defer hr.mu.Unlock()
	if hr.late == 0 {
		h.failf("held reader: no replaced generation was read late (%d generations read, %d late reads missed)", hr.read, hr.missed)
	}
	for _, seq := range slices.Sorted(maps.Keys(hr.rows)) {
		if seq == 0 || seq > uint64(len(evs)) || disaggRowDigest(evs[seq-1]) != hr.rows[seq] {
			h.failf("held reader: a generation held seq %d, which is not the archive union's row", seq)
		}
	}
}

func (h *disaggHarness) heldLate() int {
	h.held.mu.Lock()
	defer h.held.mu.Unlock()
	return h.held.late
}

// disaggRowDigest is a digest of every field a segment row stores.
func disaggRowDigest(ev segment.Event) [32]byte {
	d := sha256.New()
	var b [8]byte
	u := func(v uint64) {
		binary.BigEndian.PutUint64(b[:], v)
		_, _ = d.Write(b[:])
	}
	s := func(v []byte) {
		u(uint64(len(v)))
		_, _ = d.Write(v)
	}
	u(ev.Seq)
	u(uint64(ev.WitnessedAt))
	u(uint64(ev.IndexedAt))
	u(uint64(ev.Kind))
	s([]byte(ev.DID))
	s([]byte(ev.Collection))
	s([]byte(ev.Rkey))
	s([]byte(ev.Rev))
	s(ev.Payload)
	return [32]byte(d.Sum(nil))
}

func disaggRowString(ev segment.Event) string {
	return fmt.Sprintf("seq=%d kind=%d %s %s/%s rev=%s witnessed=%d payload=%d bytes",
		ev.Seq, ev.Kind, ev.DID, ev.Collection, ev.Rkey, ev.Rev, ev.WitnessedAt, len(ev.Payload))
}
