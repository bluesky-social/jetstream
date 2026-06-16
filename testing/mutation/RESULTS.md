# Oracle Mutation Campaign Results

Each campaign appends a dated section; history is never overwritten so the
oracle's detection power is visible over time. See
`docs/superpowers/specs/2026-06-12-oracle-mutation-campaign-design.md` for the
method and `testing/mutation/run.sh` for the driver.

## Retired mutants

The active catalog no longer carries mutants that have been reclassified as
stale/dead under the current implementation. Their historical scorecard rows
remain below so the reasoning is not lost.

| mutant | retired | reason |
|---|---|---|
| m007_compaction_chunk_boundary | 2026-06-15 | Invalid/dead under current compaction chunk construction; the modeled corrupt shape cannot be produced by current chunk snapshots. |
| m010_nextblockoffset_reset | 2026-06-15 | Stale/dead for sealed oracle observations; `Writer.Seal` rebuilds footer block metadata from physical frames. |

## Active catalog check 2026-06-15 — retired mutants removed

- commit under test: `767792e`
- driver: `just mutation-campaign`
- catalog: 17 active mutants; retired mutants `m007` and `m010` are absent
- purpose: verify the active catalog still runs after removing stale/dead
  legacy patches

| mutant | result | note |
|---|---|---|
| m001_delete_mapped_to_update | KILLED@default | default oracle killed the mutant |
| m002_watermark_floor_off_by_one | SURVIVED | known seed-dependent/future-roadmap gap |
| m003_merge_cursor_no_advance | SURVIVED | known restart-depth gap |
| m004_rev_filter_inverted | KILLED@default | missing expected event diagnostic |
| m005_backfill_status_check_inverted | KILLED@stress | rev-regression diagnostic |
| m006_merge_commit_error_swallowed | SURVIVED | known store-fault gap |
| m008_header_offset_byteslice | KILLED@default | default oracle killed the mutant |
| m009_checksum_range_off_by_one | SURVIVED | known closed-loop checksum blind spot |
| m011_wire_frame_length | KILLED@default | active segment walk failed |
| m012_block_event_count_off_by_one | KILLED@default | default oracle killed the mutant |
| m013_collection_rkey_swap | SURVIVED | known dead path in this simulator config; companion `m017` covers hot path |
| m014_rev_dropped | SURVIVED | known dead path in this simulator config; companion `m018` covers hot path |
| m015_collection_count_double | SURVIVED | known read-path index blind spot |
| m016_bloom_size_off_by_one | KILLED@default | default oracle killed the mutant |
| m017_commit_collection_rkey_swap | KILLED@default | event-log mismatch diagnostic |
| m018_commit_rev_dropped | KILLED@default | event-log mismatch diagnostic |
| m019_sync_tombstone_dropped | KILLED@default | event-log equivalence caught missing sync row |

## Targeted follow-up 2026-06-15 — event-log equivalence

- commit under test: branch `testing-revamp` after
  `oracle: assert lifecycle event-log equivalence`
- driver: targeted manual equivalent of `just mutation-campaign m019`
  (`git apply --check`, apply, `go build ./...`, default oracle tier, reverse)
- scope: Workstream 3 event-log equivalence

| mutant | result | disposition |
|---|---|---|
| m019_sync_tombstone_dropped | KILLED@default | New event-log equivalence assertion caught a missing `KindSync` row even though replacement rows can allow final state to converge. Diagnostic: `oracle: missing expected event ... kind=sync`. |

## Targeted follow-up 2026-06-15

- commit under test: branch `testing-revamp` after
  `oracle: reclassify stale mutation expectations`
- driver: targeted `just mutation-campaign m018`, `m010`, and `m007`
  before retiring stale/dead mutants `m010` and `m007` from the active catalog
- scope: Phase 1 of the oracle robustness roadmap

| mutant | result | disposition |
|---|---|---|
| m018_commit_rev_dropped | KILLED@default | Fixed by rejecting empty `Rev` on commit-kind observed events. |
| m010_nextblockoffset_reset | SURVIVED | Reclassified stale/dead for sealed oracle observations. `Writer.Seal` rebuilds footer block metadata by walking physical frames, so the mutated `Writer.nextBlockOffset` only corrupts active `Writer.Blocks()` metadata; existing segment tests already cover that active API. |
| m007_compaction_chunk_boundary | SURVIVED | Reclassified invalid/dead under current compaction chunk construction. A row at `seq == chunkEnd` can only be dropped if the same chunk snapshot contains a tombstone with `seq > chunkEnd`; merge and steady chunk snapshots are both bounded to `<= chunkEnd`, and later chunks still rewrite older rows. |

The original campaign remains below as historical data. These follow-up
results correct two stale mutant interpretations rather than counting them as
oracle assertion work.

## Campaign 2026-06-15

- commit: `75d9251` (branch `mutation-campaign`)
- default seed: 42; survivors swept with 5 random stress seeds where the
  hypothesis was seed-dependent (see "Seed sweeps" below)
- catalog: 18 mutants — orchestrator 6, segment 7, live 5
- driver: `just mutation-campaign`; tiers escalate default → stress → restart
- runtime: full campaign ≈ 14 min (several kills land via the harness
  5-minute after-bootstrap barrier timeout, not a fast assertion)

### Scorecard

| mutant | subsystem | expected | actual | note |
|---|---|---|---|---|
| m001_delete_mapped_to_update | live | default | KILLED@default | delete archived as update → extra record (via bootstrap barrier timeout) |
| m002_watermark_floor_off_by_one | orchestrator | stress | KILLED@stress (4/5 seeds) | flaky detection — assertCompacted, seed-dependent |
| m003_merge_cursor_no_advance | orchestrator | stress | SURVIVED | ESCAPE — restart tier does not exercise this crash seam |
| m004_rev_filter_inverted | orchestrator | default | KILLED@default | `oracle: missing … app.bsky.feed.post/…` |
| m005_backfill_status_check_inverted | orchestrator | default | KILLED@stress | `oracle: rev regression for DID …` (needed scale) |
| m006_merge_commit_error_swallowed | orchestrator | stress | SURVIVED | ESCAPE (predicted) — needs store-fault injection |
| m007_compaction_chunk_boundary | orchestrator | stress | SURVIVED (5 seeds) | ESCAPE — boundary row never re-evaluated |
| m008_header_offset_byteslice | segment | default | KILLED@default | corrupt header offset → segment open fails |
| m009_checksum_range_off_by_one | segment | default | SURVIVED | ESCAPE — symmetric checksum (write==read), closed loop |
| m010_nextblockoffset_reset | segment | default | SURVIVED (5 seeds) | ESCAPE — block read by index, not recorded offset |
| m011_wire_frame_length | segment | stress | KILLED@default | `walk active frames: …` torn-tail on reopen |
| m012_block_event_count_off_by_one | segment | default | KILLED@default | block decode truncated/trailing bytes |
| m013_collection_rkey_swap | live | default | SURVIVED | dead path in this config — see m017 (companion on hot path) |
| m014_rev_dropped | live | default | SURVIVED | dead path in this config — see m018 (companion on hot path) |
| m015_collection_count_double | segment | none (survival) | SURVIVED | confirmed blind spot — footer collection index unread by oracle |
| m016_bloom_size_off_by_one | segment | none (survival) | SURVIVED | confirmed blind spot — bloom filters unread by oracle |
| m017_commit_collection_rkey_swap | live | default | KILLED@default | `oracle: missing … app.bsky.feed.post/…` — hot path IS exercised |
| m018_commit_rev_dropped | live | default | SURVIVED | ESCAPE — rev never compared (see Escapes) |

Summary: **8 killed, 10 survived.** Of the 10 survivors, 2 were predicted
(m015/m016 blind-spot demonstrators), 2 are dead-code-path artifacts that
their companions m017/m018 corrected, and 6 are genuine escapes or
escape-class findings.

Note on catalog growth: m013/m014 originally targeted `convertVerifiedOps`,
the async-resync fallback branch. The simulator emits real `#commit`
envelopes, which route through `convertCommit`, so those two mutated a path
this config never executes. m017/m018 were added mid-campaign to mutate the
same two fields on the hot path. This is itself a finding: the resync/
unknown-envelope conversion path has zero oracle coverage.

### Seed sweeps

Only seed-dependent hypotheses were swept; structural escapes (m009, m013,
m014, m015, m016, m018) survive by construction and gain nothing from more
seeds, and m003/m006 are restart-timing-dependent rather than seed-dependent,
so they were not swept. This is a deliberate scope limit, not full coverage.

- **m002**: KILLED on 4 of 5 random stress seeds → *flaky detection*. The
  oracle CAN see this watermark off-by-one, but only when the boundary seq
  happens to carry a superseded create/update. This is the single most
  actionable operational result: it argues the nightly `oracle-sweep` seed
  count is load-bearing, not decorative — a single-seed CI run would have a
  ~20% chance of missing this real bug.
- **m007**: SURVIVED all 5 seeds → originally treated as a true escape; later
  reclassified as invalid/dead under current compaction chunk construction and
  retired from the active catalog.
- **m010**: SURVIVED all 5 seeds → originally treated as a true escape; later
  reclassified as stale/dead for sealed oracle observations and retired from
  the active catalog.

### Escapes — analysis and disposition

**m018 / m014 — dropped `rev` is invisible (oracle gap; fix recommended).**
`invariants.go:21` skips events with empty rev, and `compare.go:35` compares
rev only when *both* sides populate it — but ground truth never populates rev
(`model.go` RecordValue.Rev doc). So dropping rev entirely blinds both
checks. m017 proves the hot path is otherwise exercised (the collection/rkey
swap on the same struct was caught instantly), so this is a true gap, not a
dead path. Disposition: **fix the oracle** — have CheckInvariants reject an
empty rev on a commit-kind event (a create/update/delete must carry a rev),
which costs nothing and closes the hole. This gap was closed in Milestone A.

**m009 — symmetric checksum (oracle structurally cannot catch; accepted).**
`xxh3HeaderFooter` is used both to write the seal checksum (seal.go:123) and
to verify it on read (reader.go:193). A mutation to its byte range changes
both sides identically, so they always agree. This is a miniature of the
"atmos closed loop" blind spot described in `docs/oracle/DESIGN.md`: the oracle cannot
detect a bug that lives in a function shared by the writer and reader.
Disposition: **accepted blind spot** — only an independent checksum oracle
(or a committed golden segment with a known-good checksum) would catch it.
Cross-referenced in the oracle design document.

**m010 — block read by index, not by recorded offset (historical, retired).**
`DecodeBlock` (reader.go:301) seeks via the block-index entry's offset, and
the oracle decodes blocks 0..N by index. The `nextBlockOffset` bookkeeping the
mutant corrupts feeds a path the oracle's read does not depend on in this
config. A real consumer using offset-based seeks could diverge. Disposition:
**fix the oracle** candidate — add a read mode that follows recorded offsets,
or assert offset monotonicity in ObserveSegments. Later Milestone A review
found this mutant stale/dead for sealed oracle observations because
`Writer.Seal` rebuilds footer metadata by walking physical frames; the active
patch was retired on 2026-06-15.

**m007 — compaction boundary row never re-evaluated (historical, retired).**
The `>` → `>=` weakening keeps the row at exactly chunkEnd, and because the
watermark advances to chunkEnd it is never revisited. assertCompacted did not
catch it across 6 seeds. Disposition: **fix the oracle** — CheckCompacted
should assert that the boundary seq itself is evaluated, not just rows
strictly below it. Later Milestone A review found this mutant invalid/dead
under current compaction chunk construction because the modeled corrupt shape
cannot be produced by current chunk snapshots; the active patch was retired on
2026-06-15.

**m003 — merge-cursor off-by-one not exercised by restart tier (real gap).**
The restart oracle only covers 4 enumerated crashpoints; a merge-cursor
double-process needs a crash precisely between source completion and the next
run, which the current harness does not stage for this seam. Disposition:
**fix the oracle** — extend the restart harness to crash at the
source-complete seam, OR (better) the random-time kill loop from
the crash/restart tier in `docs/oracle/DESIGN.md` would cover this class
without enumeration.

**m006 — swallowed commit error needs store-fault injection (predicted).**
Predicted to survive: under normal runs `commitSourceComplete` never fails,
so the inverted check is dormant. Confirms the store-fault tier requirement in
`docs/oracle/DESIGN.md`: the oracle has no way to make a store write fail.
Disposition: **accepted, pending** the store-fault oracle tier.

**m015 / m016 — footer/bloom read-path indexes (confirmed blind spots).**
Predicted survival, confirmed. The oracle decodes every block sequentially
and never consults the footer collection-count index or the per-block bloom
filters, so corruption there is invisible. These mutants exist to *document*
the gap with evidence. Disposition: **accepted blind spots** — would be
closed by the product replay and XRPC egress tiers in
`docs/oracle/DESIGN.md`, which exercise the read indexes a client uses.

### Prediction misses (corrections to our model of the oracle)

- **m005** predicted default, killed at **stress**: the inverted backfill
  status check only produces a detectable rev regression at scale; default
  mode's smaller overlap window didn't surface it. The oracle's default tier
  is weaker on merge-dedup bugs than assumed.
- **m011** predicted stress, killed at **default**: torn-tail truncation on
  reopen is caught immediately; no scale needed.
- **m002** predicted a clean stress kill; actual is **probabilistic** (4/5).
  Detection of boundary bugs is seed-sensitive — the most important
  correction, see Seed sweeps.
- **m013/m014** predicted default kills; actually mutated a **dead path** in
  this simulator config. Correction: not every plausible-looking production
  line is on the path simulator traffic takes; companions m017/m018 were
  required to test the hot path.

### Bottom line

The campaign measured the oracle rather than re-confirming it. It caught
every hot-path data-shape bug we threw at it (m001, m004, m008, m011, m012,
m017) and several at scale (m005), but it surfaced **two active oracle gaps
worth fixing** (m018 rev-blindness and m003 merge-cursor restart seam), **three structural blind spots**
now named in `docs/oracle/DESIGN.md` (m006 store-fault, m009 closed-loop
checksum, m015/m016 read-path indexes), and one operational signal — **m002's flaky
detection justifies the multi-seed nightly sweep**. The over-fitting worry was
warranted in specific, now-documented places; it was not warranted as a
blanket claim about the oracle.

## Campaign 2026-06-15 (getTombstones overlay)

- commit: `bb135af` (branch `feat/gettombstones-overlay`)
- default seed: the driver's default tier seed; the one survivor was swept
  with 3 random seeds (see below)
- catalog: 4 new mutants — overlay encoder 3 (m019–m021), tombstone 1 (m022)
- target test: `TestOracle_DefaultLifecycle`, which now runs
  `assertOverlayReconstruction` (segments(<=W) + overlay((W,M]) +
  live((M,inf)) reconstruction must equal ground truth) alongside the
  existing compacted/oracle checks
- driver: `just mutation-campaign mNNN`; all four are `tiers: default`

### Scorecard

| mutant | subsystem | expected | actual | note |
|---|---|---|---|---|
| m019_overlay_drop_record_tombstones | overlay | default | KILLED@default | record-tombstone group count forced to 0 → deleted record in (W,M] emitted; `emitted a record that ground truth deleted` |
| m020_overlay_drop_did_tombstones | overlay | default | SURVIVED(3 seeds) | dead path in this scenario — overlay carries **zero** DID tombstones (W=1672 M=1686 overlay.DIDs=0); sync/account-delete land at/below W and are compacted away |
| m021_overlay_record_seq_base_zero | overlay | default | KILLED@default | record seq delta encoded against base 0 not W; decoder re-adds W, inflating tombstone seqs above live records → `failed to emit a live record` |
| m022_shoulddrop_did_seq_inverted | tombstone | default | KILLED@default | `>`→`<` in ShouldDrop DID branch; caught by the compacted oracle and/or reconstruction |

Summary: **3 killed, 1 survived.** The three kills confirm the new
reconstruction assertion has real detection power on the overlay's
record-tombstone section (m019 drop, m021 seq-base) and on the shared
ShouldDrop suppression logic (m022). The single survival (m020) is a
documented dead-path artifact, not a weakness in the test.

### Escapes — analysis and disposition

**m020 — overlay DID-tombstone section is empty in this scenario (dead path;
not seed-sensitive).** The reconstruction's combined suppression set is
`overlay((W,M]) ∪ live((M,inf))`. For dropping the overlay's DID-tombstone
section to matter, a sync or account-delete tombstone must fall *inside* the
overlay window (W,M]. It does not: instrumenting `assertOverlayReconstruction`
showed `W=1672 M=1686 overlay.DIDs=0 overlay.Records=6` — the overlay window
is a thin 14-seq band carrying only six record tombstones and **no** DID
tombstones. The lifecycle scenario generates the sync divergence (and the
bootstrap account delete) earlier; those DID tombstones have seq ≤ W by the
time the steady compaction watermark advances, so compaction evicts them
before the served overlay is built, and any later kill would be in the live
tail (> M), recovered by the `live((M,inf))` fold regardless of the overlay.
The mutant therefore edits a section the served blob never populates in this
config. Swept across 3 random seeds: SURVIVED on all 3 — structural, not a
flaky escape. Disposition: **accepted dead-path artifact for the default
lifecycle.** Closing it requires a scenario that lands a DID tombstone strictly
inside (W,M] — i.e. an account-delete/sync whose seq is above the steady
compaction watermark but at or below the captured overlay max-seq. That is a
narrow timing window the current harness does not deliberately stage. Filed as
follow-up #21: extend the overlay scenario (or add a dedicated overlay-window
fixture) to exercise an in-window DID tombstone so m020 becomes killable.
