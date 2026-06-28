# High Availability / Read-Scaling Clustering — Living Brainstorm & Worklog

> ⚠️ **NOT IMPLEMENTED — DESIGN BRAINSTORMING ONLY.** As of 2026-06-28 and commit
> `c56082e`, none of this exists in the codebase. There is no leader election, no
> read cluster, no follower mode, no shared-storage backend, no fencing — jetstream
> runs single-node / active-passive exactly as `docs/README.md` §6 describes. This
> document captures exploratory design thinking and benchmark data only; it is not
> a commitment to build any of it, and nothing here has been promoted into the
> source-of-truth design (`docs/README.md`). The benchmark harness referenced in §6
> was throwaway and has already been removed from the tree.
>
> **Status:** EXPLORATION. This is a living document; keep it up to date as we
> gather data and change our minds. Decisions here are provisional until promoted
> into `docs/README.md` §6 and a real implementation plan.
>
> **Owner:** jcalabro (+ Claude). **Started:** 2026-06-27.

## 0. Problem statement

`docs/README.md` §1.1 lists "distributed consensus" as a non-goal and §6 commits
only to **asynchronous active-passive replication** (one leader, read-replica
chains, manual promotion). Jim now wants to explore whether we were too
conservative: can we run a **cluster of jetstream nodes that all field user read
requests and scale horizontally** by adding nodes, with exactly one elected
write-leader taking the firehose (single-writer, the atproto-consumer norm)?

Hard constraints that never move:

1. **Single-node self-host must stay a zero-dependency static binary.** Any
   cluster mechanism must degrade to a no-op when no external infra is
   configured. This is non-negotiable (README §1.1 goal 6).
2. Honor every documented invariant: immutable + sha256/xxh3-checksummed segment
   files, leader-only firehose, at-least-once delivery + idempotent clients,
   `relay/cursor` never ahead of durable segment data (the segment-fsync →
   pebble-`Sync` ordering).
3. "Never crash, never corrupt data"; "crash over corruption" for invalid
   internal state.

## 1. Why this is more tractable than §1.1 implies

Two structural properties of what we already built make single-writer/many-reader
**much** easier than the active-active multi-master we (correctly) ruled out:

- **Segments are immutable + content-checksummed.** So (a) replication = whole-file
  **content-addressed set reconciliation** ("which `seg_*.jss` do I lack? fetch +
  verify sha256") — no Merkle/rsync anti-entropy, which exists to reconcile
  *mutable divergent* replicas we don't have; and (b) **data-level split-brain is
  architecturally impossible** — two writers produce *disjoint* content-named
  files, never a corrupted shared one.
- **The pebble metadata store is (almost entirely) a derived materialized view.**
  `repo/<did>`, `account/<did>`, `sync/chain|host/<did>`, `seq/next` are all
  reconstructible from segments; the DID *list* is reconstructible from the relay
  `listRepos`. Only a few small singletons (`phase`, `compaction/seq`,
  `relay/cursor`) are genuinely node-authoritative. So the "we'd have to replicate
  pebble" fear — which likely drove the §6 non-goal — **can be made to disappear**:
  never replicate pebble; rebuild it locally from already-replicated checksummed
  segments. This is Kleppmann's "turning the database inside out" / CQRS.

So active-active (every node ingests + reconciles) stays out of scope; but
**single-writer + disaggregated reads** is a well-trodden shape (Druid/Pinot
historicals, ClickHouse replicas, Lucene/ES replicas, Thanos/Loki store-gateways).

### Ground-truth correction (verified by grep)

This is **green-field**, not "wiring." Confirmed absent in the server today:

- `atmos`'s `streaming.DistributedLocker` has **zero non-test references** in
  jetstream — leader election is not wired in. (It exists in atmos:
  `streaming/lock.go`, lease-based, 3s lease / 1s renew, `NoopLock` default.)
- No `segment_sealed`/`segment_compacted` frames on the wire — only in-process Go
  callbacks (`mft.OnSegmentSealed`/`OnSegmentCompacted` via `OnAfterSeal` in
  runtime.go) + a "skip unknown control kinds" branch in the *client* decoder.
- No server-side replica ingest sink; no `--upstream` flag in `jetstreamd`; no
  `OnBecameLeader`/`OnLostLeadership` orchestrator transitions; no fencing epoch.

## 2. The three cruxes (decision axes)

Everything downstream hangs on these three:

- **Crux A — Introduce a *shared mutable* object, or not?**
  - NO (per-node pebble+manifest, rebuilt locally): a zombie ex-leader can only
    write orphan files in its own dir. **Fencing tokens become structurally
    unnecessary** — immutability *is* the safety property; the lease is pure
    liveness. (Architecture A.)
  - YES (shared manifest-of-record / cluster-global seq in S3/etcd/FDB): you've
    created the one object two leaders can race on; you **must** fence it with a
    monotonic epoch checked *at the commit, storage-side* (Kleppmann; Jepsen etcd
    3.4.3 lost ~18% of acked writes under pauses with lease-only). (Architectures
    B, C.) NOTE: `atmos`'s `DistributedLocker.Acquire` returns only `error`, no
    epoch, and none of the 5 commit sites compare an epoch — so B/C are *less safe
    than active-passive* until that fence is actually built.
- **Crux B — Are cluster-global, failover-stable cursors a product requirement?**
  `seq` is hard instance-local today (`writer.go` `nextSeq` from `seq/next`,
  reconciled by `ScanMaxSeq`); not derived from upstream relay cursor. Keeping
  per-node seq = "rewind-and-dedup on failover" (already the v1 firehose
  contract). The nastier discovered issue: with an LB read-pool a cursor client
  can be moved between read nodes on a routine health flap, and **there's no
  instance/epoch token on the wire** to detect the namespace change → silent wrong
  slice. Cheap fixes regardless: sticky routing + an instance/epoch id on every
  frame. Cluster-global seq (Arch C) solves it at the root but needs Crux A = YES.
  - (Trap: `time_us` cursors don't fail over better — `merge_runner.go` re-stamps
    `IndexedAt` to local merge wall-clock, so time cursors are also node-specific
    for merged data.)
- **Crux C — In-place compaction rewrite is single-node-safe, cluster-unsafe.**
  `segment/rewrite.go` does `tmp → os.Rename` onto the same path with a new
  checksum. Every researched columnar/search system instead writes a *new
  generation* file, advertises it, lets readers fully open it, atomically flips a
  pointer, GCs the old after a grace window. New-generation filenames
  (`seg_<idx>.<gen>.jss`) + delayed GC is independently right, safe single-node,
  and a prerequisite for clustering — **but** it's a cross-cutting format change:
  `ParseSegmentIndex` (`internal/ingest/filename.go`) rejects anything but
  `seg_<10-digit base36>.jss` (round-trips `SegmentFilename(idx)==name`), and
  `SegmentFiles` skips non-matching names. That parser feeds merge drain,
  compaction sweep, writer recovery, and the §3.4 lexicographic-sort invariant.

## 3. Candidate architectures (from the design+critique workflow)

| | A — Managed Read Cluster | B — Disaggregated / Object-Store | C — Symmetric / External HA Metadata |
|---|---|---|---|
| Shared mutable state | **None** (Crux A=no) | manifest-of-record + cursor in small control store | everything in FDB |
| Cursors | per-node (rewind on failover) | global for sealed data | **cluster-global, monotonic across failover** |
| Fencing needed? | no (immutability) | yes, at manifest commit | yes, at all 5 commit sites |
| New deps | lease backend + LB (optional→NoopLock) | object store + etcd + gossip | **FoundationDB (CGO) + S3 + fenced lease** |
| Complexity | low–medium | very-high | very-high |
| Single-node preserved | yes | yes | yes |
| Best fit | one operator, horizontal read fanout, sticky cursors | Bluesky-scale public archive on R2+CDN | the one largest operator w/ hard "never-rewind" req |

All three preserve single-node identically: every cluster mechanism degrades to a
no-op (`NoopLock`=always-leader, LocalFS backend, control store→local pebble keys).
Shared discipline: **one codebase, both paths exercised in CI forever** (Loki/Mimir
`-target` model) incl. a single-node latency/throughput regression gate (the
object-store seam can defeat `getSegment`'s `sendfile(2)` fast path if shaped wrong).

## 4. The current working hypothesis (what we're validating)

**"Reconstructible pebble + warm continuously-maintained followers."** Start at
Architecture A (Crux A = no shared mutable state). The key viability claim, in
Jim's words:

> Each node maintains its own pebble store on the fly as we go; when a failover
> happens it already has the metadata up to date, so it can take over writes
> quickly. Full reconstruction-from-scratch is NOT on the failover path.

There are **three distinct "reconstruction" operations**, and viability depends on
keeping the slow one off the critical path:

| Path | When | Bounded by | Target |
|---|---|---|---|
| Warm continuous maintenance | steady-state follower | stream lag | failover path; **seconds** |
| Incremental catch-up | follower briefly behind | downtime delta | minutes |
| Cold rebuild from genesis | new node / torn pebble | total archive (~TB, ~30M DIDs) | **must NOT be on failover path** |

**Why it's clean:** a follower is the *same `ingest.Writer` pipeline* fed by the
extended websocket instead of the relay. The leader's per-block pebble writes are
exactly three hooks (verified in `ingest/config.go` + `live/consumer.go`):
- `OnAppend` → tombstone `Observe` (maintains the tombstone tail continuously)
- `OnDurableBatch` → stages `seq/next` + `repo/<did>.Rev` into the synced batch
- `OnAfterFlush` → `saveCursorAndSyncState` → `relay/cursor` + verifier state
A follower runs the same hooks, same segment-fsync→pebble-`Sync` ordering, so its
pebble stays current to within **stream lag + one unsealed block**, and a follower
crash leaves its pebble no further ahead of its own segments than the leader's.

**Warm at promotion:** `relay/cursor` (upstream resume), `seq/next` (own counter),
`repo/<did>.Rev`, tombstone tail (only the uncompacted window above
`compaction/seq`, never the whole archive), manifest (dir scan bounded by segment
*count*, not 30M keys).

**The one real gotcha — verifier (sync 1.1) chain state.** The leader runs atmos's
verifier with a pebble-backed `StateStore`. A follower tailing the *extended*
stream gets already-verified events, so it isn't doing full MST verification. On
promotion + re-anchor to the relay, the verifier needs per-DID chain state or the
first commit per active DID can chain-break → resync storm. Mitigation fits the
model: maintain chain-rev from the stream as we go (the `rev` is on every commit
row). **This is the part that needs deliberate design, not a free side effect.**

**Cosmetic, OK to lose (already §6.3):** backfill retry counters, `LastError`,
in-flight sync state, `phase`. New leader rebuilds DID list via `listRepos` and
re-queues incomplete repos (the documented "minutes-scale warmup").

**Cold path still exists** (new node, or torn pebble discarded under
crash-over-corruption): replay segments to repopulate ~30M `repo/<did>`. Mitigation
(Loki tsdb-shipper / Thanos): leader periodically ships a **pebble SST
snapshot/checkpoint** to the same HTTP/object path it serves segments from; a fresh
node seeds from the snapshot and replays only the delta.

## 5. Experiments to validate/disprove (Jim's "no perf claims unless measured")

Three measurements decide viability + one correctness test. Tracked as tasks #2–#4.

1. **Cold rebuild time** (#2): wall-clock to repopulate ~30M `repo/<did>` rows into
   pebble. Sizes the new-node-join and torn-pebble paths. Decides whether SST
   snapshot shipping is mandatory or merely an optimization.
2. **Pebble write throughput vs firehose rate** (#3): can the per-block synced
   batch (`seq/next` + N `repo/<did>` + cursor) keep up so warm-follower lag stays
   small? Bluesky firehose is roughly ~10^3 events/s order-of-magnitude (to be
   confirmed); blocks are 4096 events / ≤30s.
3. **Manifest Open scan time** (#4): confirm node-restart manifest rebuild is
   bounded by segment count (seconds), not the 30M-key scan.
4. **(later) promote-mid-stream oracle**: assert a promoted node's pebble ==
   from-scratch rebuild. If they ever diverge, "maintain on the fly" silently lies.
   Build red-first.

### Method notes
- These are micro/component benchmarks on real code paths (`store.Store`,
  `backfill` encode, `manifest.Open`), not a full cluster. They bound the *floor*
  of cost; real cold rebuild also pays segment decompression+scan, measured
  separately if the KV floor looks fine.
- Machine: 32 cores, 123 GiB RAM, Go 1.26, Linux. Record raw numbers; note this is
  a beefy dev box, not prod hardware.

---

## 6. Findings (append as we go)

> _Filled in by the experiments below. Newest at the bottom._

**Experiment harness (throwaway, already REMOVED from the tree — these were
`JETSTREAM_HA_BENCH=1`-gated `t.Skip` tests, deleted after capturing results here
per zero-tech-debt; recreate from this spec if we need to re-measure):**
- was `internal/store/harebuild_bench_test.go` — `TestHA_ColdRebuild` (bulk-load N
  `repo/<did>` rows in 10k `NoSync` batches + final `Sync`+`Flush`; realistic
  ~325B steady-state JSON value), `TestHA_WarmThroughput` (per-block `Sync` batch =
  `seq/next`+`relay/cursor`+K full repo-row rewrites).
- was `internal/manifest/hamanifest_bench_test.go` — `TestHA_ManifestOpenScan`
  (write S sealed segments × 64 blocks, tiny payloads but full-size footers, time
  `manifest.Open`+`Wait`).
- Env knobs: `JETSTREAM_HA_ROWS`, `JETSTREAM_HA_BATCH`, `JETSTREAM_HA_BLOCKS`,
  `JETSTREAM_HA_DIDS_PER_BLOCK`, `JETSTREAM_HA_SEGMENTS`, `JETSTREAM_HA_BLOCKS_PER_SEG`.
- Machine: 32 cores, 123 GiB RAM, Go 1.26, Linux (NVMe). Beefy dev box, NOT prod;
  treat as an upper bound on a well-provisioned node, order-of-magnitude for prod.
- On Linux `store.SyncWrites = pebble.Sync` (real `fsync(2)`), so the warm test
  measures true production durability cost.

### 6.1 — Cold pebble rebuild (KV write floor) ✅ MEASURED 2026-06-27

`TestHA_ColdRebuild`: bulk-load N `repo/<did>` rows (realistic ~325-byte steady-state
JSON value), 10k-row `NoSync` batches + final `Sync` + `Flush`.

| rows | load | rate | on-disk |
|---|---|---|---|
| 1,000,000 | 1.19s | 843K rows/s | 0.07 GiB |
| 30,000,000 | **37.7s** | 795K rows/s | 1.97 GiB (70.5 B/row) |

- **Linear, no degradation** across 30M (per-5M splits steady at ~795–810K rows/s).
- This is the **pebble-write floor only** — it does NOT include reading +
  zstd-decompressing + scanning segment blocks to *produce* the rows. Real cold
  rebuild = this + segment scan cost (measure separately if needed; the I/O to read
  ~TBs of segments will dominate, so the KV side is provably not the bottleneck).
- **Verdict:** the KV repopulation floor for a from-genesis rebuild is ~40s at
  whole-network scale. Even if segment-scan makes the real number 10–30× larger,
  cold rebuild is **minutes, not hours**, on a good node — and it is OFF the
  failover path regardless. SST-snapshot shipping is an optimization, not a
  prerequisite. Plan claim (cold rebuild must not be on failover path) holds, and
  even the cold path is not scary.

### 6.2 — Warm follower throughput (per-block synced commit) ✅ MEASURED 2026-06-27

`TestHA_WarmThroughput`: per-block batch = `seq/next` + `relay/cursor` + K full
`repo/<did>` row rewrites, committed `pebble.Sync` (real fsync). K=2000 distinct
DIDs/block (deliberately heavy; a 4096-event block rarely touches 2000 distinct DIDs).

- 2000 blocks in 5.02s → **398 blocks/s**, per-block synced commit **≈2.5ms**.
- At 4096 events/block ⇒ **~1.63M events/s sustainable**.
- Bluesky firehose is order ~10³ events/s ⇒ **~1000× headroom**. fsync latency
  (~2.5ms/block) is the binding constraint, and blocks flush at most every 30s /
  4096 events, so a warm follower's pebble maintenance is **nowhere near**
  throughput-bound.
- **Verdict:** the warm continuous-maintenance model is feasible by a wide margin.
  A follower keeps its pebble current at within stream-lag + one unsealed block; the
  per-block synced write is cheap relative to the firehose rate. Plan claim holds.

### 6.3 — Manifest Open scan time ✅ MEASURED 2026-06-27

`TestHA_ManifestOpenScan`: write S sealed segments (64 blocks each — realistic for a
256MB segment with footer blooms sized for 4096 events/block), time `manifest.Open` +
`Wait`. `readSealedMetadata` reads only the footer (header + block index + per-block
blooms + collection index), never the block payload, so tiny payloads reproduce
real footer cost without TBs of disk.

| segments | ~archive size | open (warm cache) | per-segment |
|---|---|---|---|
| 500 | ~0.13 TB | 43ms | 0.09 ms |
| 12,000 | ~3 TB | **1.06s** | 0.09 ms |

- **Linear at 0.09 ms/segment**, parallelized across `GOMAXPROCS` (32 here).
- **Caveat:** warm-cache (files just written, in page cache). Cold restart reads
  footers from disk — but footers are small (~tens of KB) so even cold it's ~12k
  small `pread`s at 32-way concurrency: seconds, not minutes. (Could measure with
  cache drop later if we want the cold number precisely.)
- **Verdict:** manifest rebuild on node restart is bounded by **segment count**
  (~1s @ 3TB), categorically separate from the 30M-key pebble scan. Plan claim
  (manifest = seconds, off the critical path) holds.

### 6.4 — Summary: does the plan survive contact with data?

**Yes, the warm-follower / reconstructible-pebble hypothesis is validated on the
dimensions measured.** At the instant of failover a warm follower's failover-critical
state is all continuously maintained and cheap:
- per-block pebble maintenance: ~1000× firehose headroom (6.2)
- manifest rebuild: ~1s @ 3TB (6.3)
- even the worst case (cold rebuild, which is OFF the failover path): KV floor ~40s
  @ 30M rows, minutes-not-hours even with segment-scan overhead (6.1)

**The binding risk is NOT throughput or rebuild time — it is the verifier chain-state
warm-maintenance gotcha (§4) and the cross-cutting correctness items (Cruxes B/C).**
Next: the promote-mid-stream oracle (prove warm-maintained pebble == from-scratch
rebuild) and a design for verifier chain-state on followers. These are correctness,
not performance, and are where the real work is.

---

## 7. Open questions / follow-ups (kaizen list)

- Verifier chain-state warm-maintenance design (the §4 gotcha) — needs its own note.
- Exact wire-level instance/epoch token + sticky-routing contract for cursor
  clients on an LB read pool (Crux B), independent of which architecture wins.
- New-generation segment filename format change (Crux C): `ParseSegmentName →
  (idx, gen, ok)` audited across merge/compaction/recovery/manifest; preserve
  lexicographic-sort-equals-creation-order.
- If/when Crux B forces shared metadata: real fencing epoch from `atmos`
  `DistributedLocker.Acquire` + compare-and-reject at all 5 commit sites
  (`commitDurableBatchLocked`, `OnDurableBatch`, `commitSourceComplete`,
  `saveCompactionWatermark`, `saveCursorAndSyncState`).
- Measure real Bluesky firehose event rate to replace the order-of-magnitude
  estimate in #3.

## 8. References

- Kleppmann, "How to do distributed locking" (fencing tokens).
- Jepsen: etcd 3.4.3 (lease-only lost ~18% acked writes under pause).
- Kleppmann, "Turning the database inside out" (CQRS / derived views).
- Druid storage + compaction; Pinot prod guide; ClickHouse ReplicatedMergeTree;
  Lucene/ES segment replication; Quickwit metastore (new-generation, never in-place).
- Thanos/Cortex store-gateway; Loki tsdb-shipper; WarpStream (object-store source
  of truth, stateless caches).
- FoundationDB known-limitations (10MB/5s tx cap); etcd ~8GB ceiling.
- Bluesky relay-ops (upstream cursor resets on relay swap).
