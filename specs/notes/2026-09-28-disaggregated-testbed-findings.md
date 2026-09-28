# Disaggregated mode: local test-bed findings (2026-09-28)

These tests ran against the local disaggregated test bed on branch
`jc/new-storage` (acb0c1e). The setup was `JETSTREAM_STORAGE=disaggregated just run-prod serve`
against SeaweedFS and Postgres from `just up`, after a small partial prod
backfill, in steady state, ingesting live from bsky.network at about 450 ev/s.
There were two pods: A on :8080/:6060 and B on :8081/:6061. The archive had
one sealed segment (seqs 1..2969450, generation 2), one active segment, and
about 5.1M events by the end.

The ad-hoc harness is `_jstest/`, which is git-excluded. Its subcommands are tail, cmp, archive,
client, fanout, slow and objects.

## Findings, most severe first

### 1. A transient S3 outage crashes the process (high)

With SeaweedFS paused for 120s, pod B took over the lease. Its session start
ran `rebuildLiveTombstones`, and the block fetch gave up after the 30s
`RetryTimeout`. The process then exited:

```
leader: session ended with a fatal error epoch=12 err="orchestrator: compaction:
rebuild decode segment 1 block 407: s3: get ...: gave up after 3 attempts in 30s"
exit status 1
```

`leader.DefaultFatal` treats every error that doesn't wrap `ErrRestartSession`
as fatal. Upload failures go through the fenced catalog path and get wrapped
as `catalog.ErrSessionEnded`, so they restart the session. Object-store read
failures on session start come back unwrapped, which makes them fatal. That
covers the tombstone rebuild (`compact_deletes.go:645`) and very likely the
maintainer's hot-state rebuild (`maintainer/rebuild.go` → `decodeHot` pointer
fetches), which blocked for 20.7s in the same test and only survived because
S3 came back. This contradicts design §1838 ("S3 unreachable … the session
ends") and the never-crash rule. In k8s, every pod that takes the lease during
an S3 blip longer than 30s crashloops, and crashloop backoff then delays
recovery after S3 returns.

Fix: classify exhausted-retry and unavailable object-store errors as
session-restart. Checksum or length mismatches must stay fatal.

Test: a leader session test with an objstore fake that fails `Get` for longer
than `RetryTimeout` at session start. Assert that the session restarts and is
not fatal. Add it to `internal/jetstreamd` or the lockertest harness.

### 2. Timestamp cursors silently skip events in the bootstrap region (high)

`witnessed_at` is not monotonic with seq for seqs 25112..1958598, which is
the backfill and merge region. There are 1.64M regressions, and the worst is
12.08s. `backfill/handler.go:111` stamps one `witnessedAt` per repo at the
start of `HandleRepo`. Concurrent workers then append after admission and
writer waits, so later seqs can carry earlier stamps. docs/README.md:337 and
the lexicon both promise monotonicity, and the timestamp cursor lookup relies
on it.

Measured impact: `cursor=1790608280919350` resumed at seq 713182 when the
correct answer was 684796, which **silently skips 3,078 events**. A second
probe skipped 1,329 events. v1 clients are hit hardest because their cursor
*is* `time_us`: a v1 replay of that region showed 912k duplicate or
non-monotonic `time_us` values. The steady-state region is exact. This
probably also affects local mode.

Fix: clamp `witnessed_at = max(witnessed_at, prevWitnessed)` where seq is
assigned, in the writer, so every source gets it for free. Existing archives
need either a lookup that tolerates regressions (for example, a per-block
max-witnessed prefix) or a documented caveat.

Test: an oracle or property check that `witnessed_at` never decreases with
seq across bootstrap, merge and steady state, with concurrent backfill
workers and an admission stall injected. Add a timestamp-cursor property
test: for random T, the first delivered event is the first seq with
`witnessed ≥ T`.

### 3. Sync-1.1 resyncs bypass bulk admission and spike live latency (high)

A whole-repo resync from `ResyncAsync` flows through the live consumer, which
tags `ingest.ClassLive` at `live/consumer.go:485`. Only the failed-repo retry
loop tags bulk (`backfill/retry.go:100`). One resync (did:plc:ubsowh2uiwgltyrggr6wpnz5,
53,742 events) raised live p99 from 18ms to 790ms for about 5s, spilled into
pointer batches, and left 0 bulk batches in the metrics. Design §10.5 (lines
809 and 884) says resync replacements are bulk.

Fix: tag resync replacement appends `ClassBulk`, which sends them through the
bulk token bucket and pointer path.

Test: a live-consumer unit or simulator test in which a resync event's
appends land in the bulk class. Also a latency oracle in which a large resync
does not move live-class p99.

### 4. Records outside the atproto data model are archived but unservable or inconsistent (medium)

The ingest gate accepts records that violate the atproto data model, and the
read paths then disagree about them:

| Class | Example seqs | v2 websocket | Go client (archive decode) |
|---|---|---|---|
| float64 values (`net.anisota.*`, `app.blento.node`) | 5482, 74927, 330984.. | encode error: silent gap, WARN per subscriber per event | recoverable decode error |
| non-SHA-256 CID (hash 0x1e) | 6917–6920 | encode error: silent gap | decode error |
| record is a CBOR *string* of JSON, not a map | 349798, 349799, 521760 | **served** as `"record": "<json string>"` (lexicon violation) | decode error, so the event is missing |

In total, 98 seqs are unservable over websocket and 101 are missing from a
full Go-client replay. Every subscriber logs a WARN per bad event, so heavy
replay turns this into a log-amplification vector.

Fix: validate the data model at the ingest gate (`docs/README.md` §4.4). Drop
bad records with an `ingest_dropped_events_total{reason="invalid_data_model"}`
metric, per AGENTS.md. Also decide what to do with the already-archived rows,
and rate-limit or aggregate the per-event encode WARN.

Test: fuzz or property test that anything the ingest gate accepts round-trips
through `EncodeV2`, v1 `Encode` and the client decoder. Add the three example
payloads above to the corpus.

### 5. Cold reads hang for 30s and return 500 when S3 is down (medium)

Design §1838 says cold reads fail with 503 during an S3 outage. Observed: an
uncached `getBlock` blocked for the full 30s `RetryTimeout` and then returned
**500** (`getblock.go:127` maps every read failure to InternalError). The
v2/v1 cold path has the same fetch underneath. Every cold request holds a
connection and goroutine for 30s, so a CDN or client retry storm piles up
quickly. The 503 test in `head_test.go` covers readiness, not this path.

Fix: give reader fetches a shorter budget than writer uploads. Map
unavailable and exhausted-retry errors to 503 with Retry-After.

Test: an xrpcapi test with a failing objstore fetch that asserts 503 within a
bounded time.

### 6. A PG stall longer than the lease causes a pointless double failover (low–medium)

Every PG pause (5s and 15s) produced two leader flips, epochs 6→7→8 and
8→9→10. The new leader lost its lease 1s after acquiring it. The cause is at
`leader.go:148`: `acquiredAt` is the start of the Acquire attempt. When
Acquire blocks on a frozen PG and commits at unpause, the local lease clock
is already about 3s old, and the first renew misses. This is safe
(conservative) but adds an extra ingest stall and rebuild per incident.

Fix: after an Acquire that took longer than about Lease/2, renew immediately
before starting the session, or bound Acquire with a Lease-length context.

Test: lockertest with an Acquire that blocks for longer than Lease, then
succeeds. Assert there is no immediate lease loss.

### 7. Replay load on a pod degrades that pod's live tail (medium, capacity)

100 concurrent 50k-event replays on one pod (about 875k ev/s aggregate, 32
cores) raised that pod's live-tail p99 from 19ms to 425–738ms, with a max of
1.65s. Ingest on the other pod was unaffected. Nothing prioritizes live-tail
subscribers over cold replays, and there is no cap on concurrent cold
readers. Scaling out followers mitigates this, but a single pod is easy to
saturate.

Recommendation: add a per-pod concurrency or CPU budget for cold readers, and
a metric for it.

### 8. Retained zstd encoder history is about 512MB and scales with cores (medium, memory)

The leader heap is about 2.3GB in use and RSS about 4.9GB at idle, peaking
at 7.7GB under fan-out. 512MB of that heap is
`zstd.(*fastBase).ensureHist` reached from `segment.(*BlockBuilder).Encode`.
`segment/zstd.go:28` builds `blockEncoder` with the default concurrency
(GOMAXPROCS, 32 here), and each encoder keeps a history buffer about 16MB
across block-sized inputs. A 96-core prod node would retain about 1.5GB. The
subscribe encoders already set `WithEncoderConcurrency(1)` and a small window.

Fix: cap `WithEncoderConcurrency` to match the real number of concurrent
block encoders (maintainer and seal), or pool encoders explicitly.

### 9. Minor

- **Stale gauges on followers.** After losing the lease, a pod keeps
  reporting `jetstream_leader_epoch` and `jetstream_hot_unfolded_events`
  (B showed epoch 4 and unfolded 18141 while following). Dashboards will lie.
  Reset these gauges when the session ends.
- **Slow-consumer drops are invisible.** 200 stalled readers were cut off by
  `frameWriteTimeout` (5s), but `adversarial_drops_total=0` and
  `clean_disconnects_total=3`, and no metric counted them. Add a
  `disconnects_total{reason}` metric.
- **GC starves under session churn.** `runGC` starts its ticker at session
  start (disagg.go:731), so a leader whose sessions restart more often than
  every `GC_INTERVAL` (10m) never runs GC. The same may apply to other
  interval tasks. Consider persisting a last-run time.
- **Seq cursor beyond the tip** silently resumes at the live tip, delivering
  seqs *below* the requested cursor, for example a cursor carried over from
  another archive. Consider an info frame or an error.
- **Timestamp cursor before archive start** reports "below retention floor",
  which is misleading for an archive that never had that data.
- **Error-level logging for client-triggered reads.** `getBlock: read block
  object failed` logs ERROR per request during an S3 outage.
- **Session start blocks on S3.** With S3 down, ingest stopped entirely
  about 30s in, because every new session start needs S3 (tombstone rebuild,
  pointer batches). The design's "inline batches keep committing until the
  unfolded cap" applies only within the first session. That's acceptable,
  but the tolerance should be documented as "about RetryTimeout", not "until
  the unfolded cap".

## What held up

- **Catalog and object integrity.** Full `CheckInvariants` passed before and
  after all chaos. All 1,493 available objects verified sha256 and length
  (488MB). There were no dangling references and no shared objects. The
  three abandoned `uploading` rows from failed uploads are covered by
  `GC_ORPHAN_AGE`.
- **Archive endpoints.** Checksums and sealed metadata verify. All 726
  `getBlock` frames match the byte ranges of the full segment. Random,
  suffix, open and unsatisfiable Range requests behave correctly, and
  `If-None-Match` works. Hostile names return 400, and planSnapshot
  validation is solid (bad args, 20k DIDs, a 50MB body returns 413, GET
  returns 405).
- **Replay determinism.** A full v2 replay from cursor 1 (4.2M events in 36s,
  about 117k ev/s per subscriber) is byte-identical across two concurrent
  runs, the follower, the live capture, and an `EncodeV2` of the archive.
  Cursor boundaries are exact across the sealed, active and hot tiers.
- **Multi-pod.** The follower's live stream is identical to the leader's
  with the same latency (p99 about 19ms), so NOTIFY wake-ups work.
- **Failover and fencing.**
  - SIGKILL of the leader: takeover in 2.66s, with a 3.2s delivery stall.
  - SIGSTOP of the leader for 8s, then SIGCONT: the old leader was fenced
    ("fenced out by a newer writer epoch") and rejoined as a follower.
  - `pg_terminate_backend` on all connections: session restart and
    takeover, with a 2.9s stall.
  - PG paused for 5s and for 15s: 5.6s and 16.1s stalls.
  - S3 paused for 40s: ingest continued for 32s on inline batches.
  - In every case, both pods' streams had **0 gaps, 0 dups, and continuous
    seqs**, and the relay cursor resumed correctly.
- **Go client.** A full archive replay into live (5.1M events, caught up in
  about 40s, about 150k ev/s) survived a leader SIGKILL mid-stream. The only
  missing seqs were the 101 from finding 4.
- **Slow readers.** 200 stalled replay readers kept memory bounded
  (462→565MB) and left live latency unaffected.
- **PG write load.** About 1 `metadata_kv` upsert per event (sync state), 1
  archive-row update per hot batch, and about 200KB/s of WAL at 450 ev/s.
  This scales linearly and is fine at the 3k ev/s target.

## Suggested automated tests, by priority

1. S3 unavailable at session start restarts the session and does not crash
   the process (finding 1).
2. `witnessed_at` stays monotonic with seq across all phases (an oracle
   invariant), plus a timestamp-cursor property test (finding 2).
3. Resync appends use the bulk admission class (finding 3).
4. Anything the ingest gate accepts round-trips through every encoder and
   the client decoder, with a fuzz target and corpus seeds (finding 4).
5. Cold reads return 503 within a bounded time when objstore is down
   (finding 5).
6. A slow Acquire does not cause an immediate lease loss (finding 6).
7. A CI chaos tier covering PG pause and termination and S3 pause against
   two pods, asserting no gaps, no dups and no process exit. The oracle
   already has the stream comparison machinery. What's missing is fault
   injection into PG and objstore.
