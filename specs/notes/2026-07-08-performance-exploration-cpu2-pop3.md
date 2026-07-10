# Performance exploration — server + client on cpu2-pop3 (2026-07-08)

Status: **measurement report for Jim's review. No issues filed yet.**

Purpose: first systematic performance and resource-usage characterization of
both the jetstream server and the Go client against real full-scale data, on
production-class hardware. Establish baselines, find the actual bottlenecks
(not the assumed ones), and rank improvement opportunities that don't break
the system's contracts.

Method: both instances on cpu2-pop3 (AMD EPYC 9745, 128c/256t @2.4GHz, 1.5 TiB
RAM, md raid0 over 2× NVMe, kernel 6.8) were booted into steady state from
their existing archives — jetstream-1 (full network, 6,320 sealed segments,
1.6 TiB) on :8080/:6060 and jetstream-2 (100k repos, 2,927 segments, 801 GiB)
on :8081/:6061. Load was driven with `client loadtest` (raw websocket fan-out)
and `client subscribe --backfill-only` (archive download path), with
before/after Prometheus counter diffs, 30s pprof CPU/heap captures on server
and client (`--debug-pprof-addr`), `/usr/bin/time -v`, a 5s-interval
/proc-based CPU/RSS/IO sampler, iostat, and `ss` socket-queue sampling.
Raw artifacts: `/data/jcalabro/perf/` on cpu2-pop3 (logs, counter snapshots,
`profiles/*.pb.gz`). Firehose live rate during the tests was ~370–520 ev/s.

Every number below is measured, not extrapolated; single-run unless noted, but
counter-diff and profile agreement was checked for each headline claim.

---

## 1. Baselines (idle steady state, no subscribers)

| | jetstream-1 (full net) | jetstream-2 (100k repos) |
|---|---|---|
| Sealed segments / archive size | 6,320 / 1.6 TiB | 2,927 / 801 GiB |
| Boot → serving (warm page cache) | ~19 s (manifest open 7 s) | ~16 s (manifest open 12 s) |
| RSS | ~73–77 GiB | ~155–162 GiB |
| Heap in-use | 58 GiB | 131 GiB |
| — of which gloom bloom filters | **49 GiB (90%+)** | **88 GiB (90%+)** |
| Idle CPU (60 s counter diff) | ~0.3 cores | ~2.2 cores¹ |
| Goroutines | ~610 | ~640 |
| GC pause p50 / max | 0.9 ms / 17 ms | similar |
| Ingest lag behind relay | 0.05–0.12 s | 0.05–0.12 s |
| Disk IO | ~8–12 MB/s writes, %util < 0.5% | same device |

¹ js2 was not truly idle: its relay cursor was ~21M seqs behind js1's for the
whole session, so it was replaying the firehose at ~5.1k ev/s (js1's true live
append rate was ~400 ev/s). js2's "idle" 2.2 cores is catch-up ingest +
signature verification, and it inflates js2-side server-CPU deltas in §5.

Two surprises worth recording:

- **The smaller archive uses ~1.8× the memory of the full-network one.**
  Bloom RSS is driven by *block count*, not archive size: per-block DID blooms
  are a fixed 8,409 bytes regardless of block fill (`segment/inspect.go`,
  footer layout). Backfill-written js1 segments are dense (857 blocks/segment,
  ~4,091 events/block); js2's live/merge-written segments average ~919
  events/block (4,004 blocks/segment) — ~4.5× the bloom bytes per event.
  2,927 segs × ~4,000 blocks × 8.4 KiB ≈ 98 GiB, matching the heap profile
  (`gloom.makeAlignedUint64Slice` via `manifest.readSealedMetadata` →
  `LoadAllBlockBlooms`, `internal/manifest/manifest.go:541`).
- **Steady-state CPU is dominated by the sync verifier's secp256k1
  commit-signature checks** (~30% of idle profile samples:
  `secp256k1montgomery.Mul/Square`), plus firehose decode. It scales with
  ingest rate: ~0.3 cores at js1's 400 ev/s live rate, ~2.2 cores at js2's
  5.2k ev/s catch-up rate — so budget roughly ~0.4 cores of crypto+decode per
  1k ev/s of firehose.

## 2. Live-tail fan-out (`/subscribe`, hot readable-log path)

Ladder, unfiltered, no compression, 90–180 s per rung. "Server CPU" is
Δ`process_cpu_seconds_total`/wall and still includes js1's ~0.3-core ingest
baseline; "CPU per delivered event" is the same delta ÷ Δ`events_sent_total`
(so it overstates true marginal cost at the low rungs).

| conns | aggregate delivered | wire throughput | server CPU | CPU per delivered event |
|---|---|---|---|---|
| 1 | 377 ev/s | 0.21 MiB/s | 0.24 cores | — |
| 10 | 3.8k ev/s | 2.1 MiB/s | 0.36 cores | ~94 µs (noise floor) |
| 100 | 38k ev/s | 21 MiB/s | 1.26 cores | 34 µs |
| 500 | 130k ev/s | 71 MiB/s | 3.2 cores | 24 µs |
| 1000 (one loadtest proc) | 137k ev/s | 75 MiB/s | 4.0 cores | 24 µs |
| 1000 (4 procs × 250) | **420k ev/s mid-run** (336k avg) | ~230 MiB/s | 13.3 cores | 40 µs |

Findings:

- **The single-process loadtest client is the bottleneck above ~140k ev/s**,
  not the server (each 250-conn client process burned ~6.3 cores). The 4-proc
  split proved the server sustains 420k+ ev/s of hot fan-out at 13 cores on a
  256-thread box. Enormous headroom; record this so future load tests always
  use multiple driver processes.
- **Hot-path CPU is contention, not encoding.** At c=1000 the profile is
  ~25% runtime lock machinery (`futex`/`lock2`/`procyieldAsm`/`lockSlow`) and
  24% cumulative in `Tail.ReadFrom` — of that, 18.8 s/60 s in `Tail.mu`
  (taken *every* ReadFrom iteration and again in `Tip()` every batch,
  `internal/subscribe/tail.go:128,180`) and 9.6 s in the readlog RWMutex
  (`internal/ingest/readlog.go:191`). JSON encode is nearly invisible live
  because of encode-once memoization (~400 unique events/s).
- **End-to-end delivery latency is excellent and load-insensitive**: via the
  library client, p50 13.2 ms / p99 20.6 ms idle → p50 15.2 ms / p99 20.9 ms
  with 800 concurrent subscribers. (Floor ≈ client batcher's 20 ms flush.)
- **Adversarial-slow drops never fired** (0 across all tests) — correct, since
  these clients were fast.

### 2a. Server drops healthy-but-saturated connections every ~50 s (top finding)

At ≥500 unfiltered connections every connection died with client-side
`read EOF` on a ~40–60 s cadence (c=500: 267 reconnects by t=50s, all 500 by
t=60s, next wave ~50 s later; same signature at c=800/c=1000 and in the cold
c=100 test). `ss` showed send-queue spikes to 54 MB concurrent with the drop
waves. Zero `adversarial_drops`; `clean_disconnects` only counts ctx-done.

Mechanism (code-confirmed, `internal/subscribe/handler.go:384–496`): under
saturation the 5 s `frameWriteTimeout` on `conn.Write`, and the 30 s keepalive
`conn.Ping` — which *waits for a pong* that must queue behind megabytes of
backlogged frames (coder/websocket `Ping` semantics) with the same 5 s
deadline — both `return` and kill the connection. **Neither path increments
any metric**; from the operator's view these disconnects are invisible, and
from the client's view the server just vanished. In production this converts
a throughput-saturated-but-healthy client into a reconnect storm — and real
reconnects resume *with a cursor*, i.e. each drop converts hot fan-out into
cold replay load. This is the single most production-relevant finding.

## 3. Filter and compression variants (c=100, 120 s each)

- **Collection filter** (`wantedCollections=app.bsky.feed.like`): works as
  designed; non-matching events cost only `Filter.Wants` + a counter. Delivered
  24.3k ev/s with 1.37M filtered; per-scanned-event cost ≈ unfiltered.
- **permessage-deflate** (`--compression`): server CPU **1.9×** the
  uncompressed run (2.36 vs 1.26 cores) for the same event stream. The write
  path is 67% `writeCompressedFrame` → `compress/flate.(*Writer).Flush`.
  Per-connection flate contexts (~1.2 MB each, no cross-connection sharing)
  compress the same bytes N times, versus the v1 `compress=zstd` path whose
  shared-dictionary frame is memoized once per event
  (`internal/subscribe/compress.go`). Guidance for consumers should prefer
  the zstd query param at high fan-out.

## 4. Cold replay (cursor behind the readable log)

| scenario | delivered | throughput | notes |
|---|---|---|---|
| 1 conn, 6 h-old cursor | 79k ev/s | 43 MiB/s | steady catch-up, no drops |
| 10 conns, 2 h cursor | 152k ev/s agg | 83 MiB/s | |
| 100 conns, 2 h cursor (one proc) | 123k ev/s agg | 67 MiB/s | driver-bound + drop churn |
| 100 conns, 2 h cursor (4 procs) | **569k ev/s agg** | ~313 MiB/s | 6.6 cores server CPU, 11.5 µs/event |

- Disk was irrelevant (everything in the 1.1 TiB page cache; iostat reads ≈ 0).
  A cold-cache replay would look different; not measured here.
- **Cold-path CPU is dominated by re-encoding, and encodes are not shared
  across subscribers.** In the 100-cold-subscriber profile, 60% of subscriber
  CPU is `Entry.Encoded` → `encodeCommit` (34% `json.Marshal`, 21%
  `cbor.ToJSON`). Cold reads build fresh `Entry` objects per subscriber per
  batch (`internal/subscribe/replay.go:414`), so N subscribers replaying the
  same window (the exact post-deploy reconnect-herd shape) pay N× the CBOR→
  JSON cost that the hot path pays once. The hot path's memo lives on the
  shared `ReadLogEntry`; cold entries have no shared home.
- Same-block re-decode across subscribers is mitigated by the 64 MiB block
  cache, but a subscriber advancing in 1024-event batches through 4096-event
  blocks still re-hits the cache 4× per block, under a single global mutex
  (`internal/subscribe/blockcache.go:101`). No hit/miss metric exists to size
  this cache empirically.

## 5. Archive backfill (XRPC download path) + client

500M-seq window on jetstream-2 (~28 segments, ~7.5 GiB compressed), default
(map-mode) decode unless noted. `--download-concurrency` (dc) ladder:

| dc | wall | events/s | client CPU | client peak RSS |
|---|---|---|---|---|
| 8 | 203 s | 2.5M | 5.2 cores | 9.3 GiB |
| 16 | 134 s | 3.7M | 8.5 cores | 9.4 GiB |
| 32 (= auto clamp) | 90 s | 5.6M | 14.3 cores | 9.7–10 GiB |
| 64 | 67 s | 7.5M | 22 cores | 10.8 GiB |
| 128 | 61 s | 8.2M | 29.6 cores | 14.4 GiB |

- **The `[4,32]` auto-clamp (`options.go:55`) leaves ~1.5× on the table on big
  machines** — dc=128 is 48% faster than dc=32. Scaling flattens past 64
  (reassembler + GC become the limit), but the ceiling is well above the clamp.
- **jetstream-1, 1B-seq window: 970.8M events in 126 s = 7.7M ev/s** at 23.6
  cores / 24.5 GiB peak RSS. Sustained ~13 GiB/s of *decompressed* event
  payload into a consumer on one machine.
- **Typed fast path is 2.2×**: `--typed-likes-client` (raw records, no generic
  map) decoded 324.8M likes in 29 s (11.7M ev/s) vs 63 s generic. The alloc
  profile explains it: 508 GB allocated in a 20 s window (~25 GB/s) — 38%
  `cbor.unmarshalMap`, 16% zstd `DecodeAll` output buffers, 11% `bytes.Clone`,
  9% `toPublicEvents` slabs; 24% of client CPU is malloc/GC machinery.
- **The server side of backfill is nearly free**: serving the 500M-event
  default run moved server CPU only ~2.4 cores *including* its ~2.3-core idle
  baseline — getSegment is pure sendfile (`internal/obs/http.go:101` ReaderFrom
  delegation). Raw single-stream curl of a 299 MB segment: **5.8–6.0 GB/s**.
  getBlock traffic in these runs was modest (~1.1k requests/run, block-mode
  plan entries at range edges).
- **DID-filtered plans are spectacular**: 3 DIDs over the full js2 archive —
  planBackfill + bloom block selection + download + decode = **214.8k events
  in 0.59 s wall**. The resident block blooms earn their RAM here; the open
  question is whether they must be *resident* (see §7.2).
- Client live tail via the library: 4% CPU, 16 MiB RSS at ~370 ev/s. A
  non-issue.

## 6. Ingest under stress

Ingest never degraded during any test: lag stayed ≤ 0.2 s, `append_errors`,
`sequence_gaps`, `readable_log_pinned_overrun_bytes` all zero, and the c=1000
fan-out did not move append throughput. The writer-mutex serialization point
(`internal/ingest/writer.go:407`) is nowhere near stressed at current firehose
rates (~400–500 ev/s live). Segment rotation + seal took 1.97 s
(`seal_duration_seconds`) without visible subscriber impact. Not exercised
here: bootstrap backfill write rates (both archives were pre-built).

---

## 7. Improvement opportunities, ranked

Ordered by production impact ÷ effort. None change wire formats or on-disk
contracts.

### 7.1 P0 — Fix and instrument the saturated-subscriber drop path

`internal/subscribe/handler.go`: (a) count every disconnect cause — write
timeout, ping timeout, read error — as labeled metrics (today they're silent
`return`s; we cannot see the #1 failure mode in §2a in production); (b)
separate ping liveness from delivery saturation: skip the keepalive ping when
frames were written recently (delivered data *is* liveness), or lengthen the
pong deadline; (c) reconsider 5 s `frameWriteTimeout` versus intent — the
slow-client policy is supposed to be the arbiter of who gets dropped
(60 s window, 5 ev/s floor), but the write timeout currently fires first and
silently. A saturated-but-progressing client should be throttled by TCP
backpressure, not executed mid-frame. Definition of done: c=1000 unfiltered
single-host run holds connections for the full duration, and drop reasons are
visible in /metrics.

### 7.2 P1 — Shrink or demote resident block blooms (top RSS driver)

49–88 GiB, >90% of heap, and it scales with block count (live-written
segments are 4.5× worse per event than backfill ones). Three independent,
compatible levers:
1. **Size blooms to actual block fill** at seal time — a ~900-event block does
   not need an 8,409-byte bloom sized for 4096 (false-positive target can be
   held constant); worst case ~4× reduction on live segments.
2. **Make per-block blooms non-resident**: only the planBackfill DID path uses
   them (`manifest.go:897`); serve/replay never does. Loading them on demand
   (or mmap-ing the footer bloom region) trades the 0.6 s DID-plan latency for
   tens of GiB. Segment-level blooms (665 B–47 KiB each) stay resident to
   prune candidates first.
3. **Re-block sparse live segments during compaction rewrites** (they're
   already being rewritten) so post-compaction segments carry dense blocks —
   this also improves cold-replay decode efficiency and archive download
   density.

### 7.3 P1 — Share cold-path encodes across subscribers

Reconnect herds replay the same recent window; today each subscriber re-does
CBOR→JSON per event (§4, 60% of cold CPU). Options: key a small seq→`*Entry`
cache next to the block cache (entries already carry the memo slot), or make
`decodeSealedBlock` return shared `Entry`s per cached block rather than fresh
copies per subscriber (`replay.go:414`). A 100-subscriber herd would drop
~100× to ~1× encode cost precisely when the server is also absorbing the
reconnect storm from 7.1. Add a block-cache hit/miss metric while in there.

### 7.4 P2 — Cut hot fan-out lock traffic

`Tail.mu` is acquired twice per delivered batch per subscriber (ReadFrom +
Tip), and the readlog `notify` channel is re-made and closed per event
(`readlog.go:122`), waking every parked subscriber per single append. At
today's firehose rate this costs ~25% of fan-out CPU in lock/scheduler
machinery but is nowhere near a ceiling (420k ev/s at 13 cores). Cheap wins
when touched next: `atomic.Pointer` for the post-cutover-immutable
`readLog`/`nextSeq` fields, atomic tip counter, and (if wakeup storms ever
matter at 10× firehose) coalescing notify closes at small time granularity —
but measure against the 13 ms e2e budget first. Not urgent; file for the next
time someone is in this code.

### 7.5 P2 — Raise or remove the client download-concurrency auto-clamp ceiling

`options.go:55` caps auto-sizing at 32. On a 256-thread host explicit dc=128
is 1.48× faster. Either raise `maxAutoDownloadConc` (with the reassembler
window bound keeping memory sane — RSS grew only 9.7→14.4 GiB) or document
that operators on big iron should override. Also worth a note in the client
docs: map-mode decode allocates ~25 GB/s at full rate; `WithRawRecords` is
2.2× faster end-to-end, and library embedders don't inherit the CLI's
GOGC=400 tuning (`cmd/client/debug.go:28`) — an embedding at default GOGC=100
will spend markedly more CPU in GC.

### 7.6 P3 — Compression guidance + shared-context deflate

permessage-deflate doubles serve CPU at 100 conns and holds ~1.2 MB per
connection. Document that high-fan-out consumers should prefer the v1
`compress=zstd` shared-dictionary path (memoized per event, not per conn);
consider `CompressionNoContextTakeover` as the negotiated default to cap
per-conn memory if deflate stays.

### 7.7 P3 — Observability gaps found while measuring

- Disconnect-reason counters (7.1) — the big one.
- Block cache hit/miss/eviction counters (sizing `--subscribe-block-cache-bytes`
  is blind today).
- getSegment has no bytes-served/duration metric (getBlock does); backfill
  egress is invisible outside the generic HTTP histogram.
- Client library exposes `Stats()` but no throughput/decode-error counters; a
  caller can't see backfill progress rate without wrapping the iterator.
- The verifier's resync failures for <12 h-old repos (`MethodNotImplemented`
  upstream) log-spam at WARN with no aggregate metric; they dominated both
  serve logs during this session.

### 7.8 Non-findings (measured, fine, don't spend time)

- Ingest write path: 0.05–0.12 s behind relay, zero pinned-overrun, disk
  <0.5% util. The writer mutex is not a real-world bottleneck at current rates.
- E2E latency under load: flat p99 ≈ 21 ms at 800 subscribers.
- Server-side backfill cost: sendfile ≈ free; a single client can pull 6 GB/s.
- Server GC: max pause 17 ms on a 58–131 GiB heap; blooms are pointer-free so
  scan cost stays low.
- OTel/middleware per-request overhead: invisible in all profiles.

## 8. Test-methodology notes for next time

- One loadtest process saturates ~6.3 cores per 250 conns and tops out
  ~140k ev/s received; always fan out across processes (4×250 reached 420k+).
- `client loadtest` reconnects at the *tip* after a drop; real consumers
  resume with a cursor. A future "reconnect herd" scenario should use
  `--cursor` on reconnect to reproduce the 7.1→cold-storm coupling honestly.
- Both instances share one box and one firehose; verifier baseline (~2 cores
  each) must be subtracted from server CPU deltas — the counter snapshots in
  `/data/jcalabro/perf/*.{before,after}` already allow this.
- Everything here ran with a warm page cache (1.1 TiB cached). Cold-cache
  replay/backfill behavior (real disk reads) is unmeasured and worth one
  dedicated session — drop caches, replay 6 h, watch iostat.

## 9. State left behind

Both servers were left **running** on cpu2-pop3 (js1 pid on :8080/:6060, js2
on :8081/:6061), archives intact and ingesting. Raw measurement artifacts in
`/data/jcalabro/perf/` (scripts `livetest.sh`/`bftest.sh`/`split.sh`, counter
snapshots, loadtest logs, `profiles/*.pb.gz`); local profile copies in
`/tmp/jsperf/` on the workstation.

---

# Addendum: WAN client analysis, Boston → cpu2-pop3 (2026-07-09)

Status: **measurement report for Jim's review. No issues filed yet.**

Purpose: yesterday's session measured the client on the same box as the
server (localhost, effectively infinite bandwidth, ~0 RTT). This session
measures the same client binary from Jim's Boston workstation (32 threads,
123 GiB RAM, 10 GbE local link) against cpu2-pop3 in Seattle over the real
internet, characterizes behavior under degraded conditions, and identifies
which client design choices are LAN-invisible but WAN-dominant.

Method: identical client build (current `main`, `GOAMD64=v4`) run (a) on
cpu2-pop3 against `localhost:8081` (best case) and (b) from Boston over the
Tailscale WireGuard tunnel (direct path, not DERP-relayed; `tailscale0` MTU
1280). Degraded-network runs used a purpose-built local TCP impairment proxy
(`/tmp/wanproxy` on the workstation — rate caps, added delay, mid-stream
RST, mid-stream stalls) between the client and the server, so **no server- or
network-side configuration was touched**. All backfill runs used the same
96.04M-event window on jetstream-2 (28 segments, ~7.7 GiB compressed, seqs
341,698,006 → 437,849,218) unless noted. Live firehose rate during the
session was ~1k ev/s on js2.

## A1. Path characterization

- RTT Boston → cpu2-pop3: **69.4–71.4 ms** (avg 70.5 ms), 0/10 loss, via
  direct WireGuard (`155.204.43.106:44172`).
- Single TCP stream (curl of one 276 MB segment): **67–73 MB/s** across 6
  samples — ~560 Mbit/s. BDP at 70 ms ≈ 5 MB of window; both kernels allow
  32 MB `tcp_rmem`/`tcp_wmem` max, so the ceiling is congestion/tunnel, not
  window. Both ends run cubic.
- Aggregate path capacity: 4 parallel streams ≈ 62–98 MB/s total; 8 parallel
  ≈ **105 MB/s total** (~850 Mbit/s). So one stream captures only ~65–70% of
  what the path can carry — the rest is reachable only with parallelism.
- TTFB for a small request (planBackfill): ~0.23 s; the full-archive plan
  (3,074 entries, 399 KB JSON) completes in 0.58 s. Negotiation is not a
  WAN problem.

## A2. Best case vs. real world: the same workload, both ends

Bounded backfill dump (`--backfill-only`, default map decode) of the
96.04M-event window:

| where | dc | wall | events/s | effective wire rate |
|---|---|---|---|---|
| on cpu2-pop3 (localhost) | 32 | **12.1 s** | 7.9M | ~640 MB/s |
| Boston WAN | 8 | 129.9 s | 739k | ~59 MB/s |
| Boston WAN | 16 | 108.6 s | 884k | ~71 MB/s |
| Boston WAN | 32 | 148.6 s | 646k | ~52 MB/s |
| Boston WAN | 64 | 111.8 s | 859k | ~69 MB/s |

- **WAN is 9–12× slower than localhost, and `--download-concurrency` has no
  effect on it.** The dc=8→64 spread (646k–884k ev/s) is path variance, not
  scaling — dc=32 was the *slowest* run. This is by design: `dc` sizes the
  *decode* pool only; the network fetch path is a **single prefetch
  goroutine downloading one whole segment at a time, at most 2 ahead**
  (`internal/client/downloader.go:33,341-364`). Backfill throughput over any
  WAN is therefore exactly one TCP stream's throughput — the 71 MB/s
  best-run effective rate matches the single-stream curl ceiling to within
  noise. The measured 105 MB/s aggregate capacity means ~1.5× is being left
  on the table on *this* (clean) path; on a lossy path the gap widens
  arbitrarily, since one stream's cwnd collapse gates everything.
- Client-side CPU during WAN backfill: ~1.2–3 cores (vs 24 at localhost) —
  the decode pool is starved. A Boston consumer pays ~110 s wall for what
  the network could deliver in ~74 s and the client could decode in ~12 s.
- **Time-to-first-event: 4.1–4.4 s WAN vs 0.13 s localhost.** The breakdown
  is ~0.6 s plan + ~3.9 s for the *entire first segment*: `xrpc.QueryRaw`
  buffers the full 276 MB body in memory before the framer may slice block
  one (276 MB ÷ 71 MB/s ≈ 3.9 s). First-event latency over a WAN is
  `segment_size / single_stream_bandwidth`, which on a 10 MB/s consumer link
  is ~28 s of silence before any output.

## A3. DID-filtered (block-mode) backfill: the RTT disaster

The §5 "spectacular" DID-plan result inverts completely over a WAN, because
block-mode entries are fetched **serially, one `getBlock` GET per block,
inline on the framer goroutine** (`downloader.go:307-324`) — one block per
RTT, no pipelining, despite `MaxConnsPerHost=100` sitting idle:

| where | wall | notes |
|---|---|---|
| localhost | **0.11 s** | 217 events, 27 entries / ~200 blocks |
| Boston WAN (70 ms RTT) | **15.4 s** | ≈ 200 sequential round trips, as predicted |
| Boston WAN + 100 ms added delay | 54.3 s | scales linearly with RTT (proxy adds per-chunk delay, so slightly super-linear) |

139× slower over the real internet, ~0% bandwidth utilization — the whole
run moved a few MB. This is the single worst WAN behavior found: exactly the
"give me these 3 DIDs' history" use case that the bloom infrastructure makes
nearly free server-side is RTT-bound client-side at ~14 blocks/s.

## A4. Live tail and cutover over the WAN

- **Live tail is a non-issue**, as expected at ~1k ev/s: delivery lag from
  `time_us` to arrival in Boston measured ~40 ms (cross-machine clocks make
  the absolute number soft; the same measurement run *on* the server read
  ~56 ms, so treat both as ≈ batcher 20 ms flush + one-way delay ± clock
  skew). No reconnects, no errors over all live runs.
- **Backfill→live cutover from ~8M events behind**: 4 s silent startup
  (§A2), backfill at ~730k ev/s, clean single cutover to live at t≈15 s, no
  re-backfill stalls, steady live tail thereafter. The §14 re-backfill path
  was not triggered even at WAN speeds for this gap size. (A full-archive
  WAN sweep — days at 71 MB/s for js1's 1.6 TiB — would be a more honest
  lookback-window stress; not run.)
- **Cold websocket replay (2 h cursor) is server-paced, not WAN-paced**:
  16.1k ev/s / 9.1 MiB/s from Boston vs 15.9k ev/s / 8.9 MiB/s on
  localhost — statistically identical, and far below both the path (70 MB/s)
  and yesterday's js1 cold-replay figure (79k ev/s). js2's sparse
  live-written blocks (~919 ev/block, §1) make replay decode/pacing-bound at
  ~9 MiB/s per connection; the WAN adds nothing. Worth knowing: a Boston
  consumer catching up 2 h of js2 replay is *not* helped by a fatter pipe.
- **permessage-deflate over the WAN *reduced* replay throughput 23%**
  (12.4k ev/s vs 16.1k): at 9 MiB/s the link isn't the constraint, so
  compression only added the §3 server-side flate cost. Compression helps
  only when the consumer's link is genuinely thinner than the event stream.

## A5. Degraded-network behavior (impairment proxy)

| scenario | result |
|---|---|
| 10 MB/s rate cap, 4-segment window (1.08 GB) | 111 s ≈ ideal network-bound time; graceful, zero errors, no spurious timeouts. The jttp guards (30 s body-idle, 64 KiB/s-over-60s floor) have huge margin at consumer-broadband rates. |
| mid-stream TCP RST at 200 MB of a 276 MB segment | **Transparent recovery**: xrpc retry re-issued the GET on a fresh connection, window completed with zero surfaced errors — but the retry restarted from **byte 0**, discarding 200 MB. Invisible at 70 MB/s; on a 10 MB/s link that's 20 s of re-download per event, and it compounds with loss rate. |
| 50 s mid-stream stall (every connection, same byte offset) | 30 s body-idle timeout killed the transfer, retried ×3 (each retry re-pulled the first 150 MB then stalled), then delivered `getSegment ...: jttp: body idle timeout` as a recoverable `EntryResult.Err` after ~99 s and ended the stream with 0 events. Correct crash-loud-ish behavior (no hang, bounded time), but: the error is only visible if the consumer checks per-entry errors, and every retry paid the full prefix again. |

Also observed: a bare `--host cpu2-pop3:8081` defaults to **https** (only
loopback defaults to http, `client.go:204-240`) and fails immediately and
loudly against the TLS-less dev server (exit 1). Right default for
production, but the error lands on stderr after `final ... events=0` — an
easy thing to misread in scripts that only capture the stats line.

## A6. Improvement opportunities, ranked (WAN lens)

None change wire formats or the server's on-disk contract; the server side
already does the right things (Range/If-Range, strong per-generation ETags,
no server-side write timeout on downloads).

### A6.1 P0 — Parallelize block-mode fetches (fixes the 139× DID cliff)

`downloader.go:307-324`: fan `getBlock` requests out over the existing
connection pool with bounded concurrency (the reassembler already handles
out-of-order completion; the plan gives every block's identity up front).
16-way parallelism turns 200 RTT-serialized fetches into ~13 round trips —
~1 s instead of 15 s at 70 ms, and it degrades linearly with RTT instead of
multiplicatively with block count. This is the highest leverage change in
the client for real-world use.

**Done (#292, 2026-07-09):** block fetches now run on a shared pool
(2×download-concurrency, capped 64) with per-entry ordered futures.
Re-measured, same 3-DID scenario: 15.4 s → **1.6 s** over the WAN, and the
+100 ms-added-RTT case 54.3 s → **2.6 s** (~21× at elevated RTT). Ordering,
per-entry error, and early-stop contracts unchanged; whole-segment path
untouched (A6.2 still open).

### A6.2 P0 — Stripe whole-segment downloads across ranges/streams

The single-flight prefetcher caps every WAN backfill at one TCP stream's
bandwidth (§A2). The server already supports Range. Fetching each segment as
N concurrent range parts (e.g. 8×32 MB), or 2–3 segments concurrently,
recovers the measured ~1.5× on this clean path and much more on lossy paths
(one stream's loss event no longer stalls the pipeline). Range-parts compose
with A6.3 and A6.4: parts are natural resume/streaming units. Memory stays
bounded — today's budget is already ~2×280 MB of prefetched buffer.

**Done (#296, 2026-07-09):** implemented — 16 MB range parts, errgroup
fan-out, If-Range generation safety — **default 8 stripes**, tunable via
`WithSegmentStripes` / `--segment-stripes` (1 = single resumable stream).

Lab caveat worth recording: our only WAN vantage is the Tailscale/WireGuard
tunnel, and *on that path* striping measured 20–40% slower than the single
stream (interleaved rounds: main 38.7–41.0 s vs striped 48.1–57.0 s on a
2.5 GB window; raw-curl isolation, no client code: 1 stream 3.7 s vs 8 parts
7.0 s per segment). Plausible mechanism: WireGuard encapsulates all tunneled
TCP into one UDP flow, so parallel parts fragment a fixed ~75 MB/s tunnel
capacity, re-pay slow-start per connection, and burst-induce loss. Also
noted: multi-stream capacity probes over the tunnel varied wildly run-to-run
(39–98 MB/s aggregate) while single-stream was stable at 70–80 — one probe
is not a design premise. On a 200 Gbps LAN (cpu1→cpu2), all modes are
indistinguishable (~22 s, decode-bound).

We shipped striping as the default anyway: typical consumers pull over the
public internet, where per-TCP-flow congestion control is the expected
bottleneck and striping is the standard remedy; tunneled deployments can set
stripes=1. Public-internet validation pending (Jim); if it lands
contradicting the expectation, revisit the default and consider promoting
the tunnel observation to specs/gotchas.md.

### A6.3 P1 — Resume interrupted segment downloads with Range + If-Range

Retries restart 276 MB transfers from byte 0 (§A5). The plan already carries
the per-generation checksum (`planner.go:53-57` says it's *for* this) and
the server serves strong ETags + If-Range precisely so a resume can't splice
generations. Keep the buffered prefix, re-request `bytes=N-` with
`If-Range: "<etag>"`, fall back to full restart on 200. Turns loss-induced
resets and stall-timeouts from O(segment) re-download into O(gap).

**Done (#296, same PR):** the default stripes=1 path now resumes mid-body
with Range+If-Range from the exact failure byte (attempt budget resets on
progress, so long transfers with occasional hiccups always complete); part
retries in striped mode are O(part) for the same reason. Verified by test
(`TestFetchSegmentSingleStreamResumesMidBody` pins resume-from-exact-offset)
plus the generation-swap, no-Range-support, and no-ETag fallback paths.

### A6.4 P1 — Stream the segment body into the framer

Full-body buffering costs 4 s of time-to-first-event at 70 MB/s and ~28 s at
consumer broadband (§A2), plus two 280 MB resident buffers. Block frames are
self-delimiting; the framer could consume from the response body as bytes
arrive (or, simpler, consume A6.2's range parts as they land) and emit the
first events after the first few MB. Also shrinks the client's baseline
memory for embedders.

### A6.5 P2 — Document/tune for constrained consumers

- Compression guidance inverts by link: permessage-deflate costs 23%
  throughput on a fat link (§A4) and only pays when the link is thinner than
  the stream. Worth one line in client docs.
- `--download-concurrency` currently does nothing for WAN wall-clock (§A2);
  once A6.1/A6.2 land it should size network parallelism too, and until then
  its help text ("bounded concurrency for sealed segment/block downloads")
  overpromises.
- Server-side, cubic's loss recovery on high-BDP paths is the eventual
  ceiling for single-stream consumers; if operators ever chase last-mile
  throughput, a BBR experiment on the serving host is the standard lever —
  ops-level, no code change, unverified here.

### A6.6 Non-findings (measured, fine)

- planBackfill negotiation over WAN: 0.6 s for the full 3,074-entry plan;
  one round trip per 100k-entry page. Irrelevant.
- Live tail over WAN: lag ≈ one-way delay + 20 ms batcher; zero reconnects.
- Rate-capped (10 MB/s) backfill: exactly network-bound, zero overhead, no
  guard-rail misfires. The jttp bulk-transfer guards are well tuned.
- Client resilience to a clean mid-transfer RST: transparent, zero surfaced
  errors (modulo the re-download cost, A6.3).
- Kernel window sizing on both ends is already adequate for this BDP; no
  sysctl changes needed for the measured path.

## A7. Methodology notes

- The impairment proxy lives at `/tmp/wanproxy/` on the workstation
  (throwaway; rebuild from this note's description if needed: TCP forwarder
  with `-rate-mbps`, `-delay`, `-reset-after-bytes [-reset-once]`,
  `-stall-after-bytes -stall-for`). Local-only — nothing on cpu2-pop3 was
  modified. `tc netem` was not used (needs root on the workstation).
- Cross-machine latency numbers via `time_us` carry unknown clock skew
  (tens of ms); same-machine measurements or an NTP-audited pair are needed
  for defensible absolute lag numbers.
- The Tailscale tunnel (MTU 1280, WireGuard encap) is itself part of the
  measured path; a raw-internet consumer would see slightly different
  per-stream ceilings, but every *architectural* finding (serial block
  fetches, single-stream segment fetch, no resume, full-body buffering) is
  path-independent.
- js2's cold-replay pacing (~16k ev/s/conn vs js1's 79k) deserves its own
  look — likely the sparse-block effect from §1/§7.2, not networking.

## A8. State left behind

Both servers still **running** and healthy on cpu2-pop3 (js1 :8080/:6060,
js2 :8081/:6061), archives intact, ingest current. No configuration,
binaries, or data on the server were changed this session; the only files
created there were transient client-process outputs already present from
yesterday. Workstation-side: impairment proxy source/binary in
`/tmp/wanproxy/`, plan snapshots in `/tmp/jsplan.json`/`/tmp/didplan.json`.
All impairment proxies were killed; no stray client processes remain on
either machine.
