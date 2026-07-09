# /subscribe wire compression: CPU, ratio, and fanout analysis (2026-07-09)

Measured on cpu2-pop3 (256 cores, 1.5 TiB RAM, Linux 6.8) against the
full-network instance (jetstream-1, :8080/:6060, steady state, live firehose
~320–350 events/s appended, mean live event ~581 B JSON). Server and client
binaries built at `GOAMD64=v4` from main (build bc87143c-era). Raw run
artifacts live on cpu2-pop3 under `/data/jcalabro/comptest/` (pidstat, metric
snapshots, client logs, CPU/heap pprof profiles).

## 1. The three wire schemes (code refresher)

- **none** — raw JSON text frames.
- **permessage-deflate** (RFC 7692, PREFERRED per `internal/subscribe/doc.go`)
  — negotiated automatically when the client offers it;
  `websocket.CompressionContextTakeover`, i.e. stdlib
  `flate.NewWriter(w, flate.BestSpeed)` (L1) with a 32 KB sliding window
  **per connection** (`coder/websocket` `compress.go`). Compression runs
  per-connection per-message inside `Conn.Write`; nothing is shared.
- **custom zstd dictionary** (v1 compat, `?compress=true` /
  `Socket-Encoding: zstd`) — one process-wide `zstd.Encoder`
  (klauspost, `WithEncoderDict` on the 112,640 B v1 dictionary, dict ID
  1612007021, window 128 KiB, `WithEncoderConcurrency(1)`, default level
  ≈ SpeedDefault/3) in `internal/subscribe/compress.go:31`. The output is
  memoized per event on the shared `Entry`
  (`internal/subscribe/entry.go:94` `Compressed()` via `sync.Once`), so on
  the **hot path the frame is compressed once and fanned out to every
  caught-up zstd subscriber**. On the **cold path** (`ColdReader.Read`,
  `internal/subscribe/replay.go:415`) Entries are created fresh per batch per
  subscriber, so cold zstd subscribers each re-compress every event.

## 2. Corpus ratio + single-thread CPU (offline, 20k live events)

Corpus: 20,000 consecutive live events captured uncompressed from
jetstream-2 (11.6 MB; mean 581 B, p50 523 B, p99 1,425 B, max 12.3 KB;
99.8% commits). Bench on the workstation, single-threaded, klauspost
v1.18.0 / stdlib flate, 3 runs, <2% variance:

| scheme | ratio | compress µs/msg | decompress µs/msg |
|---|---|---|---|
| zstd-dict, prod config (level default/3) | 2.00x | 26.4 | 2.1 |
| zstd-dict, SpeedFastest (level 1) | 1.98x | 8.8 | 2.2 |
| zstd-dict, SpeedBetterCompression | 2.04x | 15–16 | ~2 |
| zstd, no dict | 1.50x | 5.0 | 2.3 |
| deflate context-takeover L1 (prod config) | 2.15x | 9.7 | ~5 |
| deflate context-takeover L6 | 2.90x | 10.3 | ~5 |
| deflate no-context-takeover L1 (per-msg reset) | 1.54x | 9.9 | — |

Observations:

- **The dictionary is worth ~0.5x ratio** (2.00 vs 1.50 without) on these
  small messages — it is doing its job as a priming context.
- **Per-connection deflate with context takeover beats zstd-dict on ratio**
  (2.15x vs 2.00x at prod configs): the 32 KB sliding window over the
  *actual recent stream* is a better "dictionary" than the static v1 one.
  At L6 the gap widens to 2.90x for ~6% more CPU than L1 — the level knob is
  ratio-rich and CPU-cheap on this corpus because tokenization, not Huffman
  depth, dominates at these message sizes.
- **The prod zstd config wastes ~3x CPU per message vs SpeedFastest for a
  1% ratio win** (26.4 → 8.8 µs/msg, 2.00x → 1.98x). Profiling shows 71% of
  the prod-config encode is `doubleFastEncoderDict.Reset` + 22%
  `fastEncoderDict.Reset` — i.e. **~93% of the per-message cost is
  re-priming dictionary tables for a ~581 B input**, not compressing.
  klauspost re-initializes the dict-primed match tables on every
  `EncodeAll` when a dictionary is loaded; the cost is proportional to
  table size, not input size, so tiny messages pay a huge fixed tax.
  (`SpeedFastest` uses a smaller table — 9 µs — but the tax structure is
  the same.)

## 3. Live fanout on the server (loadtest matrix)

`client loadtest` against :8080, 80 s runs, 60 s measurement window after a
15 s warmup/ramp, `pidstat 5` averages, wire bytes from `ss -tin
bytes_acked` deltas on :8080 (kernel-level, includes ws framing + TCP
overhead), event rates from prometheus counters. Baseline server CPU with
zero subscribers: **~17.4%** of one core (live ingest + verifier).
Two replicates at c=50/200 agreed within ~5%; single runs elsewhere.

| conc | mode | server CPU % | client CPU % | frames/s | wire B/event |
|---|---|---|---|---|---|
| 1 | none | 19.7 | 2.0 | 311 | 600 |
| 1 | deflate | 24.3 | 2.4 | 307 | 276 |
| 1 | zstd | 25.6 | 1.7 | 318 | 296 |
| 10 | none | 32.7 | 22.6 | 3,310 | 707¹ |
| 10 | deflate | 46.8 | 23.6 | 3,302 | 270 |
| 10 | zstd | 44.2 | 13.0 | 3,181 | 291 |
| 50 | none | 69.8 / 70.8² | 159 / 154 | ~17,000 | 582 |
| 50 | deflate | 128.5 / 141.2 | 149 / 167 | ~17,300 | 281 |
| 50 | zstd | 69.0 / 69.4 | 76.8 / 76.8 | ~17,300 | 296 |
| 200 | none | 219.7 / 221.9 | 569 / 593 | ~67,500 | 588 |
| 200 | deflate | 436.5 / 449.7 | 452 / 488 | ~68,500 | 277 |
| 200 | zstd | 200.0 / 204.0 | 349 / 362 | ~68,800 | 293 |
| 500 | none | 334.5³ | 968 | 138,869³ | (164)³ |
| 500 | deflate | 1,158.8 | 965 | 174,233 | 278 |
| 500 | zstd | 614.9 | 807.9 | 180,933 | 295 |

¹ c=10 none-run wire-bytes reading is noisy (ss snapshot caught socket
churn); the c≥50 runs converge on ~582–592 B/event raw.
² x / y = replicate 1 / replicate 2.
³ The none-c500 run undersent (139k vs ~175k frames/s expected): with 500
uncompressed subscribers the box hit its **network/write-path** ceiling
during ramp (kernel TX ~100 MB/s sustained here), so subscribers lagged;
the 164 B/event wire figure is an artifact of frames counted vs bytes acked
lagging. CPU for that run is correspondingly understated. Compressed modes
push less than half the bytes and did not saturate.

Wire ratio measured end-to-end (raw ~585 B/event): deflate ≈ **2.1x**
including framing, zstd-dict ≈ **2.0x** — both match the corpus bench.

### Server CPU scaling (subtract ~17.4% ingest baseline)

Marginal server CPU per 1,000 frames/s delivered, from the c=200 runs
(both replicates):

- none: ~3.0% CPU / kfps
- zstd-dict: ~2.7% CPU / kfps — **cheaper than uncompressed**: the
  once-per-event compression is amortized across 200 subscribers, and the
  halved payload reduces syscall/memcpy bytes.
- deflate: ~6.3% CPU / kfps — **2.1x uncompressed, 2.3x zstd**, growing
  linearly with subscriber count as expected for per-connection compression.

At c=500, deflate cost ~1,159% CPU (≈11.6 cores) vs zstd's ~615% (≈6.2
cores) delivering slightly *more* frames. The zstd hot-path fanout thesis
holds: **one shared compression + cheap binary writes scales with
subscribers strictly better than per-connection deflate**, and past ~50
subscribers it is even cheaper than sending raw (fewer bytes through the
kernel).

pprof confirms attribution at c=200 (30 s samples):

- deflate: 74% of samples under `Conn.Write` → 50% in
  `flate.(*compressor).syncFlush` (Huffman `bitCounts` + `deflateFast.encode`
  dominate; `sort.insertionSort` from Huffman code building is 5%).
- zstd: `compressFrame` is **1.8%** of samples (fanout amortization working);
  syscalls dominate (38%).
- none: syscalls 35%, scheduler/lock overhead next — write-path bound.

### Client CPU

- zstd clients are consistently the cheapest (c=200: ~355% vs ~470%
  deflate vs ~580% none) — but note the loadtest client does **not**
  decode zstd frames (binary frames are counted, not decompressed), so real
  zstd clients pay +~2 µs/msg (corpus bench) ≈ +0.2 core per 100k ev/s.
  Even adding that, zstd stays cheapest: fewer bytes read dominates.
- Deflate clients pay ~5 µs/msg inflate; raw clients pay the most CPU here
  simply because they read 2x the bytes at the same frame rate.

### Memory

Server RSS during c=200 runs: none 70.4 GB → deflate 72.7 GB (+2.3 GB ≈
the documented ~1.2 MB flate.Writer + window per connection, plus buffers)
→ zstd 73.2 GB. Heap profiles show flate writer allocations are pooled and
modest (~50 MB inuse at c=200); `Conn.Read` buffers (~2.5 GB across modes)
dominate either way. Memory is not the deciding axis at these scales.

## 4. Cold replay (cursor 2M seqs below the readable-log floor, c=10)

| mode | server CPU % | client CPU % | frames/s delivered |
|---|---|---|---|
| none | 176 | 599 | 147,195 |
| deflate | 504 | 798 | 183,410 |
| zstd | **162** | 51 | **13,123** |

**Cold zstd throughput collapses: 13k frames/s vs 147–183k for the others —
an ~11x replay slowdown.** Profile: 24.7% `doubleFastEncoderDict.Reset` +
6.5% `fastEncoderDict.Reset` on the server. Two compounding causes, both
visible in code:

1. Cold Entries are per-subscriber (`replay.go:415` `newEntry(&cp)`), so
   the compress-once memoization never engages — every subscriber
   re-compresses every archived event.
2. Every one of those compressions pays the ~26 µs dictionary-table Reset
   tax (§2), and worse: the process-wide encoder is built with
   `WithEncoderConcurrency(1)`, so **all subscribers serialize through one
   encoder state**. A local test confirms the ceiling: 10 goroutines sharing
   the prod-config encoder max out at ~37k msgs/s regardless of cores
   (and raising `WithEncoderConcurrency` makes it *slower* — 14k msgs/s at
   32 — because each of the pooled encoder states re-primes its own dict
   tables and evicts cache).

So a v1-compat zstd client that reconnects with an old cursor replays the
archive ~11x slower than a deflate client, and ties up ~1.5 cores of server
CPU doing dictionary resets. Ten such clients (e.g. a fleet restart after a
deploy) would saturate the shared encoder and crawl. The adversarial-slow
detector did not fire (progress is steady, just slow).

Also noteworthy: cold deflate replay hit 504% server CPU at just 10
subscribers (183k frames/s × ~10 µs deflate each ≈ 5 cores, exactly as the
corpus bench predicts) and 4 of the 10 loadtest clients hit read errors /
reconnects during the run — replay at disk speed is where per-connection
deflate hurts most.

## 5. Summary of findings

1. **Hot-path fanout via the shared zstd Entry memo works as designed and
   is the cheapest way to serve many live subscribers** — beyond ~50
   subscribers it costs less server CPU than even uncompressed, and 2.3x
   less than deflate at c=200–500. The design goal (compress once, fan out)
   is validated at the syscall level: zstd's profile is all writes, no
   compression.
2. **Per-connection permessage-deflate is the best ratio** (2.15x at L1,
   2.90x available at L6) **but the worst server CPU**, scaling linearly
   with subscribers (~6.3% CPU per 1k frames/s). Fine at tens of
   connections; dominant cost at hundreds.
3. **The zstd prod config burns ~3x the needed CPU per compression**:
   ~93% of each ~26 µs `EncodeAll` is dictionary-table `Reset`, a fixed tax
   independent of the 581 B payload. `SpeedFastest` gets 1.98x ratio at
   8.8 µs. On the hot path amortization hides this; on the cold path it is
   the bottleneck.
4. **Cold-path zstd replay is ~11x slower than deflate/none** (13k vs
   147–183k frames/s at c=10) because cold Entries aren't shared across
   subscribers AND the single shared encoder serializes all of them.
5. **No observability exists for any of this**: no metrics for negotiated
   compression scheme per connection, compressed bytes out, or compression
   CPU. The `?compress=true` population vs deflate population in production
   is currently unknowable.

## 6. Improvement opportunities (not yet filed as issues)

Ranked by measured impact:

1. **Cold-path zstd: share Entries or at least stop re-priming.**
   Options, cheapest first:
   (a) switch the subscribe encoder to `zstd.SpeedFastest` (3x per-msg CPU
   reduction, −1% ratio; frames remain standard zstd — v1 clients decode by
   dict ID, not level, so this is wire-compatible but should be verified
   against a real v1 consumer);
   (b) memoize cold Entries in the block cache keyed by (segment, block) so
   concurrent cold zstd subscribers share one compression like hot ones do;
   (c) give the cold path a per-call encoder pool instead of the global
   `WithEncoderConcurrency(1)` singleton so replays don't serialize.
   (a)+(b) together should close most of the 11x gap.
2. **Consider `flate` level for deflate, or gate it.** L1→L6 is +35% ratio
   for ~6% single-thread CPU on this corpus — but deflate CPU is already the
   dominant fanout cost, so the better lever is population steering: the
   bundled Go client always offers deflate (`internal/client/live.go:283`,
   not flag-controllable) — at fleet scale that's the expensive path by
   default. Exposing the zstd scheme (or a "no compression" choice) in the
   thick client, and documenting the CPU trade in docs/README.md, would let
   heavy consumers opt into the cheap path. (The zstd scheme is currently
   labeled "NOT PREFERRED, v1 compat only" — these measurements argue for
   rehabilitating it, or for a v3 scheme per item 4.)
3. **Add compression observability**: per-connection negotiated scheme label
   on `jetstream_subscribe_subscribers` (or a counter by scheme), a
   compressed-bytes-sent counter (payload-level, next to
   `events_sent_total`), and ideally a histogram of compress ns on the cold
   path. Without these we can't see the production population mix or catch a
   cold-replay-storm regression.
4. **Longer term: shared-window compression without the dictionary tax.**
   The measured data says the ideal scheme is "compress once with a real
   context, fan out": today that exists only via the static v1 dictionary,
   which costs ratio (2.00x vs deflate's 2.15x) and carries the Reset tax.
   A `WithSingleSegment`/no-dict zstd stream won't work per-message (1.50x),
   but batching N events per frame (the client already consumes batches)
   would let no-dict zstd hit block-level ratios (the archive gets ~4–6x on
   4096-event blocks) while keeping compress-once semantics. That's a
   protocol change (v3 / opt-in subprotocol) and needs client buy-in;
   the corpus + harness under `/data/jcalabro/comptest/` and
   `/tmp/compcorpus/` make the evaluation reproducible.
5. **Loadtest client should optionally decode zstd** (`cmd/client/loadtest.go`
   counts binary frames without decompressing), so client-side CPU
   comparisons include decode cost, and events/eps aren't reported as 0 for
   zstd runs.

## 7. Method notes / caveats

- Live-fanout numbers come from a real firehose (~320–350 ev/s appended),
  so all subscribers were caught-up hot readers; frames/s scales with
  subscriber count, not ingest.
- pidstat `%CPU` is per-process, normalized to one core (100% = 1 core).
- Wire bytes/event uses `ss` `bytes_acked` deltas across all :8080 sockets
  during the window — it includes websocket framing and retransmits, and is
  noisy below c=50; corpus ratios are the precise payload-level numbers.
- Client CPU includes websocket read + (for deflate) inflate, but NOT zstd
  decode (see §5.5). Corpus decompress cost: zstd ~2 µs/msg, deflate ~5.
- The none-c500 run saturated the write path; treat its CPU as a floor.
- Server was concurrently running its normal verifier workload (~17% CPU
  baseline, subtracted where stated) and jetstream-2 shared the box
  (~5–15% CPU); both steady across runs, replicates agreed within ~5%.
