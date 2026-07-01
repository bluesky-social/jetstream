# /subscribe-v2 wire contract redesign

Date: 2026-07-01
Status: PARKED — decisions §2.2–§2.4, §2.6–§2.7 approved; commit-scoped
framing (§2.1) approved in principle but its storage prerequisites (§2.5)
are BLOCKED on the in-flight timestamps refactor (separate branch), which
touches the same on-disk format we would version-bump. Do not start
implementation until that refactor lands; then resolve the open questions
in §2.5/§3 and proceed.
An initial wire-policy refactor (§4 step 1) was implemented, fully tested
green, and deliberately reverted to keep main quiet while parked — it is
mechanical to redo.
Labels: `subscribe`, `wire-format`, `v2`
Related: #195 (extended-mode fold-in), docs/README.md §5.1–5.2,
2026-06-18-go-client-design.md, 2026-05-27-cursor-replay-design.md

## 1. Context and goal

`/subscribe` (v1) is frozen: byte-for-byte backwards compatible with
jetstream v1. `/subscribe-v2` has shipped to no one — we can still move it
to a global maximum. After #195 folded the extended wire into v2
unconditionally, the v2 contract accreted rather than being designed:

- every commit carries the record **twice** (decoded JSON + base64
  DAG-CBOR), roughly doubling the hottest bytes on the wire;
- the upstream firehose commit's op grouping is discarded — each op is
  exploded into its own websocket message, destroying the commit's
  atomicity boundary;
- the sequence number is duplicated as both `cursor` and `seq`;
- v2 still accepts v1 timestamp cursors via the 1e15
  magnitude-disambiguation trick;
- terminal conditions are signaled by a pre-upgrade HTTP 400 whose body is
  prose (the Go client string-parses the floor seq out of it), and a slow
  consumer is dropped silently;
- the handler expresses all of this as scattered `if deps.V2` branches.

The north star is the real atproto firehose (`com.atproto.sync.subscribeRepos`
+ the event-stream spec): commit-scoped messages, strict monotonic `seq`,
explicit error frames, strict cursor validation. We adopt its *semantics*
where they are better, without adopting its DAG-CBOR framing — jetstream's
identity is the friendly JSON tail.

Everything below applies to `/subscribe-v2` only. `/subscribe` is untouched.

## 2. Approved decisions

### 2.1 Frame = one firehose commit, carrying its ops

One websocket text frame carries **one upstream firehose event**. For
commits, the frame carries the commit's ops as an array — the same
grouping `subscribeRepos` `#commit` has. This is NOT transport batching:
we never pack unrelated commits (or unrelated events of any kind) into one
frame, and we never hold tip events for a coalescing window. v2 stays a
low-latency tail; frames flow as events arrive.

```json
{
  "did": "did:plc:…",
  "time_us": 1779719009132431,
  "seq": 123458,
  "kind": "commit",
  "commit": {
    "rev": "3mmoojp7vgo2g",
    "ops": [
      {"seq": 123457, "operation": "create", "collection": "app.bsky.feed.like",
       "rkey": "3mmoojp7etg2g", "cid": "bafyrei…", "record": {…}},
      {"seq": 123458, "operation": "delete", "collection": "app.bsky.feed.post",
       "rkey": "3mmoogxqxyz2a"}
    ]
  }
}
```

Rationale and consequences:

- **Atomicity restored.** v1 (and current v2) explode multi-op commits into
  independent messages; a consumer cannot apply a commit transactionally.
  With op grouping, it can. Most commits are single-op, so the common frame
  is nearly identical in size to today's.
- **Seq stays per-op.** The entire archive/cursor/replay stack allocates
  one seq per op row. Changing seq allocation to per-commit would be deep
  surgery (manifest, replay, backfill planning) for no consumer benefit.
  Instead:
  - each op carries its own `seq`;
  - the envelope `seq` is the **last op's seq** — this is the resumption
    token clients persist;
  - grouping happens at encode time from `(did, rev)` adjacency plus a
    persisted last-op-in-commit marker — see §2.5: the investigation showed
    a pure adjacency heuristic is not sound at the live tip, so the
    grouping is persisted in the segment format (a format version bump,
    currently blocked). Tail, replay, and the rotation-seam machinery are
    otherwise unchanged.
- **Resume semantics.** `?cursor=N` delivers frames whose envelope seq
  > N. Gaps are impossible: the envelope seq is the highest seq in the
  frame, so no op above the cursor is ever skipped. Behavior for a cursor
  pointing *inside* a commit group is open question Q1 in §2.5
  (remainder-only frame vs whole-commit re-send; leaning remainder-only).
- **Filtering is per-op.** `wantedCollections` may drop some ops of a
  commit; the frame carries the surviving ops. If every op is filtered,
  no frame is sent. `wantedDids` applies to the whole frame (a commit has
  one DID). DID-level events (`identity`/`account`/`sync`) continue to
  bypass the collection filter (docs §4.x rationale unchanged).
- **Shared encode memoization is preserved — and improved.** A commit
  frame is a semantic unit identical for every unfiltered subscriber, so
  the hot-ring `Entry` memoization (encode once, fan out to all caught-up
  connections) keeps working at frame granularity. Per-connection work is
  only needed when a filter drops a strict subset of a multi-op commit —
  rare, and bounded by op count.
- `identity` / `account` / `sync` frames are unchanged in shape (minus the
  dropped `cursor` field, §2.2). v2 continues to emit `#sync` and Sync 1.1
  resync replacement rows.
- **Size caps.** `maxMessageSizeBytes` applies to the encoded frame. A
  frame exceeding the cap is skipped as a unit (today's per-event skip
  semantics, at commit granularity), counted by the existing oversize
  metric. The upstream firehose bounds a commit at 2 MB / 200 ops, so
  frames are naturally bounded.

### 2.2 Per-frame shape: seq-only, payload-selectable record

- **`cursor` wire field is dropped.** `seq` is the one resumption token.
  `time_us` remains as the informational indexed-at timestamp (it is data,
  not a cursor).
- **`?payload=json|cbor`, default `json`.** Connect-time choice of
  record representation for commit ops:
  - `json`: decoded `record` object (today's default consumer experience);
  - `cbor`: `record_cbor` (base64 DAG-CBOR) only — byte-faithful and
    cheapest on the wire; the Go client uses this and decodes locally.
  `cid` is always present on non-delete ops regardless of payload choice.
  This ends the pay-2.3x-for-everyone default that docs §5.2 flagged as a
  rate-limiting concern. There is deliberately no `both` mode (see §6).
- `Entry` memoizes per payload variant (v1, v2-json, v2-cbor); variants
  are lazily built, so the off-budget overhang stays O(ring length)
  exactly as today (entry.go approxBytes rationale).

### 2.3 Cursor semantics: strict, seq-only

- `?cursor=<seq>` is the only accepted form on v2. A value at or above the
  1e15 timestamp threshold is **rejected** (`InvalidCursor`) — timestamp
  resumption is a v1-only affordance, and the magnitude-disambiguation
  trick exits the v2 contract entirely (it remains in v1's).
- `cursor=0` replays from the oldest available seq (unchanged; matches the
  event-stream spec).
- Below the lookback floor → **reject** with `CursorTooOld` carrying
  `floor_seq` (unchanged policy, new signaling per §2.4).
- **Above the writer's next seq → reject with `FutureCursor`** (new; today
  v2 silently serves live). A future cursor means the client's persisted
  state and this server's seq space disagree — e.g. a rebuilt instance with
  a fresh seq space. Serving live would silently paper over an unbounded
  gap. Crash > corruption.

### 2.4 Error signaling: in-band error frame + close code

For conditions where a websocket session can be established, v2 accepts the
upgrade, sends one error frame, then closes with an application close code.
The error frame rides the same `kind` discriminator as every other frame:

```json
{"kind": "error", "error": {"name": "CursorTooOld", "message": "…", "floor_seq": 123}}
```

This is browser-visible (browsers cannot read pre-upgrade HTTP bodies),
machine-parseable (kills the Go client's string-parsing of the floor seq),
and mirrors the event-stream spec's error-frame pattern.

Error registry (name → close code → extra fields):

| name              | close  | fields      | when |
|-------------------|--------|-------------|------|
| `InvalidRequest`  | 4000   | —           | bad query params, malformed options_update |
| `InvalidCursor`   | 4000   | —           | non-integer cursor, timestamp-magnitude cursor |
| `CursorTooOld`    | 4001   | `floor_seq` | seq cursor below lookback floor |
| `FutureCursor`    | 4002   | `next_seq`  | seq cursor above writer NextSeq |
| `ConsumerTooSlow` | 4003   | —           | adversarial slow-drop (today: silent) |

Retryable not-ready states (bootstrap in progress, cursor replay warming
up, manifest warming up, cursor resolve 5xx) **stay pre-upgrade HTTP 503**:
they are infrastructure-level, load balancers and retry logic want the
status code, and there is no session worth establishing.

### 2.5 Storage prerequisites for commit-scoped framing (segment format v2)

Investigation (2026-07-01) established that today's ingest does NOT preserve
commit grouping reliably, so §2.1's reassembly rule cannot be a pure
adjacency heuristic:

- The live consumer explodes a `#commit` into per-op `segment.Event` rows
  and appends them **one `Writer.Append` at a time**
  (`internal/ingest/live/consumer.go:457-487`), so ops of one commit are
  usually — but not provably — seq-contiguous.
- Per-op drops break contiguity: missing record blocks in the upstream CAR
  (`internal/ingest/live/events.go:171-183`) and oversized-field validation
  (`consumer.go:459-472`) drop a strict subset of a commit's ops.
- All ops of one commit share `Rev` (`events.go:156`), and distinct commits
  from one DID have distinct revs — `(did, rev)` is a valid group key; the
  problem is only knowing where a group *ends* at the tip (is the next op
  still in flight?).
- Commits can split across block/segment boundaries (flush at
  MaxEventsPerBlock, rotation at MaxSegmentBytes) — fine for reassembly
  (the cold walk is seq-ordered), but the hot ring can evict a prefix of a
  commit.
- The live consumer currently **discards** the upstream commit's `since`
  and `prevData` fields (`comatproto.SyncSubscribeRepos_Commit` carries
  both; `convertCommit` reads only Rev/Repo/Blocks).

Decision direction (Jim, 2026-07-01): persist the grouping rather than
heuristically infer it, and take the segment-format version bump while
nothing has shipped. The format change also finally carries `since` and
`prevData`, which we have independently wanted on disk. Sketch:

1. `segment.Event` gains `Since` (string TID, ≤ uint8 len) and `PrevData`
   (CID string, ≤ uint8 len); two new var-length columns in the block
   format; header version 1 → 2 (old readers reject; pre-launch archives
   are re-ingested — there is no compat machinery and pre-launch we don't
   need any).
2. An explicit op-grouping marker so a frame closes deterministically even
   at the live tip. Leading candidate: a last-op-in-commit flag packed into
   the kind byte's unused bits (kind uses 3 of 8 bits). `(did, rev)`
   adjacency + the flag gives exact frame closure with zero heuristics.
3. The live consumer switches to `AppendBatch` per commit so one commit's
   rows are appended under one writer-lock acquisition (contiguous seqs,
   modulo the documented per-op drops, which the flag makes harmless: the
   last surviving op still closes the group).
4. `#commit` frames on the v2 wire then carry `rev`, `since`, `prevData` —
   real firehose fields enabling MST-inversion-style verification
   downstream.

**BLOCKED**: a large timestamps refactor is in flight on another branch and
touches the same on-disk format. Sequencing decision: land that first, then
fold the format-v2 changes above into (or immediately after) it, so the
format version bumps once, not twice.

Open questions to resolve when unparked:

- **Q1 — mid-commit cursor resume**: a `?cursor=` pointing inside a commit
  group: does the first frame carry only the remainder ops (simple,
  streaming-friendly, documented as "first frame after resume may be a
  partial commit"), or does the server back up to the group start so every
  frame is always a whole commit (stronger invariant; requires a backwards
  walk at exactly the rotation seam #190 just simplified)? Leaning:
  remainder-only.
- **Q2 — backfill/resync snapshot rows**: `getRepo` CARs are single-commit
  snapshots — every row legitimately shares the HEAD `(did, rev)` with
  empty `since`/`prevData`, and thousands of rows would form one "commit".
  Do snapshot rows emit one row per frame (semantically honest: they are
  not firehose commits; ingest sets the last-op flag on every snapshot
  row), or group capped at N ops per frame for replay efficiency? Leaning:
  one row per frame; revisit if replay frame overhead measures badly.
- **Q3 — op-count cap**: upstream firehose caps a commit at 200 ops / 2 MB;
  decide whether v2 re-frames pathological groups above some cap or trusts
  the upstream bound end-to-end.
- **Q4 — flag vs column**: confirm the kind-byte bit-pack against a
  separate uint8 column after the timestamps refactor settles the format;
  whichever, it must survive `segment/rewrite.go` compaction untouched.

### 2.6 Client-sourced messages

`options_update` (and `requireHello`) survive unchanged — dynamic filter
swap is a jetstream differentiator worth keeping over the firehose's
one-way purity. Malformed client messages now close with the structured
`InvalidRequest` error frame instead of a bare close frame.

### 2.7 Evolution rules

Written into docs §5.2 as client guidance:

- Unknown frame fields and unknown op fields MUST be ignored.
- Unknown `kind` values MUST be skipped, not treated as errors.
- Unknown `error.name` values MUST be treated as terminal for the session.

This is what lets us add frame metadata, event kinds, or error types
without a v3.

## 3. OPEN: compression surface for v2

Not yet decided. The question is whether v2 carries forward the v1
custom-zstd-dictionary opt-in (`compress=true` / `Socket-Encoding: zstd`),
replaces it, or ships negotiated RFC 7692 permessage-deflate only.

Measured data, 2026-07-01: 2,000-event corpus synthesized from
`internal/subscribe/testdata/golden_v1.jsonl` (structure and low-entropy
fields preserved; DIDs/CIDs/rkeys/revs/timestamps/text randomized;
478 B/event mean). The throwaway harness (`zz_compress_experiment_test.go`)
was removed when this note was parked; the methodology above is enough to
rebuild it, and step 2 below replaces the synthetic corpus with real
captured frames anyway. Encoders: klauspost/compress zstd (default level,
128 KiB window) and flate level 6, matching production config:

| scheme | ratio | CPU/event | shared across conns? |
|---|---|---|---|
| per-event zstd + v1 dict (status quo opt-in) | 0.542 | 25.6 µs | yes |
| per-event zstd, no dict | 0.690 | 4.2 µs | yes |
| per-event deflate, no context | 0.653 | 8.0 µs | yes |
| 25-event batch, deflate no-context | 0.266 | 1.9 µs | yes |
| 100-event batch, zstd no-dict | 0.193 | 1.0 µs | yes |
| per-event deflate, context takeover (current default) | 0.255 | 5.8 µs | **no** |
| per-event zstd streaming, context | 0.251 | 4.5 µs | **no** |

Facts established so far:

- **The aggregate CPU math strongly favors compress-once at fanout
  scale.** An earlier draft of this note framed per-connection deflate as
  "affordable" from the per-connection number alone; that framing was
  wrong. Shared dict-zstd costs ~25.6 µs per event ONCE regardless of
  connection count; per-connection context-takeover deflate costs
  ~5.8 µs per event PER CONNECTION. The crossover is ~5 connections.
  Legacy jetstream serves thousands of concurrent subscribers: at 2,000
  connections and ~1,500 events/s, per-connection deflate is ~17 cores of
  compression alone, versus a fraction of one core for any shared scheme —
  a ~200x aggregate difference. Compress-once fanout is a scaling
  requirement, not a nice-to-have.
- The v1 dictionary is nonetheless dominated on ratio by negotiated
  deflate (0.542 vs 0.255) at ~4x the shared CPU — evidence that the v1
  dictionary is a poor fit even for the v1 shape as measured, and that a
  retrained dictionary has real headroom to claw back.
- RFC 7692 permessage-deflate has **no preset-dictionary mechanism**, so
  standards-based deflate can never reach dictionary ratios on small
  frames. `server_no_context_takeover` (which clients MUST support, and
  the server may answer unilaterally) does make compressed frames
  connection-independent — compress-once with standard clients and no wire
  change — but per-frame no-context compression of small frames measured a
  poor ratio (0.653), and coder/websocket exposes no pre-built-frame write
  path. Note the favorable batch rows above were measured on *transport*
  batches, which no longer exist under the commit-scoped frame decision
  (§2.1); typical live frames stay small (single-op).
- Therefore the leading candidate is a **new custom-dictionary zstd
  scheme, retrained on the v2 wire shape** — the v1 mechanism (shared
  encoder, memoized compressed frame per Entry, binary frames, opt-in
  negotiation) with a new dictionary and the new frame shapes. Open
  questions the corpus must answer: achievable ratio per payload variant;
  per-event shared CPU across zstd speed levels (the v1 config measured
  25.6 µs/event — likely reducible with a faster level at modest ratio
  cost); whether `payload=json` and `payload=cbor` need separate
  dictionaries (prior: yes — base64-dominated frames have very different
  byte statistics from JSON text; a mixed dictionary likely dilutes both;
  train per-variant plus a mixed control and compare).

Decision path (in order):

1. Implement the new v2 wire shape (§2) with compression admission left
   as: negotiated permessage-deflate only, dict-zstd opt-ins rejected.
   Everything in §2 is independent of the compression outcome.
2. Capture a real production corpus from the new handler: run
   `just run-prod serve --max-backfill-repos=0`, connect to the new
   `/subscribe-v2` with `payload=json` and `payload=cbor`, and record
   raw frames (target: enough traffic to cover the collection mix,
   ~100k+ frames per variant).
3. Train candidate dictionaries (`zstd --train` / cover) per variant plus
   a mixed control; benchmark ratio and shared CPU/event across encoder
   speed levels against the §3 table's deflate baselines.
4. Decide the v2 compression surface from that data and close this
   section. If dict-zstd wins (expected), design the negotiation cleanly
   (versioned dictionary ID on the wire, so a future retrain is not a
   breaking change) rather than carrying v1's `compress=true` verbatim.
5. Keep permessage-deflate available regardless: it is the zero-config
   default for casual/browser consumers; the dictionary scheme is the
   opt-in for high-volume consumers. Revisit per-connection deflate cost
   if production shows many non-opting subscribers.

## 4. Implementation shape (refactor decisions)

The scattered `if deps.V2` branching is replaced by a small per-endpoint
**wire policy** value owned by the handler package:

- encoder family (v1 exploded single-op events vs v2 commit-scoped frames
  + payload variants),
- cursor policy (v1: clamp + timestamp translation; v2: strict seq-only,
  reject too-old/future),
- error signaling (v1: legacy HTTP/close behavior; v2: error frames + close
  codes),
- compression admission (v1: dict-zstd or deflate; v2: per §3, open),
- resync-row emission (v1: skip; v2: emit).

`serve()` is split into named phases (admission → negotiate → cursor
resolve → upgrade → run) so each policy hook has one obvious call site.
The Tail/hot-ring/replay/filter/slow-detect machinery is deliberately
untouched — the rotation-seam and slow-detect logic is well-tested and none
of these changes alter read-side semantics. The commit reassembler (group
same-`(did, rev)` rows closed by the persisted last-op marker, §2.5, into
one frame at encode time) is the one new component; it lives on the encode
path so both the hot ring's memoized entries and the cold replay path
produce identical frames.

Implementation sequencing when unparked:

1. Wire-policy extraction + serve() phase split (was implemented and
   tested green on 2026-07-01, then reverted to keep main quiet while
   parked; mechanical to redo: `policy.go` with a wirePolicy struct —
   name, emitResyncRows, rejectBelowFloor, encoded/compressed func fields
   — plus admission/upgrade/runSession phase functions in handler.go).
2. Segment format v2 (§2.5: Since/PrevData columns + last-op marker +
   AppendBatch-per-commit), folded into or immediately after the
   timestamps refactor's format change.
3. v2 frame encoder + payload variants (§2.1/§2.2).
4. Strict cursor policy (§2.3) and error frames (§2.4).
5. Go client + docs + corpus capture (§5, §3 step 2).

## 5. Consequences for the Go client

- consumes commit-scoped frames; `Batch`/`Event` mapping: a frame maps to
  one `Event` whose `Commit` carries `Ops []Op` (public API shape change
  from the flat per-op `Commit` in 2026-06-18-go-client-design.md §2.1),
- connects with `payload=cbor`, decodes records locally (identical values
  on backfill and live paths, cheaper wire),
- replaces the CursorTooOldMarker string-parsing contract with the
  structured error frame (`floor_seq`),
- handles `FutureCursor` as a terminal must-re-backfill signal,
- the JSONL live buffer keeps working (frames are single-line JSON;
  buffer entries become frames rather than per-op events).

## 6. Explicitly rejected alternatives

- **Transport batching (`{"events":[…]}` frames packing unrelated
  events):** an earlier draft of this note. Rejected: it invents a
  grouping the firehose doesn't have, requires per-connection frame
  assembly (killing shared encode memoization), and its main benefit
  (replay throughput) doesn't justify diverging from firehose shape.
  Commit-scoped frames give atomicity + firehose parity instead.
- **Per-commit seq allocation:** deep surgery across segment format,
  manifest, replay, and backfill for no consumer benefit; per-op seq with
  envelope seq = last op achieves the same resume contract.
- **DAG-CBOR binary framing (full firehose parity):** cheapest wire, but
  kills the drive-by JSON consumer story that is jetstream's identity, and
  the biggest rework. `payload=cbor` captures most of the byte win inside
  JSON framing.
- **Tip coalescing window (~25 ms) to enable shared no-context
  compression:** trades tail latency for fanout CPU on an unmeasured
  bottleneck; also moot under commit-scoped frames, which don't batch.
- **Keeping timestamp cursors on v2 for migration convenience:** carries
  the 1e15 disambiguator in the v2 contract forever; v1 exists precisely
  for that client population.
- **`payload=both` (decoded `record` + `record_cbor` in one frame):** it
  is the current pay-2.3x wire shape this redesign exists to eliminate,
  and it compresses poorly under any per-variant dictionary (JSON
  structure interleaved with base64 runs matches neither corpus). A
  consumer that wants both takes `payload=cbor` and decodes locally —
  exactly what the bundled Go client does. Dropping it also shrinks the
  Entry memoization matrix (3 variants, not 4), which every compression
  scheme multiplies against.

## 7. Test surface impact

- encoder tests: commit reassembly (single-op, multi-op, resume-inside-a-
  commit, filter-drops-subset, filter-drops-all), per payload variant;
  golden fixtures for the v2 frame shape (v1 goldens untouched);
- handler tests: error-frame registry (name/close-code/fields per
  condition), FutureCursor rejection, timestamp-cursor rejection,
  `maxMessageSizeBytes` at frame granularity;
- fuzz: FuzzEncodeV2 extended over payload variants and multi-op grouping;
- Go client contract test: structured-error parity replaces the
  CursorTooOldMarker literal parity;
- oracle: the client-driven tier picks up the new frame shape via the
  client; the seam/slow-detect tests are unaffected by design (§4).
