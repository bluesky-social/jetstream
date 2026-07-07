# Glossary

Words that show up all over the code and docs, with a one-line meaning and where to read more. If a term here drifts from the code, the code and the linked source win — fix this file.

## Storage and data format

**Segment (segment file, `.jss`)** — the on-disk unit of storage: a columnar, zstd-compressed, length-prefixed log of firehose events. Files are named with a zero-padded base-36 counter so they sort in creation (and time) order. Source of truth: `docs/README.md` §3.1, `segment/doc.go`.

**Block** — a compressed batch of events inside a segment. Blocks are the unit of decode and the unit the cold reader and block cache work with; each sealed segment's footer indexes its blocks. Source: `docs/README.md` §3.2, `segment/doc.go`.

**Seal** — the one-way transition that finalizes an active segment into an immutable file: it writes the footer (block index, DID blooms, collection index) and a finalized 256-byte header with an xxh3 checksum. After sealing, the bytes never change. Source: `docs/README.md` §3.1, `segment/doc.go`.

**Footer** — the trailing metadata block a seal writes: the block index, a segment-level DID bloom filter, per-block DID blooms, and the collection block index. Lets a reader find the right blocks without scanning the whole file. Source: `docs/README.md` §3.1.2–§3.1.4.

**Generation** — informal term for a segment file's version across rewrites. Compaction and merge rewrite sealed files into new generations while preserving block topology (a fully-dropped block stays as an `event_count=0` block) so block numbers stay stable across generations. Source: `docs/README.md` §3.3 (block topology note near the end of the section).

**Tombstone** — the record of a delete or update, keyed by AT URI. Tombstones aren't applied to segments synchronously; compaction applies them later. There is no read-time overlay — clients fold the stream themselves. Source: `docs/README.md` §3.3, `internal/tombstone`.

**Watermark (compaction watermark, `compaction/seq`)** — the highest seq that physical compaction has covered. Below it, superseded create/update rows are physically gone; the uncompacted tail `(watermark, tip]` may still carry rows a later marker will kill. Owned by the compactor. Source: `docs/README.md` §3.3, §3.5.

**Manifest** — the list of segments jetstream serves. It's deliberately not stored in pebble: it's just a directory scan plus each file's self-describing header, so it can't drift from what's on disk. Source: `docs/README.md` §3.5.

**Metadata store** — the single pebble db at `data/meta.pebble/` holding everything that isn't cheaply re-derivable from segments: `relay/cursor`, lifecycle `phase`, `repo/<did>`, `account/<did>`, `sync/<did>`, `compaction/seq`. Source: `docs/README.md` §3.5, `internal/store`.

## Ingestion lifecycle

**Bootstrap phase** — the initial full-network backfill: paginate the relay's listRepos, download every repo via getRepo, and write it to disk, while a live consumer simultaneously captures the firehose into `backfill/live_segments/`. Source: `docs/README.md` §4.1, `internal/ingest/backfill/doc.go`.

**Merge phase** — the cutover step that drains the captured live segments into the steady-state `segments/` tree, filtering out events already covered by the backfilled repo head. Source: `docs/README.md` §4.2, `internal/ingest/orchestrator/doc.go`.

**Cutover** — the whole bootstrap → merging → steady-state transition, anchored by two durable commit points (`phase=merging`, `phase=steady_state`) so a crash mid-cutover recovers by re-entering the state machine. Source: `docs/README.md` §4, `internal/ingest/orchestrator/doc.go`.

**Steady state** — normal operation after cutover: one live consumer pumps the firehose into `segments/`, failed backfills retry on the side, and compaction runs periodically. Source: `docs/README.md` §4.3.

**Cursor (seq, sequence number)** — the monotonic 64-bit id jetstream assigns each event at ingestion. Also the value clients pass as `?cursor=`. Inclusive, starts at 1, instance-local. Source: `docs/README.md` §2. See also `specs/invariants.md`.

## Serving the stream

**Readable log (hot tail)** — the byte-bounded, seq-indexed FIFO the ingest writer keeps of recently appended events, so caught-up subscribers get served from memory and wake on the next append. (This replaced the older push-broadcaster "hot ring" model — a slow reader no longer overflows a per-client channel.) Source: `internal/subscribe/doc.go`, `internal/ingest` writer.

**Cold reader** — the fallback path when a subscriber's cursor is older than what the readable log still holds in memory: a bounded disk walk over sealed segments plus the active segment's flushed region, routed through a shared decoded-block LRU cache. Source: `internal/subscribe/doc.go`, `internal/subscribe/replay.go`.

**Bucketed** — a timestamp-import status flag (`getImportStatus`) meaning the import's rows have been grouped/bucketed for processing. Narrow term, only in the import API — not a general storage concept. Source: `internal/timestamp`, `docs/README.md` §8.

## Testing

**Oracle** — the end-to-end correctness harness: boots a real server against the simulated network, drives its whole lifecycle, and compares durable output against an independent model. A bug detector, not a proof. Source: `specs/oracle.md`, `internal/oracle/doc.go`.

**Simulator** — the fake atproto network (PLC + PDS + relay) that generates real atproto-shaped bytes for the oracle and for local dev. Source: `internal/simulator/doc.go`, `specs/oracle.md`.

**Bubble** — a `testing/synctest` bubble: a test scope with a fake clock where the runtime knows when every goroutine is blocked, so the system quiesces deterministically. Only one is allowed per process. Source: `internal/oracle/doc.go`, `specs/oracle.md`.

**Tier** — one family of oracle checks that share helpers but fail with a distinct explanation (storage, event-log, replay, XRPC egress, crash/restart, store-fault, segment-fault, simulator fidelity, corpus, soak, determinism). Source: `specs/oracle.md`.

**Mutant / mutation campaign** — a curated single-edit bug (a "mutant") that models a realistic failure; the campaign applies each one and checks the oracle catches it, measuring the oracle's bug-detection power. Never fix production code to match a mutant. Source: `AGENTS.md`, `testing/mutation/RESULTS.md`, `specs/oracle.md`.
