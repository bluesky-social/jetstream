# Glossary

Definitions and references for terms used in the code and docs. The code and linked sources take precedence over this glossary.

## Storage and data format

**Segment (segment file, `.jss`)** — the on-disk unit of storage: a columnar, zstd-compressed, length-prefixed log of firehose events. Files are named with a zero-padded base-36 counter so they sort in creation (and time) order. Source: `docs/README.md` §3.1, `segment/doc.go`.

**Block** — a compressed batch of events inside a segment. Blocks are the unit of decode and the unit the cold reader and block cache work with; each sealed segment's footer indexes its blocks. Source: `docs/README.md` §3.2, `segment/doc.go`.

**Seal** — the one-way transition that finalizes an active segment into an immutable file: it writes the footer (block index, DID blooms, collection index) and a finalized 256-byte header with an xxh3 checksum. After sealing, the bytes never change. Source: `docs/README.md` §3.1, `segment/doc.go`.

**Footer** — the trailing metadata block a seal writes: the block index, a segment-level DID bloom filter, per-block DID blooms, and the collection block index. Lets a reader find the right blocks without scanning the whole file. Source: `docs/README.md` §3.1.2–§3.1.4.

**Generation** — informal term for a segment file's version across rewrites. Compaction and merge rewrite sealed files into new generations while preserving block topology (a fully-dropped block stays as an `event_count=0` block) so block numbers stay stable across generations. Source: `docs/README.md` §3.3 (block topology note near the end of the section).

**Tombstone** — the record of a delete or update, keyed by AT URI. Tombstones aren't applied to segments synchronously; compaction applies them later. There is no read-time overlay — clients fold the stream themselves. Source: `docs/README.md` §3.3, `internal/tombstone`.

**Watermark (compaction watermark, `compaction/seq`)** — the highest seq that physical compaction has covered. Below it, superseded create/update rows are physically gone; the uncompacted tail `(watermark, tip]` may still carry rows a later marker will kill. Owned by the compactor. Source: `docs/README.md` §3.3, §3.5.

**Manifest** — the list of segments jetstream serves. It is rebuilt from a directory scan and file headers, rather than stored in pebble. Source: `docs/README.md` §3.5.

**Metadata store** — the single pebble db at `data/meta.pebble/` holding everything that isn't cheaply re-derivable from segments: `relay/cursor`, lifecycle `phase`, seq tip/lease/registered vacancies, `repo/<did>`, the `pdshost/<hostname>` fleet roster/cursors, `account/<did>`, `sync/<did>`, and `compaction/seq`. Reached through the `metastore.Store` interface (`internal/metastore`); the Pebble impl is `internal/metastore/pebblestore`. Source: `docs/README.md` §3.5.

## Ingestion lifecycle

**Host roster** — the durable `pdshost/<hostname>` control-plane set obtained from relay listHosts: relay metadata, host-local listRepos cursor, terminal state, and diagnostics. The relay account count is a floor, not authoritative. Source: `docs/README.md` §3.5 and §4.1.

**Mushroom** — one of Bluesky's large shared PDS hosts under `*.host.bsky.network`; their per-host getRepo limits dominate bootstrap time. Source: `specs/notes/2026-08-03-pds-direct-backfill-design.md`.

**Bootstrap phase** — the initial full-network backfill: read the relay's host roster, paginate listRepos directly on each PDS, download repos directly from their PDS, and write them to disk, while a live consumer simultaneously captures the firehose into `backfill/live_segments/`. Source: `docs/README.md` §4.1, `internal/ingest/backfill/doc.go`.

**Merge phase** — the cutover step that drains the captured live segments into the steady-state `segments/` tree, filtering out events already covered by the backfilled repo head. Source: `docs/README.md` §4.2, `internal/ingest/orchestrator/doc.go`.

**Cutover** — the whole bootstrap → merging → steady-state transition, anchored by two durable commit points (`phase=merging`, `phase=steady_state`) so a crash mid-cutover recovers by re-entering the state machine. Source: `docs/README.md` §4, `internal/ingest/orchestrator/doc.go`.

**Steady state** — normal operation after cutover: one live consumer pumps the firehose into `segments/`, failed backfills retry on the side, and compaction runs periodically. Source: `docs/README.md` §4.3.

**Cursor (seq, sequence number)** — the monotonic 64-bit id jetstream assigns each event at ingestion. Also the value clients pass as `?cursor=`. Inclusive, starts at 1, instance-local, and may jump across a durable registered vacancy after an unclean restart. Source: `docs/README.md` §2. See also `specs/invariants.md`.

**Sequence lease / registered vacancy** — the steady writer durably reserves one block of seqs ahead of client visibility. An unclean restart permanently records the abandoned half-open interval as vacant before allocating beyond it, preventing an observed seq from being reused. Source: `docs/README.md` §2, `internal/ingest/seqlease.go`.

## Serving the stream

**Readable log (hot tail)** — the byte-bounded, seq-indexed FIFO the ingest writer keeps of recently appended events, so caught-up subscribers get served from memory and wake on the next append. Source: `internal/subscribe/doc.go`, `internal/ingest` writer.

**Cold reader** — the fallback path when a subscriber's cursor is older than what the readable log still holds in memory: a bounded disk walk over sealed segments plus the active segment's flushed region, routed through a shared decoded-block LRU cache. Source: `internal/subscribe/doc.go`, `internal/subscribe/replay.go`.

## Testing

**Oracle** — the end-to-end correctness harness: boots a real server against the simulated network, drives its whole lifecycle, and compares durable output against an independent model. A bug detector, not a proof. Source: `specs/oracle.md`, `internal/oracle/doc.go`.

**Simulator** — the fake atproto network (PLC + PDS + relay) that generates real atproto-shaped bytes for the oracle and for local dev. Source: `internal/simulator/doc.go`, `specs/oracle.md`.

**Bubble** — a `testing/synctest` bubble: a test scope with a fake clock where the runtime knows when every goroutine is blocked, so the system quiesces deterministically. Only one is allowed per process. Source: `internal/oracle/doc.go`, `specs/oracle.md`.

**Tier** — one family of oracle checks that share helpers but fail with a distinct explanation (storage, event-log, replay, XRPC egress, crash/restart, store-fault, segment-fault, simulator fidelity, corpus, soak, determinism). Source: `specs/oracle.md`.

**Mutant / mutation campaign** — a curated single-edit bug (a "mutant") that models a realistic failure; the campaign applies each one and checks the oracle catches it, measuring the oracle's bug-detection power. Never fix production code to match a mutant. Source: `AGENTS.md`, `testing/mutation/RESULTS.md`, `specs/oracle.md`.
