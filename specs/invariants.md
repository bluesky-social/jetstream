# Invariants

These rules protect archive integrity and client correctness. `docs/README.md` is authoritative; fix this summary if they disagree.

## The rules

**Sealed segments are immutable.** Compaction and merge write new files and atomically rename them into place. Cached bytes remain valid for the lifetime of the original file. See `docs/README.md` §3.1 and `segment/doc.go`.

**fsync the segment before committing to pebble.** Append and fsync each block before committing its `relay/cursor` and `repo/<did>` updates with `sync=true`. A crash between these steps may cause replay, but cannot advance the cursor past durable data. See `docs/README.md` §3.5.

**The cursor is inclusive, and seq 0 means "nothing yet."** `?cursor=N` replays starting at the event with seq N (we deliver events with seq >= N). Sequence numbers start at 1; seq 0 is a reserved "before the beginning" sentinel, so `?cursor=0` replays everything. Seqs are assigned at ingestion and are instance-local — different jetstream instances assign their own. See `docs/README.md` §2.

**A client-observable seq is never reused.** Before the steady writer can publish a seq to the readable log, that seq must be below the exclusive `seq/max_reserved` value durably committed in pebble. Unclean startup turns any abandoned lease tail into an explicit `seq/gap/*` vacancy and resumes after it; clean terminal close collapses the lease to the exact durable coverage frontier (events plus registered vacancies). Cursor resolution and cold replay may jump only registered vacancies. An unregistered coverage hole remains an error. Seqs can have gaps, but no seq a client could have observed may later identify a different event. See `docs/README.md` §2 and §3.1.1.

**At-least-once delivery; clients must be idempotent.** Resume and re-merge can deliver an event again. Consumers must tolerate duplicates. See `docs/README.md` §1.1 and §2.

**Per-DID order is preserved.** Events for each DID retain ingestion order through replay, merge, and compaction. See `docs/README.md` §2 and §3.4.

**Segment files sort in creation order, and that order is time order.** Segments are named with a zero-padded base-36 counter, so a lexicographic sort of filenames is creation order, and every event in `seg_N` was witnessed before every event in `seg_N+1`. The block topology (which DIDs and collections live in which block) is self-describing in each sealed file's footer and does not depend on any external index staying in sync. See `docs/README.md` §3.1 and §3.4.

**Stop on internal corruption; continue past invalid upstream data.** Persistence corruption, fsync failures, impossible segment structure, and durability violations must stop the process to avoid further corruption. Malformed relay frames or backfill records must not stop the server: drop the record or event, increment a warning/error metric, and bound any diagnostic logging. See `AGENTS.md` and `docs/README.md` §4.4.

## See also

- `docs/README.md` §2 — the canonical invariants list this file summarizes.
- `specs/gotchas.md` — accepted limitations and lessons.
- `specs/architecture.md` — how the subsystems these rules govern fit together.
