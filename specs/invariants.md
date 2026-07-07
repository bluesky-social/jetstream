# Invariants

This is the short list of rules jetstream must never break. It's a quick-reference for agents: if a change would violate one of these, stop and rethink. Each entry says what the rule is and where the authoritative explanation lives — `docs/README.md` is the spec, and this file just points into it. When the two disagree, `docs/README.md` wins; fix this file.

Most of these are correctness properties that clients build on or that keep the archive from corrupting itself. They've each been paid for at least once, so treat them as load-bearing rather than aspirational.

## The rules

**Sealed segments are immutable.** Once a segment file is sealed, its bytes never change again. Compaction and merge produce *new* files and swap them in with a tmp-write-then-rename; they never edit a sealed file in place. Anything that caches segment contents can trust that a given sealed file's bytes are stable for the life of that file. See `docs/README.md` §3.1 and `segment/doc.go`.

**fsync the segment before you commit to pebble.** For every block, the order is: append and fsync the block into the active segment file first, then commit the pebble batch (with `sync=true`) that advances `relay/cursor` and the per-DID `repo/<did>` bookkeeping. Never the other way around. This is what makes a crash safe: because the cursor is written after the data, a crash between the two leaves `relay/cursor` pointing at or before the last durable event, so restart just replays a few events instead of losing them. See `docs/README.md` §3.5.

**The cursor is inclusive, and seq 0 means "nothing yet."** `?cursor=N` replays starting at the event with seq N (we deliver events with seq >= N). Sequence numbers start at 1; seq 0 is a reserved "before the beginning" sentinel, so `?cursor=0` replays everything. Seqs are assigned at ingestion and are instance-local — different jetstream instances assign their own. See `docs/README.md` §2.

**At-least-once delivery; clients must be idempotent.** We do not do exactly-once. The same event can be delivered more than once (a resuming client re-receives its last-seen event; a re-merge can re-emit a row). This is a deliberate non-goal, not a bug — the same guarantee the upstream firehose gives. Anything consuming the stream has to dedupe. See `docs/README.md` §1.1 and §2.

**Per-DID order is preserved, always.** Events for a single DID are replayed in the same order they were ingested, across every phase and every segment generation. Segment files lay events out by DID on disk specifically to hold this line, and it survives compaction and merge — a record's history for one DID never reorders. Building AppViews correctly depends on this. See `docs/README.md` §2 and §3.4.

**Segment files sort in creation order, and that order is time order.** Segments are named with a zero-padded base-36 counter, so a lexicographic sort of filenames is creation order, and every event in `seg_N` was witnessed before every event in `seg_N+1`. The block topology (which DIDs and collections live in which block) is self-describing in each sealed file's footer and does not depend on any external index staying in sync. See `docs/README.md` §3.1 and §3.4.

**Crash loud on our own corruption; never crash on bad upstream data.** These are two halves of one boundary. Invalid *internal* state — persistence corruption, fsync failures, impossible segment structure, a broken durability invariant — should crash the process loudly rather than limp along and risk corrupting the archive. Invalid *upstream* data — a malformed firehose frame, an over-limit field, garbage from the relay — must never crash, stop, or exit the server: drop the offending record or event, bump a warning/error metric, log bounded diagnostics, and keep running. Treat everything from the relay/firehose/backfill as untrusted input. See `AGENTS.md` ("Never crash, and never corrupt data") and `docs/README.md` §4.4.

## See also

- `docs/README.md` §2 — the canonical invariants list this file summarizes.
- `specs/gotchas.md` — accepted limitations and hard-won lessons (the things that are *not* invariants because we deliberately decided to live with them).
- `specs/architecture.md` — how the subsystems these rules govern fit together.
