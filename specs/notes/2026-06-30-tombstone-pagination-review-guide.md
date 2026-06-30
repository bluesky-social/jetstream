# Reviewer's guide: drop client tombstones + paginated backfill

Date: 2026-06-30
Branch: `tombstone-query-plan-refactor`
For: anyone reviewing this branch before we merge it.

This branch is big (117 files). This guide is here to make it easy to review. It
explains what the change does, which parts matter most, how to run it, and which
tests to step through to understand it.

If you want the full background, three other docs have it:

- `2026-06-28-drop-client-tombstones-design.md` — the design. Read the
  "Revision 2026-06-28b" section (§R1–§R8) first. The older sections below it are
  just the thinking we did along the way; when they disagree with the §R section,
  the §R section wins.
- `2026-06-28-drop-client-tombstones-implementation.md` — the step-by-step plan,
  with notes on what actually shipped for each step.
- `2026-06-30-tombstone-pagination-review.md` — the bugs we found in our own review
  before shipping, and how we fixed them. A good list of the risky spots.

---

## 1. What this change does

There are two related changes. They ship together because they touch the same client
code, and doing them separately would mean rewriting that code twice.

### Change A: the client no longer hides deleted records

**Before:** When you asked Jetstream for old data, it tried to give you a "clean"
view. If a record had been created and then later deleted, the server and client
worked together to *hide* the create from you, so you never saw records that were
already gone. This required a special server endpoint (`getTombstones`) and special
client code (the "Suppressor").

**After:** Jetstream just gives you every event, in order, at least once. If a record
was created and later deleted, you get **both** events — the create *and* the delete.
It does not hide anything for you.

To end up with a correct picture of the network, the client now "folds" the events as
they arrive: a create adds a record, a delete removes it, an update replaces it. If you
play all the events in order, you get the right answer. The deletes are real events you
can see, not invisible gaps.

The one rule we still strictly promise: **we never silently drop data you asked for and
that we can still serve.** If the server has an event that matches your filter and skips
it, that's a bug, and we'd rather crash than quietly lose it.

### Change B: backfill is now paginated, with no client-side buffer

**Before:** The client made one big request for all the old data. If it was too big,
the server refused. While downloading, the client held incoming live events in a buffer
so nothing was lost during the handoff to the live stream.

**After:** The client asks for the old data one page at a time. On the first page it
learns the "sealed tip" (call it `S`) — the high-water mark of the archived data — and
locks that in as its target. It keeps asking for pages until it has caught up to `S`,
then connects to the live stream once, starting at `S`. There's no buffer anymore — the
server's own replay covers the handoff. Hence the slogan: "jetstream is your buffer."

### Two supporting changes

- **Sequence numbers now start at 1.** They used to start at 0, which caused a subtle
  bug where the very first event could get swallowed. Starting at 1 makes that bug
  impossible and let us delete a bunch of "is this 0 or is this nothing?" code.
- **Sentinel collections.** Change A created one gap we had to close. See §3 below —
  it's the most important part to review.

---

## 2. The one idea behind the whole change

Everything follows from a single swap:

> Instead of the server hiding deleted records for you (using a separate, changing list
> that could be out of date), the server just sends you the delete events inline, in
> order, and you apply them yourself.

That swap is why we could delete the overlay endpoint, the client Suppressor, and the
buffer. When reviewing, the question to keep asking is: *"Is there any event the server
could serve that this code now skips without sending?"* If the answer is ever yes,
that's the bug we care most about.

---

## 3. The most important part to review: the "sentinel collection" gap

This is the trickiest and most important piece. Read it carefully.

**The problem.** Most deletes are tied to a collection (a record type, like
`app.bsky.feed.post`). So if you ask for "all posts," you naturally get the post deletes
too. But some events are about a whole account, not a single record: account deletions
(`#account`), identity changes (`#identity`), and resyncs (`#sync`). These don't belong
to any collection.

Now suppose you ask only for posts. The old hiding mechanism is gone (Change A). When
you backfill, the server picks archive blocks *by collection*. An account-deletion has
no collection, so it never gets picked — and you'd never find out the account was
deleted. You'd keep its posts forever. That's exactly the kind of silent data problem we
promised not to have.

**The fix.** When the server seals or rewrites an archive segment, and a block contains
one of these account-level events, it tags that block with a fake collection name:
`$account`, `$identity`, or `$sync`. These names start with `$`, which is not a legal
collection name, so no real request can ever ask for them by accident. When you backfill
with a collection filter, the planner always includes these tagged blocks too. So the
account-level events ride along with your normal download, in order, and your fold gets
them.

**Why it's safe from races.** The server reads the actual files on disk when you ask for
a block. The delete event travels in the same download, in order, right alongside the
record it deletes. There's no separate list that could be stale. (An earlier version of
this fix *did* use a separate snapshot and needed a complicated proof; we threw it out
and replaced it with this simpler approach.)

**Files to read:**
- `segment/sentinel.go` — defines the three names and the helpers. The comment at the
  top is the best explanation.
- `segment/seal.go` and `segment/rewrite.go` — where blocks get tagged. These share one
  helper so the two paths can't drift apart.
- `internal/manifest/plan.go`, function `collectionIDsForSegment` — where the planner
  always includes the tagged blocks.

---

## 4. Other important code changes

In rough order of how much attention they deserve.

### The paginated planner (`internal/manifest/plan.go`)

This is the other piece with real correctness risk. When a page of results gets too big,
the server stops at a clean boundary and tells the client where to continue.

Things to check:
- The "continue from here" cursor is the last seq number we actually included — never
  the end of a segment we only partly included. (Stopping at the segment end would skip
  the part we didn't send, losing data.)
- A page always includes at least one unit of work, even if that one unit is over the
  size limit. Otherwise the client could loop forever making no progress.
- There are now two numbers in the result: `SealedTipSeq` (the fixed target) and
  `PlannedThroughSeq` (how far this page got). When a page isn't truncated, they're equal.
- We deleted the old "request too large" error entirely.

There's also a new safety check on load (`validateBlockOffsets` in `segment/reader.go`)
that enforces the assumption the planner relies on, instead of just trusting it.

### The client backfill loop (`internal/client/engine.go`, `live.go`)

- `sweepSealedArchive` (in `engine.go`) is the loop: ask for a page, download it, emit
  it, advance the cursor, repeat until caught up to `S`. It pins the upper bound to `S`
  the whole time.
- After catching up, the client connects to the live stream once, at `S`. No buffer, no
  "rewind margin."
- If the handoff cursor has aged out by the time we connect (a slow backfill), the server
  replies with an HTTP 400 "cursor too old." The client recognizes this and
  *transparently re-runs the backfill* from where it left off. This is bounded (max 5
  tries) and must make forward progress each time, or it crashes on purpose rather than
  loop forever.
- A lot of code was deleted here: the whole `livesink.go` file, the buffer interface, and
  at the project root `buffer.go`, `buffer_mem.go`, `buffer_file.go`, and the
  `--live-buffer-file` flag.
- `Client.Stats()` is new — it reports pages fetched, the residual gap, and re-backfill
  count, since the client library doesn't have metrics.

### The cursor policy on the server (`internal/subscribe/cursor.go`, `handler.go`)

- `/subscribe-v2` now rejects a too-old cursor with a clear HTTP 400 that includes the
  floor. `/subscribe` (v1) still silently clamps, on purpose, to stay compatible with
  the old Jetstream — **don't "fix" this.**
- If the server hits a disk error while translating a cursor, it now returns a 500 (a
  real server error) instead of a 400 that leaked the internal file path.

### Sequence numbers start at 1 (`internal/ingest/writer.go`)

The counter now seeds at 1 on a fresh install. The interesting bit to confirm: we only
do this in memory, so we still never write to the database for a brand-new empty
directory.

### The big deletions

`internal/overlay/` (the whole package), the `getTombstones` endpoint, the client
Suppressor, and the buffers. These have no remaining users (we checked the status page
and replication code). Skim these for completeness — there's no logic to verify, just
confirm nothing important got caught in the net.

---

## 5. Documentation changes

- `docs/README.md` is the main contract doc. The important edits:
  - A new invariant in §2 explaining the "fold it yourself" model — this is what
    third-party client authors will read.
  - §2.1 rewritten for the new paginated loop.
  - §3.1 / §3.3: tombstones are applied during compaction, there is no read-time
    overlay, and the sentinel-collection mechanism is explained.
  - §4.4: account/identity/sync events are now always delivered on both v1 and v2.
  - §5: the v2-rejects vs v1-clamps difference.
- `specs/oracle.md` — updated to describe the new fold-based correctness checks.
- The root `doc.go` — shows client authors the fold pattern and the re-backfill behavior.

---

## 6. How to run it

You'll need `just install-tools` first (installs the linter and test runner).

```sh
just                 # lint + short tests (the normal check)
just lint            # just the linter
just test            # short test suite, all packages
just test-long ./internal/oracle   # the full oracle suite
just lexgen          # regenerate API code after a lexicon change
```

To watch the whole system work end-to-end, use the **oracle**. It runs the real server
against a fake network and checks all the correctness rules:

```sh
go test ./internal/oracle -run TestOracle_DefaultLifecycle -v   # the main one
just oracle                # heavier stress version
just oracle-sweep 5        # several random seeds (what nightly CI does)
```

To run the server by hand against the local simulator:

```sh
just simulator       # the fake network, in one terminal
just run             # the server, in another
just run-client ...  # the bundled client
```

The mutation campaign checks that our tests actually catch bugs:

```sh
just mutation-campaign      # run all the deliberate-bug mutants
just mutation-gate          # check against the committed baseline
```

One thing to confirm after merge: run `just mutation-gate` and check it says `PASS`.
(There's a tracked follow-up, #183, but it's not a blocker.)

---

## 7. The best tests to step through

If you want to understand the change by reading tests, go in this order:

1. **`internal/oracle/foldconvergence_gate_test.go`** →
   `TestFoldConvergence_CollectionFilteredDIDTombstoneGap`. This single test shows the
   whole problem from §3 and proves the fix works. If you read one test, read this one.

2. **`segment/sentinel_test.go`** — small and fast. Shows the `$`-names are rejected as
   real collections, and that sealing/rewriting tags blocks correctly.

3. **`internal/manifest/plan_test.go`** — the pagination logic:
   - `TestPlanBackfill_MidSegmentCutCursorIsBlockMaxSeq` — proves we don't skip the tail
     of a partly-included segment.
   - `TestPlanBackfill_OneUnitOverCapStillAdvances` — proves the loop can't get stuck.

4. **`internal/client/engine_test.go`** — the client loop:
   - `TestEngineMultiPageBackfillCutover` — three pages add up to the right answer.
   - `TestEngineTooOldHandoffReBackfills` — the re-backfill recovery works.
   - `TestEngineTooOldPingPongIsFatal` — and it crashes instead of looping forever.

5. **`internal/oracle/partb_scenarios_test.go`** — the most realistic, end-to-end
   scenarios. Start with the "mid-download seal" and "caught-up handoff" ones.

---

## 8. Where to be extra careful

- **The on-disk file format.** We claim the format didn't change — only the *content* of
  the sealed footers (the new sentinel names). Double-check no version bump or new
  section snuck in. This is safe to change only because nothing is deployed yet.
- **The v1 silent clamp.** It's intentional, for backward compatibility, and we made it
  visible with a metric. Please don't ask us to turn it into an error.
- **Test coverage honesty.** Deleting the overlay package removed a test that used to
  catch one specific bug (m022). We restored that coverage a different way. Confirm the
  mutation tiers actually run and catch it.
- **Leftovers from the reverted approach.** Search for `getTombstones`,
  `wantDidTombstones`, and `snapshot` — they should only appear in the old design notes
  (kept on purpose as history), not in live code.

---

## 9. Suggested review order

1. Read §1–§3 of this guide and §R1–§R8 of the design doc.
2. The sentinel fix: `segment/sentinel.go` → `seal.go`/`rewrite.go` → `plan.go`.
3. The pagination logic in `plan.go` and its tests.
4. The client loop in `engine.go` / `live.go` and its tests.
5. The cursor policy in `internal/subscribe/`.
6. The sequence-number change in `writer.go`.
7. Skim the big deletions.
8. Read the doc changes and confirm they match the code.
9. Run the commands in §6 and step through the tests in §7.

If you only have half an hour: step through the gate test in §7 #1 (the whole point of
the change), then `TestEngineTooOldHandoffReBackfills` (the riskiest new code), then read
the findings in the review doc.
</content>
