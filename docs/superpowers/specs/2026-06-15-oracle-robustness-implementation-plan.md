# Oracle Robustness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade `internal/oracle` from a mostly final-state storage oracle into a tiered, traceable, product-path oracle that catches harder storage, replay, compaction, restart, and failure-contract bugs.

**Architecture:** Build incrementally around the existing harness. First strengthen checkers and add trace artifacts, then add event-log and public replay observers, then add crash/fault/fidelity tiers. Keep real socket/filesystem durability tests intact; fake transport and `synctest` experiments come later as supplemental modes.

**Tech Stack:** Go 1.26, `testing`, `gotestsum`, `just`, `gh`, Pebble-backed stores, `github.com/coder/websocket`, existing `internal/oracle`, `internal/simulator`, `internal/ingest`, `internal/subscribe`, `internal/xrpcapi`, and `testing/mutation`.

## Execution Status

Milestone A is complete on `main` via PR #22.

| Plan area | Issues | Commits | Status |
|---|---|---|---|
| Phase 1: mutation assertion gaps | #9 | `1967891`, `e143db7` | Complete |
| Phase 2: canonical traces | #12 | `2062913` | Complete |
| Phase 3: deterministic account event time | #18 | `19063c1` | Complete |
| Phase 4: event-log equivalence | #13-#17 | `cec8c3c`, `042724f`, `4ce97e3`, `4451aca`, `8fe6e20` | Complete |
| Phase 5 onward | unstarted | none | Planned |

The detailed checkboxes below are the original worker plan. For Phases 1-4,
they should be read as historical implementation guidance, not as outstanding
work. Future agents should resume at Phase 5 / Milestone B unless a regression
or review finding reopens Milestone A work.

---

## Mandatory Worker Protocol

Every implementing agent must follow this protocol before touching code.

- [ ] **Step 0.1: Verify GitHub CLI access**

Run:

```bash
gh auth status
```

Expected: authenticated to GitHub with issue permissions for this repo. If not authenticated, stop and ask Jim to authenticate; do not start coding without an issue.

- [ ] **Step 0.2: Create or claim exactly one issue for the phase**

Use the issue title listed in the phase. Create it before coding:

```bash
ISSUE_TITLE="copy the phase Issue title line exactly"
ISSUE_BODY="$(cat <<'EOF'
## Context
Summarize the phase rationale and link the roadmap plus the most relevant code paths.

## Definition of done
List the phase's required behavior, tests, and mutation checks.

## Notes
Call out phase-specific constraints, follow-ups, and open questions.
EOF
)"
gh issue create -t "$ISSUE_TITLE" -b "$ISSUE_BODY" -l enhancement
```

If an appropriate issue already exists, comment before starting:

```bash
ISSUE=123
PHASE_NAME="copy the phase heading exactly"
gh issue comment "$ISSUE" -b "Starting: implementing $PHASE_NAME. Plan is to follow the spec task list in order and keep any follow-up work in linked issues."
```

- [ ] **Step 0.3: Reference the issue in commits**

Commit bodies must include `Refs #N` during development. If a commit fully completes the issue, use `Closes #N` in the commit body or PR description. Do not manually close issues.

- [ ] **Step 0.4: Preserve user changes**

Run:

```bash
git status --short
```

Expected: understand any existing dirty files. Do not revert unrelated changes.

- [ ] **Step 0.5: Keep issues granular**

If a phase reveals separable follow-up work, create a new issue and link it from the current issue instead of expanding scope.

---

## Phase 1: Close Mutation-Campaign Assertion Gaps

**Execution status:** Complete via issue #9 and commits `1967891` /
`e143db7`.

**Issue title:** `oracle: close mutation-campaign assertion gaps`

**Rationale:** The first mutation campaign found bugs the oracle should catch with local checker changes. These do not require transport, trace, or scheduler work.

**What it buys:** Immediate detection improvement, lower false confidence, and a cleaner baseline for larger refactors.

**Files:**
- Modify: `internal/oracle/invariants.go`
- Modify: `internal/oracle/reconstruct_test.go`
- Modify: `internal/oracle/segments.go`
- Create or modify: `internal/oracle/segments_test.go`
- Modify: `internal/oracle/compacted.go`
- Modify: `internal/oracle/compacted_test.go`
- Reference: `testing/mutation/RESULTS.md`
- Reference mutants: `m018`; historical retired mutants `m010` and `m007`
  remain documented in `testing/mutation/RESULTS.md`.

### Task 1.1: Reject Empty Rev On Commit-Kind Events

- [ ] **Write the failing test**

Add a test in `internal/oracle/reconstruct_test.go` or a new `internal/oracle/invariants_test.go`:

```go
func TestCheckInvariantsRejectsEmptyRevOnCommitKind(t *testing.T) {
	t.Parallel()

	for _, kind := range []segment.Kind{segment.KindCreate, segment.KindUpdate, segment.KindDelete} {
		t.Run(kind.String(), func(t *testing.T) {
			err := CheckInvariants([]ObservedEvent{{
				Seq:        1,
				Kind:       kind,
				DID:        "did:plc:a",
				Collection: "app.bsky.feed.post",
				Rkey:       "r1",
			}})
			require.ErrorContains(t, err, "empty rev")
		})
	}
}
```

- [ ] **Run the failing test**

Run:

```bash
just test ./internal/oracle -run TestCheckInvariantsRejectsEmptyRevOnCommitKind -v
```

Expected: FAIL because `CheckInvariants` currently skips empty revs.

- [ ] **Implement the invariant**

In `internal/oracle/invariants.go`, reject empty `Rev` for create/update/delete before the existing rev-regression check. Include seq, kind, DID, collection, and rkey in the error.

- [ ] **Run the passing test**

Run:

```bash
just test ./internal/oracle -run TestCheckInvariantsRejectsEmptyRevOnCommitKind -v
```

Expected: PASS.

### Task 1.2: Add Segment Structural Offset Checks

- [ ] **Write failing tests**

Create `internal/oracle/segments_test.go` if it does not exist. Add tests for a helper that validates block metadata from a sealed segment reader:

```go
func TestCheckSegmentStructureRejectsNonIncreasingOffsets(t *testing.T) {
	t.Parallel()

	err := checkSegmentStructure("seg_00000000000000000000.jss", segment.Header{
		BlockCount: 2,
	}, []segment.BlockInfo{
		{Offset: 200, MinSeq: 1, MaxSeq: 1},
		{Offset: 199, MinSeq: 2, MaxSeq: 2},
	})
	require.ErrorContains(t, err, "non-increasing block offset")
}

func TestCheckSegmentStructureRejectsSeqRegressionAcrossBlocks(t *testing.T) {
	t.Parallel()

	err := checkSegmentStructure("seg_00000000000000000000.jss", segment.Header{
		BlockCount: 2,
	}, []segment.BlockInfo{
		{Offset: 200, MinSeq: 10, MaxSeq: 20},
		{Offset: 300, MinSeq: 20, MaxSeq: 30},
	})
	require.ErrorContains(t, err, "block seq overlap")
}
```

Adjust `segment.BlockInfo` field names to match the actual type in `segment` if needed. Keep the helper unexported unless another package needs it.

- [ ] **Run the failing tests**

Run:

```bash
just test ./internal/oracle -run 'TestCheckSegmentStructure' -v
```

Expected: FAIL because the helper does not exist.

- [ ] **Implement the helper and wire it into sealed observation**

In `internal/oracle/segments.go`, add `checkSegmentStructure(path string, header segment.Header, blocks []segment.BlockInfo) error`.

Wire it into `observeSealedSegment` immediately after opening the segment and before decoding blocks. It should check:

- `len(blocks) == int(header.BlockCount)`;
- offsets are strictly increasing;
- offsets are at or beyond `segment.ReservedHeaderBytes`;
- block seq ranges are increasing and non-overlapping;
- each block has `MinSeq <= MaxSeq` when the block has events.

- [ ] **Run focused tests**

Run:

```bash
just test ./internal/oracle -run 'TestCheckSegmentStructure|TestObserveSegments' -v
```

Expected: PASS.

### Task 1.3: Strengthen Compaction Boundary Checks

- [ ] **Write failing boundary test**

Add to `internal/oracle/compacted_test.go`:

```go
func TestCheckCompactedRejectsBoundarySupersededRow(t *testing.T) {
	t.Parallel()

	events := []ObservedEvent{
		{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "r1"},
		{Seq: 2, Kind: segment.KindUpdate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "r2"},
	}
	err := CheckCompacted(events, 2)
	require.ErrorContains(t, err, "superseded record row survived")
}
```

If this already passes, add the more specific mutant-derived case that currently escapes: a row at exactly the chunk boundary surviving because the checker only reasons about strictly earlier rows. Keep the test name tied to the boundary.

- [ ] **Run the focused test**

Run:

```bash
just test ./internal/oracle -run TestCheckCompactedRejectsBoundarySupersededRow -v
```

Expected: FAIL before the fix, or PASS if this exact case is already covered. If PASS, document the actual missing boundary shape in the issue and test that shape.

- [ ] **Implement the boundary fix**

Update `CheckCompacted` so the committed watermark boundary is treated inclusively everywhere the compaction guarantee requires it. Avoid weakening the rule for rows above the watermark.

- [ ] **Run phase tests and targeted mutants**

Run:

```bash
just test ./internal/oracle
just mutation-campaign m018
just mutation-campaign m019
```

Expected: oracle tests PASS; active targeted mutants are KILLED. Historical
mutants `m010` and `m007` have been retired from the active catalog after
explicit reclassification in `testing/mutation/RESULTS.md`.

- [ ] **Commit**

```bash
git add internal/oracle testing/mutation/RESULTS.md
git commit -m "oracle: close mutation-campaign assertion gaps" -m "Refs #$ISSUE"
```

---

## Phase 2: Add Canonical Oracle Trace Recorder

**Execution status:** Complete via issue #12 and commit `2062913`.

**Issue title:** `oracle: record canonical run traces`

**Rationale:** Seeds fix inputs but not interleavings. Trace artifacts make failures diagnosable even when exact schedules do not replay.

**What it buys:** CI failures include a structured account of phase transitions, faults, generated events, durable appends, compaction, replay observations, shutdown, and restart markers.

**Files:**
- Create: `internal/oracle/trace.go`
- Create: `internal/oracle/trace_test.go`
- Modify: `internal/oracle/harness_test.go`
- Modify: `internal/oracle/restart_harness_test.go`
- Modify: `internal/oracle/faults.go`
- Modify: `internal/oracle/subscribe_replay_test.go`

### Task 2.1: Build The Trace Type

- [ ] **Write tests for canonical JSONL**

Create `internal/oracle/trace_test.go` with tests for:

- monotonic trace indices;
- stable payload hash;
- JSONL records are one line each;
- `nil` trace recorder is safe through helper functions.

- [ ] **Implement `Trace`**

Create `internal/oracle/trace.go`:

```go
type Trace struct {
	mu   sync.Mutex
	next uint64
	w    io.Writer
}

type TraceRecord struct {
	Index uint64         `json:"index"`
	Kind  string         `json:"kind"`
	At    string         `json:"at,omitempty"`
	Data  map[string]any `json:"data,omitempty"`
}
```

Prefer explicit typed helper methods over arbitrary maps where call sites repeat. Use maps only for phase-specific details that are not worth a new struct yet.

- [ ] **Run tests**

```bash
just test ./internal/oracle -run TestTrace -v
```

Expected: PASS.

### Task 2.2: Wire Trace Into Existing Harness Hooks

- [ ] **Add trace setup to `TestOracle_DefaultLifecycle`**

Create a trace file under `t.TempDir()` and record:

- run start metadata;
- simulator config;
- fault plan summary;
- after-bootstrap barrier entered/released;
- after-merge barrier entered/released;
- fault-plan fired assertions;
- steady target seq;
- compaction pass results;
- shutdown start and runtime exit.

- [ ] **Trace append events**

Use `OnBootstrapLiveEvent` and `OnSteadyStateEvent` to record compact event records:

- assigned Jetstream seq;
- upstream relay cursor;
- kind;
- DID;
- collection/rkey;
- rev;
- payload hash.

Do not store full payloads.

- [ ] **Trace replay observations**

In `collectSubscribeReplay`, record replay frame kind, cursor, DID, rev, and payload hash.

- [ ] **Trace restart markers**

In `restart_harness_test.go`, record child start, marker observed, SIGKILL sent, child exit, restart start, and restart result.

- [ ] **Verification**

Run:

```bash
just test ./internal/oracle -run TestTrace -v
just test ./internal/oracle -run TestOracle_DefaultLifecycle -count=1 -v
```

Expected: PASS and test log includes trace path or digest.

- [ ] **Commit**

```bash
git add internal/oracle
git commit -m "oracle: record canonical run traces" -m "Refs #$ISSUE"
```

---

## Phase 3: Make Oracle-Observed Simulator Inputs Stable

**Execution status:** Complete via issue #18 and commit `19063c1`.

**Issue title:** `simulator: make oracle-visible event timestamps deterministic`

Actual tracking issue used: #18,
`simulator: use logical clock for account event time`.

**Rationale:** Avoidable nondeterminism makes traces harder to compare. Before
commit `19063c1`, simulator account events used wall clock while commits/syncs
used logical rev time.

**What it buys:** More stable traces and wire/event bytes for account events without introducing a broad production clock abstraction.

**Files:**
- Modify: `internal/simulator/world/accounts.go`
- Modify: `internal/simulator/world/accounts_test.go`
- Possibly modify: `internal/simulator/world/logical_clock.go`
- Possibly modify: `internal/oracle/harness_test.go`

### Task 3.1: Move Account Events To Logical Time

- [x] **Write failing test**

Add a test that two worlds with the same seed generate identical account-delete frames, including `Time`.

- [x] **Implement logical account event time**

Use the simulator logical clock for `GenerateAccountDeleteForTest` rather than `time.Now`. If account status events do not need to advance repo rev, add a dedicated logical event-time helper that persists monotonic time without conflicting with `nextRev`.

- [x] **Run tests**

```bash
just test ./internal/simulator/world -run 'Account|Logical|Delete' -v
just test ./internal/oracle -run TestOracle_DefaultLifecycle -count=1
```

Expected: PASS.

- [x] **Commit**

```bash
git add internal/simulator/world internal/oracle
git commit -m "simulator: make account event timestamps deterministic" -m "Refs #$ISSUE"
```

---

## Phase 4: Add Event-Log Equivalence For Live Events

**Execution status:** Complete via issues #13-#17 and commits `cec8c3c`,
`042724f`, `4ce97e3`, `4451aca`, and `8fe6e20`.

**Issue title:** `oracle: compare live event log against simulator firehose`

**Rationale:** Final-state comparison can miss intermediate event loss. Live simulator firehose seqs provide a natural expected stream.

**What it buys:** Detects missing intermediate events, reconnect/cursor mistakes, and shutdown flush bugs even when final state converges.

**Files:**
- Create: `internal/oracle/eventlog.go`
- Create: `internal/oracle/eventlog_test.go`
- Modify: `internal/oracle/model.go`
- Modify: `internal/oracle/harness_test.go`
- Possibly modify: `internal/simulator/world/account_view.go` or add exported firehose iterator

### Task 4.1: Define Normalized Event Shape

- [ ] **Write tests for normalization**

Create tests that normalize create/update/delete/account/sync events from `ObservedEvent` to:

- upstream cursor when available;
- Jetstream seq;
- kind;
- DID;
- collection;
- rkey;
- rev;
- payload hash.

- [ ] **Implement normalized event type**

Keep this local to `internal/oracle`. Do not alter production `segment.Event`.

### Task 4.2: Build Expected Live Log From Simulator

- [ ] **Expose a simulator firehose range for tests if needed**

If existing `FirehoseRange(cursor, limit)` is enough, use it. Otherwise add a test-oriented iterator that returns seq alongside frame bytes.

- [ ] **Decode simulator firehose frames into normalized expected events**

Use the same frame decoding path as tests already use where possible. Avoid duplicating protocol parsing loosely.

### Task 4.3: Compare Expected And Observed

- [ ] **Implement `CompareEventLog`**

First version should focus on live-generated events where `ObservedEvent.UpstreamRelayCursor` or equivalent trace data can match simulator seq. If `ObservedEvent` needs upstream cursor, add that field and populate it from segments/replay observers.

- [ ] **Add converged-final-state failing test**

Construct an expected log with create/update/update and an observed log missing the middle update but final payload matching. `Compare` should pass and `CompareEventLog` should fail.

- [ ] **Wire into default lifecycle after steady-state**

Run event-log comparison after steady append ack and before shutdown, then again after shutdown flush.

- [ ] **Verification**

```bash
just test ./internal/oracle -run 'TestCompareEventLog|TestOracle_DefaultLifecycle' -count=1 -v
```

Expected: PASS.

- [ ] **Commit**

```bash
git add internal/oracle internal/simulator/world
git commit -m "oracle: compare live event log against simulator firehose" -m "Refs #$ISSUE"
```

---

## Phase 5: Promote `/subscribe` Replay To A Primary Oracle Observation

**Issue title:** `oracle: compare subscribe replay against archive observations`

**Rationale:** The product is replay. Filesystem observation alone does not validate cursor semantics, JSON encoding, hot/cold handoff, or serving behavior.

**What it buys:** Catches product-path regressions and several read-path blind spots that storage-only observation cannot see.

**Files:**
- Modify: `internal/oracle/subscribe_replay_test.go`
- Create: `internal/oracle/replay_observer.go`
- Create: `internal/oracle/replay_observer_test.go`
- Modify: `internal/oracle/harness_test.go`

### Scope And Guidance

- Turn `collectSubscribeReplay` into a reusable observer that returns normalized events and a detailed failure report.
- Compare replay from cursor `0` to filesystem-observed events after after-merge and after steady shutdown.
- Add cursor-boundary cases around:
  block start, block end, segment start, segment end, compaction watermark.
- Keep direct filesystem observation. The point is to compare surfaces, not to replace one with the other.

### Verification

```bash
just test ./internal/oracle -run 'TestReplayObserver|TestOracle_DefaultLifecycle' -count=1 -v
```

Run targeted mutants after implementation:

```bash
just mutation-campaign m015
just mutation-campaign m016
```

If they still survive, update `testing/mutation/RESULTS.md` with the specific remaining blind spot.

---

## Phase 6: Add XRPC Segment Egress Oracle

**Issue title:** `oracle: validate XRPC segment egress`

**Rationale:** Backfill clients download segment files through public XRPC handlers. Correct files on disk are insufficient if manifest, headers, cache, or serving paths are wrong.

**What it buys:** Validates the archive download contract and cache invalidation after compaction.

**Files:**
- Create: `internal/oracle/xrpc_observer.go`
- Create: `internal/oracle/xrpc_observer_test.go`
- Modify: `internal/oracle/harness_test.go`
- Reference: `internal/xrpcapi`

### Scope And Guidance

- Use the public listener returned by `Runtime.PublicAddr`.
- Fetch segment list through public XRPC.
- Download every eligible sealed segment.
- Decode downloaded bytes with `segment.Open` from a temp file or an added byte-reader helper if one exists.
- Compare XRPC-observed events to filesystem-observed events.
- Include segment index, byte length, max seq, and checksum in trace.

### Verification

```bash
just test ./internal/oracle -run 'TestXRPCObserver|TestOracle_DefaultLifecycle' -count=1 -v
```

Add a focused compaction test where a segment is rewritten and the XRPC observer sees the rewritten version.

---

## Phase 7: Add Random-Time Restart Kill Loop

**Issue title:** `oracle: add random-time restart kill loop`

**Rationale:** Enumerated crashpoints cover seams we anticipated. Random process death explores crash timing between named checkpoints.

**What it buys:** Better coverage of merge cursor, mid-commit, compaction, manifest, and shutdown durability windows.

**Files:**
- Modify: `internal/oracle/restart_harness_test.go`
- Create: `internal/oracle/restart_random_test.go`
- Modify: `internal/oracle/config.go`
- Modify: `justfile`

### Scope And Guidance

- Add an opt-in test, not default short mode.
- Parent starts child with a seeded scenario.
- Kill decision should support:
  trace event count;
  phase predicate;
  wall-clock fallback.
- After restart, run storage, compaction, event-log, and replay checks.
- Record kill decision in trace.

### Verification

```bash
just test-long ./internal/oracle -run TestOracle_RandomRestart -v
```

Add or refresh a mutation that models `m003` and require this tier to kill it.

---

## Phase 8: Add Store-Fault Oracle Tier

**Issue title:** `oracle: inject metadata store failures`

**Rationale:** Some dangerous bugs only happen when local persistence fails. The current oracle cannot make those paths run.

**What it buys:** Tests "crash loud, do not corrupt" around repo status, seq, cursor, syncstate, compaction watermark, and manifest persistence.

**Files:**
- Create: `internal/oracle/store_fault.go`
- Create: `internal/oracle/store_fault_test.go`
- Modify high-risk call sites only as needed after design review
- Reference: `internal/store`

### Scope And Guidance

- Start with one boundary: compaction watermark write or merge source-complete commit.
- Faults must be deterministic by operation name and ordinal.
- The first implementation may wrap specific store calls rather than generalizing every store operation.
- Record injected and consumed faults in trace.
- Assertions must distinguish expected abort from corruption.

### Verification

```bash
just test ./internal/oracle -run 'TestStoreFault|TestOracle_StoreFault' -v
just mutation-campaign m006
```

Expected: targeted store-fault scenario catches swallowed persistence errors.

---

## Phase 9: Expand Simulator Fidelity

**Issue title:** `simulator: add oracle fidelity event modes`

**Rationale:** The simulator should exercise more production-shaped upstream behavior without making default mode noisy.

**What it buys:** Coverage for identity, account status, malformed input, oversized/drop behavior, unknown lexicons, partial CARs, and sequence anomalies.

**Files:**
- Modify: `internal/simulator/world`
- Modify: `internal/simulator/http`
- Modify: `internal/ingest/live`
- Modify: `internal/oracle/config.go`
- Modify: `internal/oracle/harness_test.go`

### Scope And Guidance

Implement in small sub-issues. Do not do every fidelity case at once.

Recommended order:

1. identity events;
2. account statuses beyond deleted;
3. oversized fields that must be dropped and counted;
4. unknown lexicons;
5. missing CAR blocks and malformed bounded frames;
6. seq gaps/duplicates in adversarial modes.

For each case, define expected behavior before implementation:

- archived or dropped;
- cursor advanced or not;
- metric/trace event required;
- process must not crash.

### Verification

Run focused unit tests plus:

```bash
just test ./internal/oracle -run TestOracle_DefaultLifecycle -count=1
JETSTREAM_ORACLE_MODE=stress just test ./internal/oracle -run TestOracle_DefaultLifecycle -count=1
```

Add mutants or fixtures for each new fidelity path.

---

## Phase 10: Add Real-Data Corpus

**Issue title:** `oracle: add real atproto corpus checks`

**Rationale:** The simulator and Jetstream share `atmos`. Real production bytes are the practical way to catch shared-library closed-loop assumptions.

**What it buys:** Independent coverage against real relay/PDS encodings and historically observed malformed data.

**Files:**
- Create: `testing/corpus/atproto/`
- Create: `internal/oracle/corpus_test.go`
- Possibly create: `internal/oracle/corpusdata/README.md`

### Scope And Guidance

- Start with a tiny public-safe corpus.
- Store payload hashes and normalized expected events when raw data cannot be committed.
- Keep large/private corpus fetches out-of-band and documented.
- Run corpus outside the lifecycle oracle.

### Verification

```bash
just test ./internal/oracle -run TestOracleCorpus -v
```

CI should run the small corpus. Larger corpus can be nightly/manual.

---

## Phase 11: Add Long-Horizon Soak Mode

**Issue title:** `oracle: add long-horizon soak mode`

**Rationale:** Single lifecycle runs miss accumulation bugs: watermark drift, manifest/cache growth, tombstone leaks, goroutine leaks, and repeated restart effects.

**What it buys:** Operational confidence for a mission-critical long-lived daemon.

**Files:**
- Create: `internal/oracle/soak_test.go`
- Modify: `internal/oracle/config.go`
- Modify: `justfile`

### Scope And Guidance

- Keep out of default CI.
- Add `just oracle-soak`.
- Run multiple steady epochs with compaction and replay checks after each.
- Optionally combine with random restart after Phase 7.
- Emit periodic trace checkpoints and metrics snapshots.

### Verification

```bash
just oracle-soak
```

Expected: bounded runtime documented in the issue and test logs. Every failure must produce trace artifacts.

---

## Phase 12: Determinism Experiments

**Issue title:** `oracle: prototype deterministic harness subsets`

**Rationale:** Fake transport and `synctest` can reduce flakes, but only after assertion power and traceability are strong enough to justify the complexity.

**What it buys:** Faster and more reproducible logical subtests without weakening real durability/replay tiers.

**Files:**
- Create experimental files under `internal/oracle` or `internal/simulator/http`
- Do not alter restart durability tests to use fake storage

### Scope And Guidance

Prototype narrowly:

- HTTP `RoundTripper` for listRepos/getRepo/PLC;
- fake fault body shapes;
- one `testing/synctest` test around a small no-real-I/O concurrent unit.

Do not replace public `/subscribe` or restart tests with fake I/O.

### Verification

The phase is successful only if it produces a short result document or issue comment answering:

- what became deterministic;
- what remained nondeterministic;
- runtime impact;
- code churn;
- whether to adopt, revise, or abandon.

---

## Milestone Verification Gates

### Gate A: After Phases 1-4

Run:

```bash
just test ./internal/oracle
just mutation-campaign m018
just mutation-campaign m019
```

Expected: all oracle tests pass and active targeted mutants are killed.

### Gate B: After Phases 5-6

Run:

```bash
just test ./internal/oracle -run TestOracle_DefaultLifecycle -count=1 -v
just mutation-campaign m015
just mutation-campaign m016
```

Expected: replay and XRPC observers agree with filesystem observation; footer/bloom mutants are killed or specifically reclassified.

### Gate C: After Phases 7-8

Run:

```bash
just test-long ./internal/oracle -run 'TestOracle_RandomRestart|TestOracle_StoreFault' -v
just mutation-campaign m003
just mutation-campaign m006
```

Expected: restart and persistence-fault mutants are killed or have documented dispositions.

### Gate D: Before Treating The Upgraded Oracle As Release-Blocking

Run:

```bash
just test ./internal/oracle
just oracle
just mutation-campaign
```

Expected:

- no stale mutants;
- every survivor has a written disposition in `testing/mutation/RESULTS.md`;
- failure traces are generated on intentionally failing tests;
- issue comments summarize any residual risk.

---

## Final Documentation Updates

After each milestone:

- [ ] Update `internal/oracle/ROBUSTNESS_ROADMAP.md` if the architecture changes.
- [ ] Update `ORACLE_TODO.md` to remove completed items and link new issues.
- [ ] Append mutation-campaign results to `testing/mutation/RESULTS.md`.
- [ ] Comment on the GitHub issue with test commands and results.

Do not let roadmap, issues, and mutation scorecard diverge. The durable worklog is part of the oracle.
