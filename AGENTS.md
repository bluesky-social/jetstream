# AGENTS.md

## Orientation

Jetstream is a full-network archive and live-streaming service for atproto. Backfill is served as HTTPS segment-file downloads; live tail is the same JSON websocket protocol as Jetstream v1.

- `README.md` covers running the app, tests, and the simulator.
- `docs/*` is for documentation that is intended to be read by humans and agents alike
- `docs/README.md` is the source of truth for the system. Read it before any non-trivial change, especially anything touching the on-disk segment format.
- This file is the team's coding conventions. It overrides anything inferred from existing code.
- `specs/*` is documentation intended to be read only by agents
- `specs/notes/*` are specs and plans documentation that catalogs our train of thought while working on tasks as we go

Agent-facing living docs, in a good reading order for getting oriented:

- `specs/architecture.md` — the high-level map: the big subsystems, how they fit, and a "where to look" table routing each topic to its authoritative source. Start here.
- `specs/invariants.md` — the short list of rules that must never break. Read before changing anything on the ingest, storage, or serve paths.
- `specs/glossary.md` — one-line definitions of the terms that show up everywhere.
- `specs/gotchas.md` — accepted limitations and hard-won lessons: things that look like bugs but are deliberate, and mistakes not worth making twice.
- `specs/client.md` — the client protocol end to end: archive negotiation, download/decode, cutover, live tail, wire compression, and the failure modes at each seam. Read before changing `internal/client`, the module-root API, or anything on the /subscribe-v2 wire contract.
- `specs/oracle.md` — the source of truth for the oracle/simulator testing rig.
- `specs/mutation.md` — how the mutation campaign measures the oracle's bug-detection power.

These summarize and route; `docs/README.md` and each package's `doc.go` remain authoritative. When a living doc disagrees with them, fix the living doc.

## Repo layout

```
cmd/
  jetstream/      main binary: serve, inspect-segment, timestamp import, version
  simulator/      local PLC + PDS + Relay on :7777
segment/          on-disk segment file format (header, blocks, footer, reader, writer, sealer); public API
internal/
  ingest/         the segment Writer (append/flush/seal, seq, readable log)
    backfill/     initial full-network backfill (listRepos + getRepo)
    live/         live firehose consumer (subscribeRepos)
    orchestrator/ ingestion lifecycle state machine + merge/cutover
    syncstate/    sync 1.1 resync bookkeeping
  subscribe/      websocket /subscribe endpoint (v1 protocol parity) + cold reader
  xrpcapi/        archive download over HTTP/XRPC (planBackfill, getSegment, getBlock)
  client/         thick Go client: archive negotiation, fold, cutover to live
  server/         HTTP listeners (public :8080, opt-in debug :6060) and middleware
  store/          pebble-backed cursor + metadata store
  manifest/       segment manifest (directory scan + self-describing headers)
  tombstone/      delete/update/account tombstone set for compaction
  timestamp/      operator timestamp-import pipeline
  importer/       import job manager
  repoexport/     reconstruct a repo CAR/MST from archived events
  identity/       DID resolution
  status/         /status endpoint collector
  diskspace/      data-dir free-space accounting
  crashpoint/     deterministic crash-injection seams (test-gated)
  simulator/      fake atproto network: world (traffic), http (PLC/PDS/relay), fanout
  oracle/         end-to-end correctness harness (see specs/oracle.md)
  corpus/         real-data corpus tests (independent of the lifecycle oracle)
  format/         shared wire/format helpers
  obs/            metrics, tracing, slog setup
  lifecycle/      graceful start/stop helpers
  jetstreamd/     process runtime wiring (options, startup, shutdown)
  version/        build version stamp
  web/            static debug UI assets
```

Atproto lexicon JSON (authoritative for XRPC and record schemas) lives at `~/go/src/github.com/bluesky-social/atproto/lexicons` on dev machines.

## Working in the codebase

The justfile is the single source of truth for build/test/lint. Prefer `just` recipes over invoking `go test` / `golangci-lint` directly so behaviour matches CI.

Frequently useful beyond what's in the README:

```sh
just test ./segment -run TestX  # one test (gotestsum forwards args after `--`)
just bench ./segment            # benchmarks
just fuzz 30s ./segment         # fuzz every Fuzz* target for 30s each
just modernize                  # apply gopls modernize rewrites
```

Oracle tests live in `internal/oracle` and compare Jetstream's durable output against a simulator model. Run them after changes to ingest, segment persistence, lifecycle/orchestrator phases, cursor handling, or restart recovery:

```sh
just test ./internal/oracle                                      # fast short-mode oracle checks
just test-long ./internal/oracle -run TestOracle_Restart -v      # non-short restart/recovery oracle
just oracle                                                      # heavier stress oracle mode
just oracle-sweep                                                # deterministic multi-seed stress sweep
```

`specs/oracle.md` is the source of truth for the oracle/simulator testing architecture: why the oracle exists, what it can and cannot prove, its current tiers, mutation-campaign discipline, and how future testing work should extend it. Read it before changing `internal/oracle`, `internal/simulator`, or the mutation campaign.

`specs/oracle/` is a failure diary: one markdown file per oracle test incident, recording the commit, repro command, analysis, root cause, and fix. Read past entries when diagnosing a new oracle flake, and add an entry (see `specs/oracle/README.md` for the convention) whenever you root-cause one.

The default `just` target intentionally runs short tests, so it does not execute non-short restart or stress oracle coverage. Use `just test-long` or the dedicated oracle recipes when the change could affect crash/restart correctness or end-to-end event fidelity.

The oracle's bug-detection power is measured by a mutation campaign. `testing/mutation/mutants/*.patch` are curated single-edit bugs ("mutants"), each documented with the production failure mode it models and a prediction of which oracle tier should catch it. `just mutation-campaign` applies them one at a time and verifies the oracle kills them; the scorecard lives in `testing/mutation/RESULTS.md`. Never apply these patches outside the driver, and never "fix" production code to match a mutant — they are deliberate bugs. Re-run the campaign after major changes to ingest, segment, or orchestrator logic; a STALE result means the underlying code moved and the mutant needs re-review and refresh.

Configuration is env-var driven (`JETSTREAM_*`). Defaults for local dev land in the committed `.env`; `just run-prod` overrides inline. Do not put secrets in `.env`.

## Observability

Use the package-level metrics/tracer rather than rolling your own. `obs.Tracer("foo")` returns a tracer namespaced under `jetstream/foo`. HTTP handlers should be wrapped with the `otelhttp` middleware in `obs.Middleware`. Logging is `slog`, with `JETSTREAM_LOG_LEVEL` and `JETSTREAM_LOG_FORMAT` (text/json) env-var overrides.

## CI

`.github/workflows/ci.yml` is heavily security-hardened. Two jobs: `lint` and `test (race)`. They run on every push to any branch.

## Task tracking

We track work as **GitHub issues in this repo** (`gh issue ...`). This is an open-source project, so issues are the public, durable worklog — they live alongside the code, not in a private tracker. The goal is a granular, persistent history: someone reading the issues months later should be able to reconstruct what was done and why.

**One issue per discrete unit of work.** A unit is something a single focused change resolves — a bug, a small feature, a refactor of one component, a doc update. If a task naturally splits into independently-committable pieces, file an issue per piece rather than one broad issue. Prefer too granular over too coarse.

**File the issue before starting the work**, so the issue number is available to reference in the branch and commits. Skip issue-filing only for trivial, in-the-moment fixes (typos, formatting) that ship in an unrelated commit.

**Title** — imperative and subsystem-scoped, mirroring our commit style: `ingest: dedupe overlapping live/backfill events`, `segment: validate footer CRC on open`. Lead with the affected area from the repo layout (`ingest`, `segment`, `subscribe`, `store`, …).

**Body** — keep it tight but self-contained:
- *Context* — what's wrong or missing, and why it matters (link `docs/README.md` sections or code with `path:line` when relevant).
- *Definition of done* — the observable outcome that closes the issue (behaviour, test, metric).
- *Notes* — open questions, alternatives considered, or follow-ups to split out later (kaizen: record out-of-scope problems as their own issues rather than scope-creeping this one).

**Labels** — use the existing labels; don't invent a taxonomy without discussion.

**Status & linking** — the worklog lives in the issue:
- Post a comment when meaningfully starting or when state changes (blocked, approach changed, finding). These comments are the persistent log — favour a short comment over silence.
- Close issues *through commits/PRs*, never by hand: put `Closes #N` (or `Fixes #N`) in the commit body or PR description so the link is permanent and the issue auto-closes on merge to the default branch.
- Reference related issues with `#N` to build the graph; split discovered follow-up work into new issues and link them.

**Recipes:**

```sh
gh issue create -t "segment: validate footer CRC on open" -b "$(cat <<'EOF'
## Context
...

## Definition of done
...
EOF
)" -l bug
gh issue list --state open                  # current worklog
gh issue comment <N> -b "Starting: approach is ..."
gh issue view <N> --comments                # full history of one unit
# closing happens via "Closes #<N>" in the commit/PR, not `gh issue close`
```

## Practices

- **Testing.** Be liberal with the right kind of test for the job:
    - Unit tests sparingly — limited utility in this codebase, but useful for very small code paths.
    - Integration tests for happy paths.
    - Fuzz and property-based tests for untrusted input and edge cases that violate invariants.
    - Swarm tests for meaningful randomness (not white-noise flakes).
    - Smoke tests against real production occasionally.
    - Oracle tests that run a seeded simulator that validate data correctness at various parts of the server lifecycle
    - Tests must be fast. If a package's full test suite takes >1s, question it and try to bring it under a second.
- **Observability over logging.** Minimal stdout/stderr. Instrument with Prometheus metrics and OTEL traces liberally.
- **Local dev simplicity.** The justfile is the UX. CI mirrors it as closely as possible.
- **Few dependencies.** Only the whitelist below; question additions:
    - `github.com/jcalabro/atmos`, `gloom`, `gt`, `jttp`
    - `github.com/urfave/cli` v3
    - `github.com/zeebo/xxh3`
    - `github.com/coder/websocket`
    - `github.com/stretchr/testify`
    - `github.com/klauspost/compress`
    - `github.com/prometheus/client_golang`
    - `go.opentelemetry.io/otel` and related
    - `github.com/puzpuzpuz/xsync`
    - anything under `golang.org/x`
- **Follow existing conventions.** Don't introduce new patterns when the codebase already has one for code style, error handling, or logging.
- **Comments explain why, not what.** Exported symbols and packages get a high-level docstring; otherwise comment only when the reasoning isn't obvious from the code.
- **Never crash, and never corrupt data.** The process is a mission-critical, long-lived server daemon. Add observability in the case of incorrect/adversarial user input, but don't crash.
    - Treat all upstream relay/firehose/backfill data as user input. Invalid external records must not abort, stop, exit, or crash the server.
    - If upstream record data cannot be represented safely in Jetstream's internal format (for example a field exceeds a segment column width), drop that record or event, increment a warning/error metric, and log bounded diagnostic fields. Do not silently truncate or coerce it.
    - Preserve crash-loud behavior for invalid internal state, persistence corruption, fsync/store failures, and other conditions where continuing could corrupt Jetstream-owned data.

## Which checks to run for which change

`just` is the source of truth for build/test/lint (see "Working in the codebase" above for the less-obvious recipes). Beyond the default `just` (lint + short tests), match the change to the extra coverage it needs:

| If you changed… | Also run |
|---|---|
| anything | `just` (lint + short tests) — always |
| segment format, ingest, orchestrator, cursor, or restart/recovery | `just test-long ./internal/oracle`, `just oracle-sweep`, and the relevant fuzz targets (`just fuzz 30s ./segment`) — short tests skip the crash/restart and stress oracle coverage |
| the oracle, simulator, or mutation catalog | `just test ./internal/oracle`, then re-run the mutation campaign (`just mutation-campaign`) and check the scorecard didn't regress |
| anything that could move a mutant's target code | `just mutation-gate` — but on a **clean working tree**: the driver applies/reverts patches with git and aborts on a dirty tree (see `specs/gotchas.md`) |
| parsing of untrusted input (frames, CARs, CSV) | the matching fuzz target (`just fuzz`), plus a corpus check if `internal/corpus` is affected |
| lexicon-derived code | `just lexgen` and confirm no drift |
| hot-path code (segment writer/sealer) | `just bench ./segment` and compare against the baseline |

When in doubt, `specs/oracle.md` explains which tier catches which class of bug.

## Memory promotion

Any per-session or per-machine memory (an agent's private scratchpad) is exactly that — a scratchpad. It is invisible to the next agent and to Jim. **If a fact is worth knowing in a second session, promote it to a durable, checked-in home in the same PR that discovered it:**

- an accepted limitation or a "we tried X, don't do it again" lesson → `specs/gotchas.md`
- an oracle failure you root-caused → a `specs/oracle/` diary entry
- a rule others must follow → this file
- behavior of the system → the relevant `doc.go` (or note it for `docs/README.md`, which Jim owns)

The test: if forgetting it would let a future agent re-introduce a bug or re-litigate a settled decision, it doesn't belong only in memory.
