# Maintainability improvements — plan of record (2026-07-03)

Status: **active planning doc.** Split out of
`2026-07-02-tech-debt-paydown-ideation.md` (§0/§3/§4) after Jim's review; the
parent doc holds the full exploration evidence and decision trail. This doc is
the forward-looking plan only. Issues are linked on individual items as work is
split out.

Scope note: oracle/simulator testing work lives in
`2026-07-03-oracle-testing-improvements.md`. Explicitly out of scope per Jim:
user-facing documentation, HA mode.

## Settled decisions (do not relitigate)

- **`segment/` stays public.** Deliberate: it's a well-defined file format to
  be exposed to the world with documentation. No move to `internal/`.
- **Release engineering deferred until pre-1.0 vetting.** Product isn't ready
  for outside operators yet. The plan (below, "Deferred") stays written down
  for when it's time. The two §Fix-now bugs are correctness fixes, not
  release engineering — they don't wait.
- **Test suite: leave it alone.** No deletion/consolidation campaign; delete
  tests opportunistically during future refactors. The cohort analysis and the
  binding "do not touch" list (m022/#182 precedent) live in the parent doc §1.
- **SECURITY.md: done** (2026-07-03, copied from bluesky-social/atproto,
  committed).
- **Nix flake: yes** (new item from Jim) — dev env + CI; Dockerfile stays
  non-nix (tentative).

## Fix now (bugs found during the survey)

1. **Dockerfile version stamp silently broken.** `Dockerfile:46-48` ldflags
   target `github.com/bluesky-social/jetstream-v2/internal/version.*`; module
   is `github.com/bluesky-social/jetstream`. `-X` on a nonexistent path is
   silently ignored → every Docker build ships `Version=dev Commit=unknown`.
   Fix the path; also the stale `jetstream-v2` in the
   `internal/version/version.go` example comment and the README badge URL.
   Add a docker-build (no push) step to push CI so the Dockerfile can't drift
   again — this is what would have caught it.
2. ~~`.env` → `just run-prod` flag leak~~ — **NOT A BUG (jrc 2026-07-03):
   intentional.** `run-prod` is a local dev loop pointed at real upstream, not
   a production-config rehearsal; inheriting the dev-speed flags
   (`SKIP_MERGE_DISCOVERY`, `DISABLE_REPO_ACTION_RATE_LIMITS`, 1s status TTL)
   from `.env` is deliberate, for fast iteration. Follow-ups instead of a fix:
   (a) document the intent in a justfile comment on `run-prod` + a gotchas.md
   entry so future audits (agent or human) don't re-flag it — this survey did,
   which proves undocumented intent will keep being rediscovered as a bug;
   (b) note in the deferred release-engineering bucket that pre-1.0 vetting
   will want a faithful production-config recipe (`run-prod-faithful` or
   similar) since no recipe currently provides one.

## CI improvements (keep push CI fast; use scheduled lanes)

- **govulncheck** — nothing scans deps today. `just vuln` recipe + step in the
  scheduled workflow; egress allowlist needs `vuln.go.dev:443`.
- **Fuzz lane** — 13 targets, zero committed `testdata/fuzz/` corpora, no CI
  invocation. (a) commit regression corpora, (b) `just fuzz 60s` in the
  6-hourly scheduled job, (c) longer-term: ClusterFuzzLite/OSS-Fuzz — this
  code parses hostile network input.
- **Benchmark regression tracking** — weekly scheduled `-count=10` +
  benchstat vs a committed baseline; segment writer/sealer is the hot path of
  a full-network archive.
- **Darwin/arm64 compile check** — `segment/sync_darwin.go` has real
  behavioral divergence and never compiles in CI. Compile-only matrix step
  (`GOOS=darwin GOARCH=arm64 go build ./...`). Consider a runtime warning log
  on darwin (fsync no-op is a footgun against real data).
- **Pin the `just` version** in `extractions/setup-just` (currently
  latest-per-run: a nondeterministic tool download in otherwise-hardened CI).
- **Verify dependabot gomod PR branches trigger the push-only workflow**
  (one-time check; a silent no-CI-on-deps-bumps gap would be easy to miss).
- **Raise test job `timeout-minutes` 10 → 20** — will page spuriously before
  it pages truthfully as the suite grows.

## Local DX

- **Nix flake** (new): small flake pinning Go + tool versions so new devs get
  a stable env; CI uses the same flake so local and CI match. Open question:
  Dockerfile via nix too? Tentative call: no — keep the distroless Dockerfile;
  the flake owns the dev/CI toolchain. Revisit when building it.
- `just cover` — coverage profile + HTML; the numbers worth watching are
  `segment/` and `internal/ingest/`.
- `just profile-cpu` / `just profile-heap DURATION` — wrap the debug
  listener's pprof endpoints.
- `just docker-build` — local Dockerfile exercise (pairs with Fix-now #1).
- Optional: pre-commit hook installer in `install-tools` (`core.hooksPath`)
  running gofmt + lexgen drift — the classic fails-in-CI-10-minutes-later
  pair.

## Operational readiness (pre-production)

- **Disk-free / ENOSPC posture** — the most predictable incident for an
  append-forever archive. No Statfs anywhere; `/status` reports bytes used,
  never free.
  DECIDED (jrc 2026-07-03): **crash-loud**, same class as fsync failure per
  the AGENTS.md posture. The process crashes with a clear, prominent error
  message describing the situation (disk full, data dir path, what the
  operator should do). No read-only degraded mode for now — Jim may revisit
  later, but is deliberately not introducing new lifecycle machinery without
  fully considering it first. Recovery path is the existing (well-tested)
  torn-tail truncation on restart.
  Work: `jetstream_data_dir_free_bytes` gauge (statfs on the data dir) +
  the crash behavior + tests at EVERY level per Jim: unit/integration for the
  writer error path, AND oracle segment-I/O fault injection proving injected
  ENOSPC → clean crash → clean recovery, no corrupt tail (oracle doc, work
  item 3), plus coverage in the advanced tiers (crashpoint/mutation) as they
  apply.
- **Unrecognized-env-var rejection at startup** (#223) — a typo'd `JETSTREAM_*` var is
  silently ignored today; reject unknown prefix matches before command execution.
- **Panic posture for long-lived goroutines** — one `recover()` in the whole
  codebase; an ingest/orchestrator panic dies as a bare stderr traceback that
  log shippers mangle. Top-of-goroutine recover-log-rethrow in
  `internal/jetstreamd/runtime.go` so panic value + build info land in
  structured logs before death; document `GOTRACEBACK` guidance for operators.
- **Named freshness SLI** — upstream cursor minus last durable seq, as a
  duration: the one number a "how far behind the network" SLO hangs off.
  Define it as its own metric now so dashboards don't derive it from two
  gauges.
- **Document `/readyz` semantics** — it means "listeners bound," not "ingest
  healthy." Probably correct for a single-process service; say so in the
  handler comment.

## Format longevity

- **Write the segment-format migration policy** (docs §3.1) while v1 is the
  only version in the wild: does a v2 reader read v1 files in place (preferred
  at multi-TB scale)? Is `segment/rewrite.go` the migration vehicle? A
  decision, not code — and it constrains what the 158 reserved header bytes
  can be used for. Extra weight now that `segment/` is confirmed public API.
- **Dropped: `inspect-segment --verify` deep-check mode** — 2026-07-05
  decision (jrc): do not add this operator CLI surface. Recompute/re-derive
  footer, index, and bloom invariants in reusable segment code and oracle
  coverage instead (#208).

## Agent-docs bundle (~1 day total; full analysis in parent doc §4)

1. `specs/notes/README.md` index + status convention
   (`active` / `landed — superseded by docs §N` / `abandoned`); AGENTS.md rule:
   mark notes landed in the PR that merges the work.
2. `specs/gotchas.md` — accepted-limitations + lessons, seeded from private
   memory (single-event net-new-DID window, ReadRow quoted-newline hole,
   classifier-copies lesson, pebble Get-after-Close, mutation-campaign
   dirty-tree). Highest-value new doc: the only knowledge category with no
   shared home today.
3. Invariants hoist: "§0 Invariants" summary atop docs/README.md (seal
   immutability, fsync-then-pebble ordering, cursor inclusivity/seq-0, block
   topology across generations, crash-loud boundary), linked from AGENTS.md.
4. Three missing doc.go files (`internal/oracle` — define *bubble*;
   `internal/ingest/live`; `internal/simulator`) + canonical doc.go for
   `backfill`/`orchestrator` (replacing per-file package comments).
5. Glossary routing table in AGENTS.md (term → sentence → authoritative
   source).
6. Refresh AGENTS.md layout tree (missing 10 of 22 internal packages) + a
   ~15-line change-class → required-just-recipe table (incl. the
   mutation-campaign-needs-clean-tree gotcha).
7. AGENTS.md memory-promotion rule: auto-memory is a scratchpad; facts worth a
   second session go to gotchas/diary/docs in the same PR.

Not doing (decided): formal ADR directory; cross-package "flows" doc (highest
churn; doc.go seam descriptions carry it).

## OSS hygiene (cheap, before wider visibility)

- CONTRIBUTING.md — must explain the push-only-CI "maintainer pushes your
  branch" flow, or external contributors will assume CI is broken.
- CODEOWNERS, issue templates.
- Resolve docs/README.md §10 inline `NOTE (jrc):` comments.
- `cmd/compare`: already paid for itself (v1 differential, closed); delete
  when no longer useful, before 1.0 makes it permanent.

## Deferred: release engineering (pre-1.0, written down for later)

`release.yml` on `v*` tags (separate minimal-permission workflow, same
hardening posture): linux/{amd64,arm64} binaries + buildx multi-arch image →
GHCR; `--sbom=true --provenance=true`; binary provenance via
`actions/attest-build-provenance`; CHANGELOG.md discipline (issue-per-unit
convention already generates the raw material). One module ⇒ server and Go
client version in lockstep — document when tagging. Registry/creds ownership:
decide at implementation time.

Also for pre-1.0 vetting: a faithful production-config recipe
(`run-prod-faithful` or similar) that does NOT inherit `.env`'s dev-speed
flags — `run-prod` deliberately inherits them for fast local iteration (see
Fix-now #2), so no current recipe runs the config production will actually
use.

## Suggested sequencing

1. Fix-now bugs (hours).
2. Agent-docs bundle (~1 day, compounds for every future session).
3. govulncheck + fuzz lane + corpora (~1 day).
4. ENOSPC decision + gauge (+ hands off to oracle doc item 3).
5. Nix flake (~1 day).
6. Remaining CI items, DX recipes, OSS hygiene as fill-in work.
7. Migration policy + `--verify` mode when touching segment next.
