# Status Dashboard Host And Account Diagnostics Design

## Overview

Jetstream's public `/status` page will become a dense, tabbed operator dashboard for understanding backfill, merge, steady-state ingest, host health, account state, and collection distribution. The page stays server-rendered with Go `html/template`, CSS, and minimal dependency-free browser JavaScript for local table filtering/tab affordances. There is no npm, TypeScript, bundler, or client-side framework.

All tabs are public. This is intentional: operators should be able to send host and account diagnostic links to PDS operators in the open ecosystem.

The first implementation should preserve the current `/status` page's visual language: dark/light theme support, narrow readable layout, dense tables, restrained styling, and high signal above the fold. The browser mockup in the brainstorming session is the visual baseline: the current page evolved into tabs, not a separate web app.

## Routes And UI Contract

`/status` remains the only user-facing status page. Tabs are addressable through query parameters:

- `/status` and `/status?tab=summary`
- `/status?tab=hosts`
- `/status?tab=accounts&did=did:plc:...`
- `/status?tab=accounts&handle=example.com`
- `/status?tab=collections`

The summary tab shows most of the current status data: phase, backfill counts, live ingest cursors, network totals, segment health, cursor lookback, metadata store summary, and the top failing host buckets.

The hosts tab renders all known host buckets from maintained Pebble aggregates. It defaults to failing buckets first, sorted by failed repo count descending, then total repo count descending. It includes concise columns: host bucket, total repos, active repos, complete repos, failed repos, last attempted time, representative error, and a compact view of the latest bounded error samples. Client-side filtering may hide/show already-rendered rows, but page correctness must not depend on JavaScript.

The accounts tab performs one account lookup at a time. Lookup by DID is authoritative. Lookup by handle uses a local declared-handle index only; `/status` must not perform network identity resolution. Account results should show DID, declared handle, PDS host bucket, active flag, backfill status, attempts, last attempted time, last error, backfill rev, latest rev, updated time, record/byte counters when available, and related host aggregate context.

The collections tab moves the existing collection table out of the summary tab. It remains dense and table-oriented.

## Metadata Model

Extend the existing Pebble metadata model without adding a new dependency.

`repo/<did>` remains the canonical per-account row. Extend `RepoStatus` with fields for:

- `handle`: declared handle from the DID document's `alsoKnownAs`, normalized when parseable
- `pds`: original PDS endpoint URL, when valid
- `host`: normalized host bucket
- `last_attempted_at`: wall-clock time when Jetstream recorded the latest repo download outcome. With the current atmos callback surface, this is the completion time of the success path or final failed attempt, not the start time of each individual retry attempt.
- existing and current backfill failure metadata: status, attempts, latest error, started/completed timestamps

Add a declared-handle index:

```text
handle/<normalized-handle> -> did
```

This index is best-effort operator convenience. It is not a verified handle authority. If a DID's declared handle changes, the old handle index entry must be removed when the previous value is known from the repo row.

Add host aggregates:

```text
host/<bucket> -> JSON<HostStatus>
```

`bucket` is either a normalized PDS hostname or a synthetic pre-host bucket:

- `unresolved.did`: DID resolution failed before a PDS endpoint was known
- `invalid-pds`: DID document resolved but did not contain a usable `AtprotoPersonalDataServer` endpoint

There is no relay fallback bucket in this design.

`HostStatus` should include enough data to render the hosts tab cheaply:

- host bucket string
- total repo count
- active repo count
- backfill status counts: not started, complete, failed
- last attempted time
- latest representative error string
- grouped error counts by coarse error class
- latest 5 error samples, newest first, each with DID, attempted time, error class, and truncated error string

Error strings stored in Pebble should be bounded to a fixed byte length to prevent unbounded metadata growth.

## Update Flow

Normal `/status` collection must not scan all `repo/` rows. Production status snapshots already use maintained counts and the in-memory manifest; host diagnostics should follow the same pattern.

Add a Jetstream-owned resolver wrapper around the atmos `identity.Resolver` used by backfill. On `ResolveDID`:

- If resolution fails, record the DID's host bucket as `unresolved.did` and leave handle/PDS empty.
- If resolution succeeds, parse the DID document for declared handle and PDS endpoint.
- If the PDS endpoint is usable, record the original endpoint and normalized host bucket.
- If the PDS endpoint is missing or unusable, record the bucket as `invalid-pds`.

Backfill store transitions update repo rows, handle index rows, global backfill counts, and host aggregate rows. Where the existing code already commits repo status and aggregate counts together, keep the same durability posture and batch related metadata updates together. If a transition moves a repo between host buckets or statuses, decrement the old bucket/status counters and increment the new bucket/status counters.

On download failure, update:

- `repo/<did>.Backfill.Status = failed`
- attempts and latest error fields
- `last_attempted_at`
- host failed count/error counters
- host latest 5 error samples

On download success, update:

- `repo/<did>.Backfill.Status = complete`
- backfill rev, latest rev, completed/updated timestamps
- latest error cleared on the repo row
- host complete/failed counts adjusted as needed

Active flips from `listRepos` update the repo row and host active count.

## Error Classification

The dashboard should group errors enough to be operationally useful without pretending to know root cause. Initial classes:

- DID resolution failure
- invalid or missing PDS endpoint
- HTTP status from getRepo, including 429 and 5xx
- timeout or context deadline
- CAR parse failure
- commit verification failure
- local write or flush failure
- unknown

The raw latest error remains visible, truncated, and HTML-escaped.

## Performance And Operational Constraints

The summary and hosts tabs must be cheap on production-sized metadata stores. The hosts tab reads `host/` aggregate rows, not all `repo/` rows. Account lookup reads one `repo/<did>` row, or one `handle/<handle>` row followed by one `repo/<did>` row.

Host aggregate updates add write amplification to the backfill path. Keep the per-transition work bounded: one repo row, maybe two handle index rows, one or two host aggregate rows, and the existing aggregate count row. Do not add unbounded logs or per-attempt history.

The latest 5 host error samples are a ring or bounded slice stored in the `HostStatus` JSON. They are diagnostic samples, not a complete audit log.

## Testing

Add focused tests for:

- host bucket normalization and synthetic bucket assignment
- repo status transitions updating host aggregate counters correctly
- failed -> complete retry success moving counts out of failed
- active flips updating host active counts
- latest 5 host error samples staying bounded and ordered newest first
- declared-handle index create, update, delete, and lookup behavior
- account lookup by DID and by local declared handle
- template rendering for each tab
- HTML escaping for host names, handles, DIDs, and error strings
- manifest-backed `/status` path avoiding full `repo/` scans

When implementation touches backfill persistence, run the relevant backfill/status/web tests and the oracle coverage appropriate to lifecycle or recovery changes.
