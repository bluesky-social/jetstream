# Grafana dashboard

`jetstream.json` is the standard operational dashboard shipped with jetstream.
It covers the whole lifecycle — bootstrap/backfill, merge/cutover, and steady
state — plus process/runtime health, from a Prometheus scrape of the jetstream
binary. One dashboard serves both storage modes: local (`JETSTREAM_STORAGE=local`,
one process with a data dir) and disaggregated (`JETSTREAM_STORAGE=disaggregated`,
PostgreSQL + S3 with one leader pod and any number of followers).

## Prerequisites

- Prometheus scraping the **debug listener's** `/metrics` endpoint on every
  jetstream process (in disaggregated mode, every pod). The debug
  listener is bound with `--debug-addr` / `JETSTREAM_DEBUG_ADDR`
  (conventionally `:6060`) and is disabled when unset. The public listener
  does not expose metrics.
- Grafana 10.4 or newer (the JSON declares `schemaVersion` 39).

Example scrape config:

```yaml
scrape_configs:
  - job_name: jetstream
    static_configs:
      - targets: ['jetstream-host:6060']
```

## Naming instances

The `Instance` dropdown is driven by the Prometheus `instance` label, which
defaults to the scrape target's address (with Kubernetes service discovery,
a pod IP). Override it at scrape time to get human-readable names — every
metric, alert, and dashboard then agrees on the friendly name:

```yaml
scrape_configs:
  - job_name: jetstream
    static_configs:
      - targets: ['10.0.1.5:6060']
        labels: { instance: pop1 }
      - targets: ['10.0.2.5:6060']
        labels: { instance: pop2 }
```

With Kubernetes service discovery, use a relabel instead (`instance` is only
defaulted from the address when nothing else set it):

```yaml
relabel_configs:
  - target_label: instance
    replacement: pop1        # one value per cluster/site scrape config
  - source_labels: [__meta_kubernetes_pod_name]
    target_label: pod
```

`instance` names a **deployment**, not a process. A disaggregated deployment
is several pods that share one archive, so give them all the same `instance`
and tell them apart with a `pod` label. The `Pod` dropdown and the per-pod
panels read it; a single-process deployment without a `pod` label still works.
Don't derive `instance` from the pod name: the overview would then treat each
pod as its own deployment.

## Importing

Grafana → Dashboards → New → Import → upload `jetstream.json` (or paste it),
then pick your Prometheus data source. Everything is parameterized: there are
no hardcoded datasource UIDs, and the `job`/`instance`/`pod` variables let one
dashboard cover several jetstream deployments from the same Prometheus.

For provisioned setups, drop the file into a [dashboard
provider](https://grafana.com/docs/grafana/latest/administration/provisioning/#dashboards)
directory instead.

## Layout

| Row | What it answers |
|---|---|
| Overview | storage mode, phase, leader, build, event rates, upstream freshness, visibility latency, catalog lag, integrity, memory and disk headroom |
| Live firehose | relay intake, seq gaps (upstream loss), reconnects, verifier failures and throttled resyncs |
| Ingest writer & segments | append/drop rates, blocks and segments, seal latency, per-pod readable log (both modes) |
| Leadership & catalog · disaggregated | who leads, why sessions ended, lease/fence errors, event visibility latency, follower lag and refresh phases, readable-log feed lag and throughput |
| Hot path & maintainer · disaggregated | hot batch commits, admission backpressure, unfolded backlog, folds/seals, bootstrap direct writer |
| PostgreSQL · disaggregated | transactions by kind (rate, p99, time, errors), connection pool use and waits |
| Object store & GC · disaggregated | S3 requests, latency, bytes, errors and corruption; object states, GC, object cache |
| Backfill & retry | initial backfill progress, pending-pass retry queue, steady-state failed-repo retry |
| Serving | /subscribe clients, egress, cursor modes, getBlock, HTTP latency |
| Storage | compaction, tombstones, manifest, rewrite I/O |
| Store & integrity | pebble op latency (local) and every should-be-zero error counter (both modes) |
| Cutover & merge *(collapsed)* | phase transitions, merge rev filter, state durations |
| Process & Go runtime | per-pod CPU, memory, goroutines, FDs, GC pauses, network I/O |

Some panels legitimately show "No data": rows marked *disaggregated* and
panels marked *local* in the other mode, and panels for phases the process
isn't in (e.g. merge counters in steady state). Where both modes have an
equivalent (blocks, segment size, seal latency), one panel carries both and
each mode fills in its own series. Orchestrator phase transitions and writer
session starts (leader elections) are surfaced as annotations across all
panels.

### Leader-owned gauges

In disaggregated mode only the leader runs the writer, live consumer,
maintainer, backfill and compaction, but a follower keeps exporting the last
value those gauges had when it last led. Panels that read them (phase,
upstream freshness, backfill progress, hot backlog, tombstone set, merge
progress, …) therefore keep only the series from the pod where
`jetstream_leader_is_leader == 1`. A local process always reports itself as
leader, so the same queries work there. Keep that filter when adding panels
over such gauges.

## Editing

Edit in Grafana, then export via Share → Export → "Export for sharing
externally" and replace `jetstream.json` with the result. Keep the
`datasource`/`job`/`instance`/`pod` template variables intact.
