# Grafana dashboard

`jetstream.json` is the standard operational dashboard shipped with jetstream.
It covers the whole lifecycle — bootstrap/backfill, merge/cutover, steady
state, and timestamp import — plus process/runtime health, from a single
Prometheus scrape of the jetstream binary.

## Prerequisites

- Prometheus scraping the **debug listener's** `/metrics` endpoint. The debug
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
  # …or derive it from pod metadata:
  - source_labels: [__meta_kubernetes_pod_name]
    target_label: instance
```

## Importing

Grafana → Dashboards → New → Import → upload `jetstream.json` (or paste it),
then pick your Prometheus data source. Everything is parameterized: there are
no hardcoded datasource UIDs, and the `job`/`instance` variables let one
dashboard cover several jetstream deployments from the same Prometheus.

For provisioned setups, drop the file into a [dashboard
provider](https://grafana.com/docs/grafana/latest/administration/provisioning/#dashboards)
directory instead.

## Layout

| Row | What it answers |
|---|---|
| Overview | phase, build, event rates, upstream freshness, disk headroom |
| Live firehose | relay intake, seq gaps (upstream loss), reconnects, verifier |
| Ingest writer & segments | append/drop rates, block flushes, seal latency, readable log |
| Backfill & retry | initial backfill progress + steady-state failed-repo retry |
| Serving | /subscribe clients, egress, cursor modes, getBlock, HTTP latency |
| Storage | compaction, tombstones, manifest, rewrite I/O |
| Store & integrity | pebble op latency and every should-be-zero error counter |
| Cutover & merge *(collapsed)* | phase transitions, merge rev filter, state durations |
| Timestamp import *(collapsed)* | import phases, row throughput, reject reasons |
| Process & Go runtime | CPU, memory, goroutines, FDs, GC pauses, network I/O |

Panels for phases the process isn't in (e.g. the import row while idle, merge
counters in steady state) legitimately show "No data" — series appear when the
phase runs. Orchestrator phase transitions are also surfaced as dashboard
annotations across all panels.

## Editing

Edit in Grafana, then export via Share → Export → "Export for sharing
externally" and replace `jetstream.json` with the result. Keep the
`datasource`/`job`/`instance` template variables intact.
