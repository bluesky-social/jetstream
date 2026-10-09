# Migrating a local archive to disaggregated storage

This runbook moves a running local-mode Jetstream (segment files and Pebble on
one volume) to disaggregated storage (PostgreSQL and an S3-compatible object
store) with no read downtime and no data loss. Seqs, sealed segment bytes, and
ETags carry over unchanged, so client cursors and downloads keep working.
Ingest pauses once, during the handoff, for a few seconds to about a minute;
the upstream relay buffers in the meantime, so events arrive late, never lost.

The design and its reasoning are in
`specs/notes/2026-10-09-local-to-disagg-migration.md`.

## How it works

The local process replicates its own archive. Started with
`--migrate-to-disaggregated`, it runs a migrator alongside normal ingest and
serving:

1. **Seeding.** It copies every sealed segment, byte for byte, into the object
   store and the catalog, and copies its metadata. It holds the disaggregated
   writer lease, and every catalog write is fenced.
2. **Tailing.** It ships each new durable block, each seal, and each metadata
   change as local ingest produces them. The catalog is always a consistent
   prefix of the local archive. Disaggregated pods can serve it read-only, but
   none can lead it.
3. **Handoff.** On request, it stops local ingest, ships what is left,
   verifies, and commits `migration/state = done`. A pod then takes the lease
   and continues ingest from exactly where the local process stopped. The
   local process enters drained mode: it serves archive downloads from its
   files but refuses new live subscribers.

Local compaction is paused from the start of the migration until it ends, so
the sealed segments the catalog holds stay identical to the local files.

## Before you start

- **A PostgreSQL database and a bucket prefix** for the new archive, reachable
  from the local process and from the pods, with the credentials disaggregated
  mode needs (`jetstream serve --help`, "Disaggregated storage").
- **Object-store capacity** for the whole archive, plus headroom for
  compaction and GC once the pods take over.
- **Local volume headroom.** Compaction is paused for the whole migration, so
  deletes and updates accumulate. `JETSTREAM_MIGRATION_MAX_COMPACTION_PAUSE`
  (default 5 days) bounds how long the handoff may wait.
- **The relay's replay window.** The emergency rollback (below) re-ingests
  from the handoff-time relay cursor, which works only while the relay still
  holds it.
- **A deploy freeze on the local process** from the handoff until you are
  satisfied with the pods. Restarts during seeding and tailing are fine: the
  migration resumes from what the catalog holds.

## 1. Create the archive

With the disaggregated storage settings in the environment:

```sh
jetstream storage init --migrate-from-local
```

This creates the archive with `migration/state = seeding`, so no
disaggregated pod will lead it until the handoff. Running it again finishes an
init that failed part way.

## 2. Start the migration

Restart the local process with the same storage settings plus:

```sh
JETSTREAM_MIGRATE_TO_DISAGGREGATED=true
```

Keep `JETSTREAM_STORAGE` unset (local) and the data directory as before. The
`JETSTREAM_MIGRATION_*` settings (`jetstream serve --help`, "Migration to
disaggregated storage") tune
it. Start with the read throttle (`JETSTREAM_MIGRATION_READ_BYTES_PER_SEC`,
default 64MiB/s) and raise it while watching the volume's latency and
cold-read latency.

Watch progress with:

```sh
jetstream migrate status
```

or the local process's debug listener at `/debug/migration`, or the
`jetstream_migration_*` metrics. The useful ones:

- `segments_local` and `segments_remote`: seed progress.
- `lag_seqs`: how far the catalog trails local ingest while tailing.
- `failures_total{step}`: a migrator session that ended in error. The
  migrator retries under a new lease; local ingest and serving are never
  affected.
- `resync_diffs_total`: metadata a periodic full resync found different. It
  should stay at zero after the seed.

The state moves from `seeding` to `tailing` once every sealed segment and the
metadata are copied.

If the migrator stops with an error it cannot retry (for example, the source
holds a metadata key with no migration rule, or a sealed segment changed
behind its back), it logs it, reports it in `status`, and leaves the local
process running normally. Fix the cause and restart, or abort.

## 3. Validate the replica

Start disaggregated pods (`JETSTREAM_STORAGE=disaggregated`) behind a
non-public endpoint. While the state is `tailing` they serve the replica
read-only and stand by for the lease. Before the handoff, check:

- **Stream equality.** Subscribe to the local process and to a pod with the
  same cursor, and compare `(seq, payload)` frame by frame, both live and
  replaying from well back in the archive.
- **Archive equality.** Compare `planSnapshot` responses for several filters,
  ETags for every segment, and whole-file hashes for a sample of segments,
  including every segment that contains a seq vacancy.
- **Pod readiness.** At least one pod is ready and caught up with the replica.

## 4. Hand off

1. **Flip the public routes to the pods.** Pods serve everything the local
   process does, and the local process keeps serving archive downloads
   through and after the handoff, so a client mid-download is unaffected.
2. **Request the handoff:**

   ```sh
   jetstream migrate handoff
   ```

   The migrator refuses, with the reason, unless the state is `tailing`, the
   replica trails local ingest by at most
   `JETSTREAM_MIGRATION_HANDOFF_MAX_LAG_SEQS`, a full metadata resync finished
   within `JETSTREAM_MIGRATION_HANDOFF_MAX_RESYNC_AGE`, the compaction pause is
   within its bound, and every imported segment still matches its local file.
   A refused or failed handoff restarts local ingest and keeps tailing; ask
   again once the cause is fixed.

   `JETSTREAM_MIGRATION_HANDOFF_FULL_VERIFY=true` adds a full metadata
   comparison while ingest is stopped. It is the strongest check, and it
   lengthens the ingest pause by a full read of both stores.

3. **Watch the pods.** One takes the lease, checks the catalog's invariants,
   and resumes ingest from the copied relay cursor. `jetstream migrate status`
   shows `done` and the handoff seq.

The local process is now drained. It closes its remaining live subscribers
over `JETSTREAM_MIGRATION_DRAIN_SPREAD` (default one minute); they reconnect
through the flipped routes and resume from their cursors. A restart keeps it drained: its local
handoff guard stops ingest for good. Keep it serving archive downloads until
no client could still hold a plan from it, then stop it. Keep its volume until
the rollback window you are willing to support has passed.

## Rolling back

### Before the handoff: abort

```sh
jetstream migrate abort
```

The migrator marks the catalog `aborted`, resumes local compaction, and stops.
The local process carries on as before. Drop the database and the bucket
prefix (or create new ones) before another attempt; a new attempt seeds from
scratch.

### After the handoff: reclaim (emergency only)

This undoes a finished handoff and is costly for clients. Use it only if the
pods cannot serve; prefer fixing forward.

1. Stop every disaggregated pod, and the local process.
2. With the storage settings in the environment, run on the local volume:

   ```sh
   jetstream migrate reclaim --data-dir /path/to/data
   ```

   It takes the writer lease (and refuses while a pod holds it), marks the
   catalog `reverted` so no pod can lead it again, clears the local handoff
   guard, and makes the local archive resume past every seq the pods assigned,
   plus `--margin` (default one billion). The seqs in between become a
   vacancy.
3. Start the local process without `JETSTREAM_MIGRATE_TO_DISAGGREGATED`, and
   flip the routes back.

The local process re-ingests the relay from its handoff-time cursor. Clients
holding cursors from the pod era jump over the vacancy and see those events
again under new seqs: duplicates, never losses. Segments the pods sealed after
the handoff are written again locally with different content under the same
names, so a client that pinned a plan from the pods sees new bytes.
