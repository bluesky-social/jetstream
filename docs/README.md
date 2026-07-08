# 1. Executive Summary

Jetstream v2 is full network archive and live streaming service for atproto. It is an open-source product that allows us and other atproto builders to quickly and easily gather all data on the network in order to build novel products, perform network analysis, etc.

It ingests every record from all known repos on a relay, then cuts over to the live firehose. It stores data in a custom, highly optimized columnar file format. Users then connect to the server jetstream v2 to seamlessly stream through backfilled data as well as live.

Jetstream v2 is the next evolution of Jetstream v1, with expanded capabilities and better aligned with the atproto ethos. It provides the same user-friendly JSON interface that allows for data filtering in a backwards-compatible manner, but also enables fast and easy full network backfill. It also stores the raw CBOR so each record and account is interrogatable if desired.

It is self-hostable and cheap to run. We will also provide it as a free service, transparently replacing our existing Jetstream v1 instances (i.e. same URLs, same websocket payload).

From here on out, I call Jetstream v2 simply "jetstream", and the old jetstream implementation will be called "jetstream v1".

### 1.1 Goals and Non-Goals

Jetstream v2 is designed with the following use-cases in mind:

1. Archive the whole network locally to disk in a highly compressed format
2. Use that local cache to backfill all data (or a subset) and cut over to live seamlessly
    1. This enables building AppViews quickly and in a robust manner
3. Transparently replace Jetstream v1 while providing the ability for independent parties to interrogate the validity of its data
4. Provide a CDN-friendly downloadable archive for all known accounts and events
5. Maintain a database of witness timestamps on records
6. Dead-simple and cheap for us and others to operate on a single server
    1. The machine doesn't need much CPU, but it would benefit from a fair bit of ram for initial backfill, and a reasonably large disk (a few TB)
    2. High availability is a future goal; the earlier replication design was dropped (see Section 6)

We also have the following non-goals, which are explicitly not included in this design:

1. Exactly once delivery
    1. We do at-least-once delivery and require clients to be idempotent (they already should be if they're subscribing to the existing firehose!)
2. Cryptographic proof storage
    1. Since we lay out records in order per-DID, we actually should be able to reconstruct the MST on the fly for a user
3. Query engine with arbitrary queries or point lookups
    1. This is a replay cache, not a general purpose database. We only support large range scans
4. Distributed consensus
    1. High availability will be addressed by a separate design later (see Section 6)
    2. Doing distributed consensus is quite challenging, and I want to ship a robust system quickly
    3. This isn’t a one-way door; we can add raft on segment blocks eventually. But I want to avoid ballooning the complexity of the original design so we can ship on reasonably short timelines

### 1.2 Why Build This Now?

First, our users are asking for this. 2026 is the Atmosyear! Jetstream increases atproto adoption by providing an easy, robust, and cheap way to build AppViews and do network analysis.

Second, the current production Bluesky data plane has several real limitations, all of which we will fix:

1. It's poorly tested and very challenging to run locally in a dev environment, leading to slow iteration times, bugs in production, and high operational toil
2. Scylla as a replay tool is error prone, risks production stability, and requires writing a lot of code
3. It only stores the lexicons for which we planned ahead, so we're missing interesting lexicons from our database (i.e. `standard.site`)

Finally, Jetstream v1 is a user-friendly tool and is cheap to run, but drops all ability to interrogate its data. We want the same excellent user experience of a JSON websocket that's filterable, but we also want a mechanism by which third parties can examine it for validity and completeness.

## 2. Architecture Overview

Jetstream is a single static executable that runs on a single server. High availability is deferred to a future design (Section 6). We do this for simplicity and so we can ship in a reasonable amount of time.

It completes full network backfill, transitions to the live tail seamlessly. Then, its clients to subscribe to the full network or certain data slices (similar to Jetstream v1).

It tracks each event via its own `sequence number`, a monotonic 64-bit integer assigned at ingestion time (this sequence number is also known as the `cursor`). The cursor is *inclusive*: `?cursor=N` replays starting at the event with seq N (we deliver events with seq >= cursor). Sequence numbers start at 1; seq 0 is a reserved "nothing yet" sentinel, so `?cursor=0` replays from before the first event (i.e. everything). Combined with at-least-once delivery (see the invariants below), a client resuming from its last-seen cursor re-receives that last event, so clients must be idempotent (the bundled Go client dedups by seq, re-anchoring at last-seen-seq and dropping the re-delivery). The client can use this to either backfill data starting from the beginning of time, some arbitrary point, or just start streaming the current live tip.

Same as the normal firehose, cursors are instance-local. Each jetstream instance assigns its own seq values independently, so when switching between instances, clients should rewind their cursor by a small margin and rely on at-least-once delivery to cover the overlap.

We enforce some invariants that are required for building correctly on atproto:

1. No data loss, even in the face of crashes, network weather, etc.
    - We ensure the cursor of the firehose to which we're subscribed that's durably written to disk always matches or is older than the events that have been written
    - Note: it's okay to have the same event written to disk multiple times because we don't enforce exactly once delivery
2. Events for a single DID are always replayed in the same order in which they were originally ingested
    - This naturally implies ordering by DID as well, which is often required for building AppViews correctly
3. At least once delivery
    - We explicitly don't enforce exactly once delivery
    - All clients must be idempotent to repeated calls (all existing jetstream v1 clients should be doing this already anyways!)
4. Eventually-consistent, cooperative completeness
    - Jetstream is an at-least-once, filter-honoring event log, not a live mirror of current network truth. It delivers create rows for records that are already dead on the network and does **not** silently fold them away. Deletions are positive marker events (`#delete`, `#update`, `#account` with `active=false, status=deleted`, `#sync`), retained durably forever, never silent absences.
    - The completeness guarantee is **no silent loss of in-scope, retrievable data**: every event matching a subscription's filter that the server can still serve is delivered at least once, in seq order. (If the server holds a matching event and walks past it without delivering it, that's a bug — we crash rather than corrupt.)
    - A correct consumer reaches network truth by **folding** the stream it receives: creates/updates apply; deletes, account-deletes, and syncs remove. Completeness is a joint property of the server (preserves every marker, delivers every in-scope event), the client (folds idempotently), and the user (subscribes to the markers their data model needs). The bundled clients fold for you; third-party clients must too.
    - Bounded incompleteness: below the compaction watermark, superseded create/update rows are physically gone, so a backfill never emits them — nothing to reconcile. The only already-dead records a consumer transiently holds live in the uncompacted tail `(W, tip]` (≈ one compaction interval); those converge as their markers arrive.

Jetstream goes through a bootstrap phase where it seeds a user list from a relay's `com.atproto.sync.listRepos` endpoint, saving all DIDs to a file. During the iteration through the `listRepos` call, it also downloads DID docs to find the user's PDS, calling `com.atproto.sync.getRepo` to backfill the repo to disk. We lay out each event's data on disk in a custom file format called `segment files` as described in Section 3. Once a repo has been downloaded and saved to disk, we mark it as complete in our per-DID tracking file.

During bootstrap, the process does not accept user requests and instead returns a 503 so clients cannot get in to inconsistent states.

During that initial backfill, we also immediately start consuming from the live firehose and storing it to the side of all the backfilled files. This ensures that once the backfill is complete, we can compact those live files to the normal segment stores, and the transition to live will be quick and easy. The initial backfill may take many hours (the fastest we've seen is about 16 hours).

Once the backfill phase is complete, it continues to subscribe to the live tail of the upstream firehose. As it receives events, it stores them in a WAL, and once the WAL reaches a large enough size, it's sealed in to a segment block as described in Section 3.

## 2.1 Client Overview

It's at this transition point that Jetstream starts to field user requests. It handles two request modes:

- HTTPS file downloads of the large segment files
- Live websocket tail in the same JSON format as Jetstream v1 for live events

We provide a seamless user experience similar to the following example Go code.

```go
func main() {
    client := jetstream.Subscribe(
        "jetstream.us-west.bsky.network",
        jetstream.WithCollections([]string{"app.bsky.feed.post"}), // optional
        jetstream.WithDIDs([]string{"did:plc:4uz2445cjiw7w4nobfgnu35f"}), // optional
        jetstream.WithBatchSize(64), // optional
    })

    for events, err := client.Events(ctx) {
        if err != nil {
            continue // handle error
        }
        
        if err := db.WriteBatch(events); err != nil {
	          continue // handle error
	      }

				// Or, handle events individually
        // for event := range events {
            // handle each event
        // }

        lastCursor := events.LastCursor()
        if err := db.SaveCursor(lastCursor); err != nil {
            continue // handle error
        }
    }

    if err := client.Close(); err != nil {
        // handle err
    }
}
```

Under the hood, the client library and server come up with a plan that details which segment files to download, and when to transition to the live websocket. It begins downloading data from either source, but presents events to the user in a single format so they don't even need to be aware of whether or not they're completing a backfill or tailing live. This means that we need to have relatively "thick" client libraries (TypeScript and Go to start) that understand the semantics of Jetstream.

For instance, if a user requests all `standard.site` documents since two weeks ago according to some cursor value, the client library asks the server for the HTTP segments since that time period. It downloads them with some amount of bounded concurrency, and emits them in the `client.Events(ctx)` for loop iterator.

Just like the firehose, the way the data is laid out on disk naturally ensures events within a single DID are delivered in order (though multiple DIDs may be interleaved together).

The client paginates `planBackfill` over the sealed archive — each page names the segments/blocks to download and reports a `sealedTipSeq` (the archive tip, pinned for the whole backfill) and a `plannedThroughSeq` continuation cursor. The client downloads and emits each page, advancing the cursor, until `plannedThroughSeq >= sealedTipSeq`. Only then does it connect `/subscribe` once, at the sealed tip, to pick up the active segment and the live tail. There is no client-side cutover buffer ("jetstream is your buffer"); the websocket lookback window is no longer load-bearing for correctness. Segments sealed *during* the backfill are picked up by the cold-replay path when `/subscribe` re-reads the manifest at connect, and the live tail is deduped by seq, so the seam is at-least-once with no gap.

The client folds the stream as it goes (creates/updates apply; deletes/account-deletes/syncs remove); it does not need to download a tombstone overlay or suppress rows — deletion markers arrive inline as their own events (more on this later).

## 3. Data Layout

### 3.1 Segment Files

### 3.1.1 Overview

`Segment files` store events in a format optimized for fast range scans and high compression ratios. No other access patterns are prioritized (i.e. we don't support point lookups). This is not a general purpose database; it's highly specific to being a full network cache that's optimized for fast replay on as small a disk as possible.

Events in segment files are sorted by the order they were ingested by Jetstream. That means we also naturally sort events in order per-DID (though there is no global ordering).

Each segment file is either `active` or `sealed`. There is only one active segment file at a time, and it's the one that is currently open and being appended to. We use a single on-disk format for both states. The difference between active and sealed is:

1. An active segment reserves the first 4 bytes for the magic number, then 252 bytes as zeroes for the fixed header. It contains zero or more fully-flushed blocks appended to the end of the file.
2. A sealed segment has the 256-byte fixed header populated, its blocks unchanged from active state, and a variable-length footer appended at the end

We detect state at read time by checking the checksum bytes at offset 4: zeroes means active (don't trust the header). A non-zero checksum means sealed.

Each block contains some number of events (4096 by default, but operator-configurable). Each event has some metadata fields and its full raw CBOR, all stored in a columnar format as described in Section 3.2. Storing CBOR is important so Jetstream is interrogatable and can be audited/spot checked for correctness (i.e. comparable to a `getRecord` call on the user's PDS for consistency checks). A block is flushed to disk whenever it reaches 4096 events or 30 seconds have elapsed since the block started filling, whichever comes first. Once a block is ready, we zstd compress it and append it to the active segment file, prefixed with an 8-byte uint64 length. The length prefix makes crash recovery and sequential scans straightforward without needing a zstd-aware frame walker.

As we subscribe to the upstream firehose, we assign each event a sequence number, store them in an in-memory buffer, and forward it to downstream subscribers. Once we've accumulated a full block in-memory, we write it to the active segment file on disk and fsync, then update our latest seen cursor in the metadata db (see section 3.5). The persisted cursor is always less than or equal to the the latest durable event in the segment file.

On crash/restart, we seek to the active segment back to the last complete block (walking the 8-byte length prefixes from offset 256 forward), resume the upstream firehose from the persisted cursor, and rely on at-least-once semantics to cover the overlap. Worst-case, re-fetched and re-delivered traffic is one block. All downstream subscribers must be idempotent to duplicate event delivery (they already should be!). Note that sequence numbers will never go backwards or be duplicated; they only go forward (even on crash and restart).

After a segment file accumulates enough blocks (~256MB of compressed data), we seal it by writing the variable-length footer at the end of the file, seeking to offset 0 and overwriting the reserved 256 bytes with the finalized fixed header, fsync, and rotate to a new active file. The process continues until the heat death of the universe.

Deletions and updates do not modify segment files synchronously. Every delete and update is recorded as a tombstone, keyed by its AT URI (Section 3.3), and applied physically during compaction — the server rewrites sealed segments to drop superseded creates/updates below the compaction watermark. There is no read-time overlay: the server does not suppress rows at delivery time, and clients fold the stream they receive (creates/updates apply; deletes/account-deletes/syncs remove) rather than consulting a separate tombstone set.

Every so often, we compact updates/deletions into the sealed segments. Compaction snapshots tombstones above `compaction/seq`, rewrites sealed segments atomically, and refreshes serving metadata for changed files. See Section 3.3 for more details.

The net of this is that segment files are immutable between compaction passes and only rewrite on the merge-tail pass or the steady-state compaction cadence. These files are CDN-friendly with etags, enabling parallel backfill for clients and easy seeding of another instance from a given jetstream instance's archive.

### 3.1.2 File Format

The binary format of the segment file is as follows:

```
Jetstream Sealed Segment File (.jss):
┌──────────────────────────────────────────────────────────┐
│ Fixed-Len Header (256 bytes, finalized at seal time)     │
│   magic:                   [4]byte = "jss0"              │
│   checksum:                uint64  (xxhash3)             │
│   version:                 uint16                        │
│   block_count:             uint32                        │
│   event_count:             uint32                        │
│   unique_did_count:        uint32                        │
│   min_seq:                 uint64                        │
│   max_seq:                 uint64                        │
│   min_witnessed_at:        int64   (unix micros)         │
│   max_witnessed_at:        int64   (unix micros)         │
│   footer_offset:           uint64                        │
│   did_bloom_offset:        uint64                        │
│   block_did_bloom_offset:  uint64                        │
│   collection_index_offset: uint64                        │
│   block_index_offset:      uint64                        │
│   _reserved:               [158]byte  (future expansion) │
├──────────────────────────────────────────────────────────┤
│ Block 0                                                  │
│   block_len:     uint64 (LE, compressed byte length)     │
│   block_data:    [block_len]byte (ZSTD frame, checksum)  │
│ Block 1                                                  │
│ Block 2                                                  │
│ ...                                                      │
│ Block N                                                  │
├──────────────────────────────────────────────────────────┤
│ Variable-Len Footer (appended at seal time)              │
│   Block Index [N entries, each 52 bytes]:                │
│     offset:            uint64  (byte offset in file)     │
│     compressed_size:   uint32                            │
│     uncompressed_size: uint32                            │
│     event_count:       uint32                            │
│     min_seq:           uint64                            │
│     max_seq:           uint64                            │
│     min_witnessed_at:  int64   (unix micros)             │
│     max_witnessed_at:  int64   (unix micros)             │
│   DID Bloom Filter                                       │
│     Serialized gloom.Filter (MarshalBinary)              │
│     Covers all unique DIDs in this segment               │
│     Sized for 0.1% false positive rate                   │
│   Per-Block DID Bloom Filters                            │
│     One fixed-size gloom.Filter per block, packed        │
│     contiguously and indexed by multiplication           │
│   Collection Block Index (zstd compressed body)          │
│     String table + per-block collection bitmasks         │
└──────────────────────────────────────────────────────────┘
```

An active segment has the same layout minus the footer, with the fixed header left as 256 zero bytes until seal.

The xxhash3 is the hash of all fields after the hash (`version` through the end of the collection block index). The magic number and checksum itself are not included in the checksum.

### 3.1.3 DID Filtering

The variable-length footer has two structures that work together to enable fast scans of "give me all events for user X":

1. Segment-level DID bloom filter ("might user X be in this segment file?")
2. Per-block DID bloom filters ("which blocks in this segment might contain events for user X?")

We use gloom for both and persist its current `MarshalBinary` representation directly. A backwards-incompatible gloom serialization change is therefore a Jetstream segment format change; readers are only expected to support the format produced by the pinned gloom version. We serialize the segment-level bloom to disk upon segment seal, and also keep all segment's blooms in Jetstream server's memory since it's small.

The per-block blooms are kept on disk (one bloom per block, all sized for the configured max events per block so we can index them by multiplication with no offset table). Today the manifest holds every segment's per-block blooms resident in memory; if that footprint becomes a problem, a future change may swap them for an LRU cache of the hot set.

The lookup flow for "give me all events for DID X in this segment" is:

1. Check the segment-level bloom filter (in memory)
    - On negative filter result, skip the whole segment, never touching disk at all
2. On hit, load the per-block blooms for this segment (cache hit, else `pread` from disk into cache)
3. Check each per-block bloom for DID X to produce a candidate block list
4. Decompress only those candidate blocks, scan the `did` column within each for matching events

```
Per-Block DID Bloom Filters:
┌─────────────────────────────────────────────────────┐
│ Header (8 bytes, uncompressed)                      │
│   block_count:      uint32                          │
│   bloom_size_bytes: uint32  (every bloom same size) │
├─────────────────────────────────────────────────────┤
│ Blooms (block_count × bloom_size_bytes)             │
│   Block 0 bloom: [bloom_size_bytes]byte             │
│   Block 1 bloom: [bloom_size_bytes]byte             │
│   ...                                               │
│   Block N bloom: [bloom_size_bytes]byte             │
└─────────────────────────────────────────────────────┘
```

### 3.1.4 Collection Block Index

We store a compact per-block summary of which collections are present in the segment file.

The index consists of a string table of unique collection NSIDs (assigned uint32 IDs by table position) as well as collection counts, followed by a bitmask per block where bit N is set if collection ID N appears in that block. The whole index is stored as a single ZSTD frame with content checksums enabled.

The collection block index is quite small per-file and kept in server memory to attempt to minimize the number of times we need to touch the disk for queries by collection.

```
Collection Block Index:
┌──────────────────────────────────────────────────────────┐
│ Header (16 bytes, uncompressed)                          │
│   collection_count:  uint32 (unique collections)         │
│   block_count:       uint32                              │
│   bitmask_len:       uint32 (ceil(collection_count / 8)) │
│   uncompressed_size: uint32                              │
├──────────────────────────────────────────────────────────┤
│ Body (zstd compressed)                                   │
│   Collection Table [collection_count entries]            │
│     Per entry:                                           │
│       len:   uint8                                       │
│       count: uint32                                      │
│       nsid:  [len]byte                                   │
│   Block Bitmasks [block_count × bitmask_len bytes]       │
│     Bitmask for block 0: [bitmask_len]byte               │
│     Bitmask for block 1: [bitmask_len]byte               │
│     ...                                                  │
│     Bit N set = collection ID N present in this block    │
└──────────────────────────────────────────────────────────┘
```

### 3.2 Segment Blocks File Format

Each block contains 4096 events (configurable by the server operator). Events within a block are stored in a columnar layout. All columns are concatenated and compressed as a single zstd frame with content checksums enabled so that bit flips or partial writes are detected on decompression.

Note that this default size of 4096 was chosen somewhat arbitrarily and we should run experiments on real-world data to measure compression ratios of larger blocks, and try to square that against scans filtering by did or collection needing to examine too much data. More experimentation is required to pick a good default size. Similarly, is 256mb a good size for the overall file?

Each block is composed of the following data:

```
Compressed block (single ZSTD frame):
┌──────────────────────────────────────────────────────────┐
│ Column Metadata (4 bytes)                                │
│   event_count:  uint32                                   │
├──────────────────────────────────────────────────────────┤
│ Fixed-size columns (contiguous arrays):                  │
│   seq[]              event_count × uint64 (LE)           │
│   witnessed_at[]     event_count × int64  (LE)           │
│   indexed_at[]       event_count × int64  (LE)           │
│   kind[]             event_count × uint8                 │
│   collection_len[]   event_count × uint8                 │
│   did_len[]          event_count × uint16                │
│   rkey_len[]         event_count × uint8                 │
│   rev_len[]          event_count × uint8                 │
│   event_len[]        event_count × uint32 (LE)           │
├──────────────────────────────────────────────────────────┤
│ Variable-length columns (concatenated):                  │
│   collections[]      sum(collection_len) bytes           │
│   dids[]             sum(did_len) bytes                  │
│   rkeys[]            sum(rkey_len) bytes                 │
│   revs[]             sum(rev_len) bytes                  │
│   payloads[]         sum(event_len) bytes of CBOR        │
└──────────────────────────────────────────────────────────┘
```

The `event_count` field is required because record deletions may mean we have fewer than the configured max number of events per block.

Each event carries two timestamps. `witnessed_at` is when this jetstream instance first saw the event; we assign it at ingestion and never change it, and it stays monotonic with the sequence number (our range scans and the `?cursor=<timestamp>` lookback rely on that). `indexed_at` is the "display" timestamp we hand to clients as `time_us`; it defaults to `witnessed_at` and a value of `0` means "not set, fall back to `witnessed_at`". Only a timestamp import (Section 8) writes `indexed_at`. Both are unix microseconds. This is the same two-column layout as before — the columns were previously named `indexed_at` and `rendered_at` — so the block bytes and the segment `version` are unchanged.

The `kind` column is a `uint8` discriminator that identifies which firehose event type each row represents:

```
kind values:
  1 = Create     (#commit op: record created)
  2 = Update     (#commit op: record updated)
  3 = Delete     (#commit op: record deleted)
  4 = Identity   (#identity event)
  5 = Account    (#account event)
  6 = Sync       (#sync event)
```

### 3.3 Record Updates/Deletions, Account Deletions, and Compaction

Jetstream supports record updates, record deletion, and account deletion. When an account is deleted, all its records will also be deleted. We will only store data on the latest version of a record; we will not store full record history until that is also kept on-protocol (perhaps some day?). We treat an update similar to a delete, blowing away the original record and replacing it with the newly updated version.

As updates and deletions come over the firehose, and we store them in the active segment file as normal. However, we do eventually need to alter the original source data in older, sealed segment files to ensure we’re not storing data that the user requested be deleted. The process of cleaning up the older data is known as compaction.

Compaction runs once at the tail of the merge phase, after the destination segments are sealed and before the server enters `phase=steady_state`. This makes the first served archive view delete/update compliant. In steady state, compaction runs every `--compaction-interval` (`JETSTREAM_COMPACTION_INTERVAL`, default `4h`; `0` disables compaction) and can also run early when the in-memory tombstone set reaches `--compaction-tombstone-cap` (`JETSTREAM_COMPACTION_TOMBSTONE_CAP`, default `32000000`).

In the pebble kv store, we keep track of `compaction/seq`, the highest sequence number covered by physical compaction. Each pass gathers tombstones above the prior watermark, rewrites affected sealed segment files with a temporary-file + fsync + rename sequence, then advances the watermark only after every rewrite has completed.

Tombstones are retained as event rows forever. Record tombstones come from `KindDelete` and `KindUpdate`; DID tombstones come from account deletes (`active=false`, `status=deleted`) and `KindSync`. Compaction physically removes only superseded `KindCreate` and `KindUpdate` rows older than the tombstone seq. Delete, account, identity, and sync rows are retained so mid-stream clients and future audit tooling can observe the event history.

Segment rewrites preserve block topology and historical seq/witnessed-at envelopes. A block whose rows are all dropped remains present as an `event_count=0` block, so manifest block indexes, cursor translation, and cold replay can continue to use stable block numbers across generations. Rewritten files get new checksums; HTTP downloads derive validators from the file descriptor they serve, and the subscribe block cache keys decoded blocks by segment checksum.

We also store tombstones since the most recent compaction in memory in the server. This is purely a compaction-internal structure — it powers steady-state compaction and early cap-triggered passes (it gates when a pass runs and feeds the size gauge). There is **no read-time overlay endpoint**: the server never exposes the tombstone set to clients and never suppresses rows at delivery time. Backfill clients converge by folding the markers they receive inline (Section 4.5), not by downloading a separate suppression set.

The one case folding-inline does not cover on its own is a **collection-filtered** backfill that needs DID-level markers (account-delete, `#sync`). Those markers carry an empty collection, so a naïve collection-filtered plan would never select their blocks, and a deleted account's records would survive forever in that consumer's fold — a silent loss. We close this in the segment index rather than with an overlay: when a block contains a DID-level marker, the seal/rewrite indexer tags it with a reserved sentinel collection (`$account`, `$identity`, `$sync` — see `segment/sentinel.go`). These names begin with `$`, which makes them invalid NSIDs, and the planner only admits real NSIDs / NSID-authority wildcard prefixes, so no client request can name or prefix-match a sentinel. The planner unconditionally admits a segment's sentinel ids under any collection filter, so marker blocks are always selected (the per-block DID bloom still narrows by DID). The markers then ride inline through `getBlock` in seq order, exactly as record-level deletes do, and a folding consumer converges with zero client-side special-casing.

#### Historical backfill planner endpoint (`network.bsky.jetstream.planBackfill`)

The server exposes sealed-archive transport planning as an XRPC procedure, `network.bsky.jetstream.planBackfill`. It accepts exact DID filters, collection filters (exact NSIDs or `.*` namespace wildcards), and an optional seq window with `(afterSeq, beforeSeq]` semantics. Missing or empty DID/collection filters mean match all.

The planner runs over manifest-resident sealed-segment metadata: segment seq bounds, block seq bounds, segment DID blooms, per-block DID blooms, and per-block collection summaries. It never opens segment files on the normal path. It returns an ordered list of whole segments or inclusive block ranges that may contain matching rows, plus two seq fields: `sealedTipSeq`, the sealed-archive tip (capped by `beforeSeq` when present), stable across pages of the same archive; and `plannedThroughSeq`, the continuation cursor — the highest sealed seq this page accounts for.

The planner has a one-sided correctness contract: no false negatives, possible false positives. DID bloom filters may include blocks that do not contain the requested DID, and block-level collection summaries are still only transport hints. Clients must decode rows, apply exact DID/collection filtering, fold deletes/updates, and de-duplicate/idempotently process events.

**Pagination.** Servers bound per-page cost with configurable limits: maximum distinct DIDs, maximum distinct collections, maximum response/work entries, and a whole-segment density threshold. When a plan would exceed the per-page entry limit, the server **truncates at a work-unit boundary** (a whole segment, or one coalesced block range) and reports `plannedThroughSeq` as the `MaxSeq` of the last included unit — never the enclosing segment's `MaxSeq` after a mid-segment cut, which would skip the un-included tail blocks. The server never silently truncates or refuses: there is no `PlanTooLarge`. At least one unit is always admitted per page, so a single oversized unit still makes progress (no zero-progress livelock). When a page is not truncated, `plannedThroughSeq == sealedTipSeq`.

Putting it all together, the client's backfill→live loop looks like:

1. The client calls `planBackfill(afterSeq=cursor)` (page 1) to learn which sealed segments/blocks may satisfy its query (i.e. "give me all `standard.site` documents") and pins `S = sealedTipSeq` as the upper bound for the whole backfill.
2. The client downloads the planned segments/blocks, decodes them, applies exact DID/collection filtering, and emits matching rows. DID-level markers (`#account`/`#identity`/`#sync`) ride inline via the sentinel index (Section 3.3), so a collection-filtered backfill receives the deletions it needs to fold.
3. The client advances `cursor = plannedThroughSeq` and, while `cursor < S`, calls `planBackfill(afterSeq=cursor, beforeSeq=S)` again (pinning `beforeSeq = S` so the range never floats) and repeats step 2.
4. Once `plannedThroughSeq >= S`, the sealed range is fully consumed. The client connects `/subscribe` exactly once at `cursor = S` to pick up the active segment and the live tail. Inclusive replay plus client-side seq dedup makes the seam at-least-once with no rewind margin; segments sealed during the backfill arrive via cold replay (the server re-reads the manifest at connect).
5. The client folds every row it receives (creates/updates apply; deletes/account-deletes/syncs remove). It applies no overlay and suppresses nothing.

There is no overlay download and no separate tombstone-negotiation step. The client owns the orchestration among paginated planning, historical block/segment downloads, exact filtering, folding, and the single live-tail cutover. A crashed backfill resumes from its last continuation cursor rather than restarting. If the handoff cursor `S` ages below the server's lookback floor during a slow backfill, the terminal `/subscribe` connect returns an explicit HTTP 400 "cursor too old" (Section 5) and the client transparently re-enters the pagination loop from its last processed seq rather than silently skipping the `(S, floor]` gap.

### 3.4 File Organization

Events within segment files are laid out on disk by DID so we can naturally maintain the invariant of "all events from a user must be replayed in the same order in which the events were indexed".

The data directory file layout is the following:

```
data/
  meta.pebble/             <- unified metadata store (see Section 3.5)
  segments/
    seg_0000000000.jss     <- fixed header + compressed blocks (+ footer once sealed)
  backfill/
    live_segments/         <- segment files for the live tail consumer during the backfill phase
      seg_0000000000.jss
```

Segments are named with a counter as a 10-digit zero-padded base-36 string. Segment files and seq ranges sort lexicographically in creation order. This means that all events in segment file 0 have witnessed at timestamps before all events in segment file 1.

### 3.5 Metadata Store

All structured metadata that isn't derivable by cheaply rescanning segment files lives in a single pebble database at `data/meta.pebble/`. We picked pebble because it's pure Go, handles tens of millions of keys comfortably, and gives us atomic multi-key batch writes for free, which matters for the durability ordering described below.

Keys are namespaced by prefix:

```
relay/cursor            -> uint64 upstream firehose seq we've durably persisted
phase                   -> string current lifecycle phase: bootstrap, merging, or steady_state
phase/entered_at        -> RFC3339Nano timestamp when the current lifecycle phase was entered
backfill/timing/started_at   -> RFC3339Nano timestamp when initial bootstrap backfill started
backfill/timing/completed_at -> RFC3339Nano timestamp when initial bootstrap backfill drained
compaction/seq          -> the highest-watermark sequence number of the most recent compaction; owned by the compactor
repo/<did>              -> JSON<RepoStatus> per-DID backfill and steady-state bookkeeping
account/<did>           -> JSON<AccountStatus> hosting status, only present when non-active
sync/<did>              -> JSON<SyncState> present while a resync is in progress
```

The `backfill/timing/*` keys are operator diagnostics, not control-plane state. When the bootstrap backfill engine drains, the orchestrator commits `phase=merging`, `phase/entered_at`, `backfill/timing/started_at`, and `backfill/timing/completed_at` in one synced pebble batch. That makes the status page's completed-backfill duration durable without creating a recovery dependency on it. Older data directories that predate these keys can still enter steady state; they simply render the completed backfill duration as unknown.

`RepoStatus` carries both initial backfill state and steady-state bookkeeping with the following fields:

```go
type Status string

const (
    StatusNotStarted Status = "not_started"
    // awaiting whole-repo replacement via the explicit post-merge pending
    // retry pass; currently produced by interrupted bootstrap recovery
    StatusPending Status = "pending"
    StatusComplete   Status = "complete"
    StatusFailed     Status = "failed"
    // account exists but its repo is unfetchable (deactivated/suspended/
    // taken down); terminal and never retried
    StatusUnavailable Status = "unavailable"
)

type RepoStatus struct {
    Backfill    RepoBackfillStatus `json:"backfill"`
    PDS         string             `json:"pds,omitempty"`
    Host        string             `json:"host,omitempty"`
    Handle      string             `json:"handle,omitempty"`
    // latest rev, updated on every commit
    Rev         string             `json:"rev,omitempty"`
    UpdatedAt  time.Time           `json:"updated_at,omitempty"`
    LastAttemptedAt time.Time      `json:"last_attempted_at,omitempty"`
    RecordCount int64              `json:"record_count,omitempty"`
    TotalBytes  int64              `json:"total_bytes,omitempty"`
    Active      bool               `json:"active"`
}

type RepoBackfillStatus struct {
    Status        Status    `json:"status"`
    // rev at end of initial download
    Rev           string    `json:"rev,omitempty"`
    Attempts      int       `json:"attempts,omitempty"`
    RetryCount    int       `json:"retry_count,omitempty"`
    LastError     string    `json:"last_error,omitempty"`
    NextAttemptAt time.Time `json:"next_attempt_at,omitempty"`
    StartedAt     time.Time `json:"started_at,omitempty"`
    CompletedAt   time.Time `json:"completed_at,omitempty"`
}
```

The per-block durability ordering is: append and fsync the block into the active segment first, then commit a single pebble batch with `sync=true` that advances `relay/cursor` and updates `repo/<did>.Rev` and other fields for every DID present in the block. Only after both steps complete do we treat the block as durable. Because the pebble batch always follows the segment fsync, a crash between the two leaves `relay/cursor` pointing at or before the last durable event, so if we do crash, we'll just replay some relatively small number of events.

Segment persistence failures are crash-loud, uniformly. Any write, fsync, or rename error on a segment path — the active writer's block flush and seal, the pebble durable-batch commit, a compaction rewrite, or a timestamp-import patch — aborts the process rather than continuing past unarchived data; there is no read-only degraded mode. Disk-full (`ENOSPC`) errors additionally carry an actionable operator message on every one of those paths ("fatal persistence error: disk full while ... free space or move the data directory, then restart jetstream"), and the `jetstream_data_dir_free_bytes` gauge exists to alert before it comes to that. Recovery after any such crash is the normal restart path: the torn-tail walk truncates at the last fully-durable frame and the persisted cursor replays the small un-committed window. Compaction rewrites and import patches write a sibling `.tmp`, fsync, then rename, so a failure at or before the rename always leaves the original segment untouched. All of this is enforced by a deterministic segment I/O fault-injection seam (`segment.IOFaultInjector`, nil in production) that the oracle's segment-fault tier drives end-to-end through a real runtime.

Everything else is deliberately kept out of the metadata store. The segment manifest is just a directory scan plus each file's self-describing 256-byte header, so we don't duplicate it. DID-to-PDS caches and handle resolutions come back from the PLC directory when we need them. Per-DID hosting status flows in as `#account` events; we keep the current value in pebble so we can answer quickly, but it's always reconstructible by replaying segments.

## 4. Ingestion Pipeline

This section describes how events get from the PDSes and relay into our segment files.

### 4.1 Bootstrap Phase

On first startup, we kick off two things in parallel:

1. The live firehose consumer
    1. Connects to `com.atproto.sync.subscribeRepos` on the relay
    2. We start the live tail first to ensure we don't miss any events
    3. Events are written in segment files to the temporary `./data/backfill/live_segments` folder
    4. We treat these as temporary events and will compact them in to the long-term `segments` folder in the merge phase
2. The backfill engine
    1. Downloads all results from `com.atproto.sync.listRepos` on the relay, writing each DID to `repo/<did>` in the metadata store with `StatusNotStarted`
    2. Downloads each repo via `com.atproto.sync.getRepo` and writes the events directly to the active segment file
    3. On successful completion, sets `repo/<did>.Status = StatusComplete` and records the `BackfillRev` in Pebble.

This phase takes a while. At time of writing with current rate limits on the mushroom PDSes on the new relay, it takes ~16 hours.

Once we complete the backfill phase, we seal the active segment so when we resume the live consumer during the steady state phase, it starts with a fresh file.

### 4.2 Merge Phase

Once the backfill of all repos is complete, we need to merge the segment files accumulated by the live firehose consumer in to the `./data/segments/` folder for permanent storage.

The bootstrap-live consumer has been continuously persisting the upstream firehose cursor to `relay/cursor` on every block flush, so by the time we enter the merge phase the cursor already reflects the latest durable bootstrap-live event. We stop the live consumer before merge runs; when we transition to the steady-state phase, the new live consumer reads `relay/cursor` and resumes from that watermark. At-least-once delivery covers the at-most-one-block overlap. The merge phase itself is a relatively small amount of data and should only take a few minutes.

We take the events from the sealed and active segment files in `./data/backfill/live_segments/` and replay those events in to the main `./data/segments/` directory. We do that simply by opening a new segment file in `./data/segments` with the next contiguous file name, and writing events from `./data/backfill/live_segments` to the new file(s). We seal and roll over to the next segment file once the active one reaches its size limit.

We ensure that we don't store any events out of order by checking the DID's `BackfillRev` from `repo/<did>` in the metadata store against the revs of the events we're compacting. We drop the events whose rev is less than or equal to `BackfillRev`. This also should ensure we don’t store duplicate events (though because of at-least-once semantics, this is not a strict requirement).

After sealing the destination segment, we run the merge-tail tombstone compaction described in Section 3.3 so the archive is delete/update-compliant before `phase=steady_state` is written.

After the last surviving event has been durably written:

- We seal the active segment file in `./data/segments/`
- We `os.RemoveAll` the entire `./data/backfill/` directory
    - This tree is temporary and only valid during the initial backfill phase; nothing outside the merge code should ever read from it again
- Only after these steps do we durably write `phase=steady_state`
    - Each step is durable on its own (segment seal fsyncs, RemoveAll is observable on next directory scan, pebble delete is Sync=true), so a crash at any point during merge is recoverable on restart

Additionally, since the merge phase takes some time, at the end, we also scan the relay with `listRepos` to find accounts that were newly created during the merge phase. We treat these repos the same as repos that failed to download during the initial backfill, and we will do a full `getRepo` call on them during the steady-state phase.

### 4.3 Steady State Phase

The steady-state phase simply consumes from the upstream firehose and writes events to the active segment file in the `./data/segments` directory as normal. Every so often, it compacts updates and deltions in to the sealed segments as described in Section 3.3.

Every block seal commits a single pebble batch that advances `relay/cursor` and refreshes `repo/<did>.LatestRev` for every DID in the block. We always fsync the segment block first and then commit the pebble batch with `sync=true`, so the persisted cursor can never get ahead of the durable event data. A crash between the two steps is handled by the active-segment recovery path described in Section 3.1, and the upstream resumes from whatever cursor pebble last saw.

If there were any accounts that failed to download during the initial backfill phase or the post-merge `listRepos` discovery pass (i.e. `repo/<did>.Status == StatusFailed`), we periodically retry downloading them with exponential backoff in the background until they succeed. Retry eligibility and backoff are stored on `repo/<did>` via `RetryCount` and `NextAttemptAt` so process restarts do not create retry storms. When we do successfully download a repo that previously failed, we treat it similar to a whole-repo `#sync` event: mark all previous events for that DID as deleted, and recreate from the downloaded CAR file.

Bootstrap crash recovery can promote a pre-existing `StatusNotStarted` row to `StatusPending` ([#262](https://github.com/bluesky-social/jetstream/issues/262)) instead of re-downloading it at low seqs. Merge runs one immediate pending retry pass after draining the captured live tail, so the synthetic sync and replacement rows land above the live tail. If that pending attempt fails transiently, the row becomes `StatusFailed` with normal backoff metadata and the periodic failed-repo loop handles later retries.

A live first sighting is **not** a `getRepo` trigger. If a repo was behind a firewall or otherwise hidden from the relay during bootstrap and later starts emitting live traffic, Jetstream archives the live events it receives and does not create a `repo/<did>` row or enqueue a background download for that DID. That condition is a PDS/operator repair case: the PDS should emit a new `#sync` event when the repo needs an authoritative full re-download. `StatusPending` rows from the removed first-sighting enqueue path remain decodable, but new live first sightings must not create them and the steady-state failed-repo retry scan does not treat them as eligible.

The `/subscribe` live tail reads the steady writer's readable log ([#248](https://github.com/bluesky-social/jetstream/issues/248)). The steady writer is shared by multiple producers — the live consumer plus the failed-repo retry runner above — so visibility hangs off the seq allocator itself: every event appended through the writer is copied into an ordered in-memory log as soon as it receives a seq, before any flush, async compression, or rotation can move it between durability stages. The writer advances the log's durable watermark only after the segment block is fsynced and the `seq/next` Pebble batch commits. Entries at or above that watermark are pinned; durable entries below it are retained under the configured byte budget. `/subscribe` reads the log for resident seqs and uses the cold reader only for cursors below the log floor, which are durable by construction.

### 4.4 Upstream Input Validation

All upstream data — relay firehose frames and backfill CARs alike — is treated as untrusted input. A validation gate at each ingest conversion point ([#197](https://github.com/bluesky-social/jetstream/issues/197)) enforces:

- **Revs must be spec-valid TIDs.** Rev ordering drives the merge filter, the stale-resync guard, syncstate promotion, and compaction tombstone folding, so a record under a garbage rev is never archivable. On the live path an invalid commit/sync rev drops the **whole event** (every row shares the rev) and advances the cursor past it — the input can never become valid, so there is nothing to replay. On the backfill path an invalid rev **fails the repo** (visible in failed-repo diagnostics and retried by the retry loop); atmos's repo loader already rejects invalid non-empty revs before our handler runs, so the empty rev is the reachable case.
- **Op paths must be spec-valid.** Collections are validated as NSIDs (which also rejects `$`-prefixed names that could shadow the `$account`/`$identity`/`$sync` sentinel index — see Section 3.3) and rkeys against the record-key syntax. A spec-invalid path drops **that op only**; well-formed siblings in the same commit or repo archive normally, matching the missing-block precedent. Because validated rkeys are pure ASCII, the delivery-side problem of `json.Marshal` substituting U+FFFD for invalid UTF-8 (a client delete-by-rkey that silently never matches) disappears.
- **Spec-valid but unrepresentable stays distinct.** A legal rkey of 256–512 bytes exceeds our 255-byte segment column; those records drop under the existing `ErrFieldTooLong` path with their own reason so operators can tell "network sent garbage" from "legal but we chose not to represent."

Every drop increments one shared counter family, `jetstream_ingest_dropped_events_total{source=live|backfill, reason=invalid_rev|invalid_collection|invalid_rkey|field_too_long|missing_block}`. Validation drops are counter-only by design — hostile upstream input must not be able to drive log volume — with span events carrying per-drop detail when a trace is active. (This family replaced the older `jetstream_livestream_dropped_events_total`, `jetstream_livestream_dropped_ops_missing_block_total`, and `jetstream_backfill_dropped_records_total` counters.)

### 4.5 Identity, Account, and Sync Events

As noted in Section 3, all event types are stored in the segment files, not just commits.

Internally, Jetstream doesn't care about handles, identity updates, or hosting status, but consumers of the application certainly do. `#identity`, `#account`, and `#sync` events are stored in-line with `#commit` events in the same segment blocks and passed along to clients as they come over the firehose.

#### Identity/Account delivery and the collection filter

The legacy `/subscribe` (v1) endpoint preserves the original Jetstream contract: regardless of `wantedCollections`, all subscribers receive `#account` and `#identity` events (they are still gated by `wantedDids`). This is intentional backwards compatibility and must not change.

`#account` and `#identity` are delivered **unconditionally** — regardless of `wantedCollections`, on **both** `/subscribe` (v1) and `/subscribe-v2`, and by the Go client (still gated by `wantedDids`). `#sync` is also unconditional with respect to `wantedCollections`, but it is only emitted on **`/subscribe-v2`**: the v1-compatible JSON wire skips `#sync` for v1 parity (v1 never emitted it — `encoder.go` returns `errSkipEvent` for the v1 `Encode`, while `EncodeV2` emits it). The bundled Go client connects to `/subscribe-v2`, so it receives `#sync`; a v1 `/subscribe` consumer does not. Earlier drafts dropped `#identity` (and hid `#account`) under a collection filter; that policy is gone (see [#142](https://github.com/bluesky-social/jetstream/issues/142), [#171](https://github.com/bluesky-social/jetstream/issues/171)). The reason is correctness, not just intuitiveness: a DID-level marker (an account delete `active=false, status=deleted`, or a `#sync` divergence) is what a folding consumer uses to purge a dead account's records, so it must reach every subscriber — including a collection-scoped one. There is no client-side suppression and no "fold before the delivery filter" step; the markers are delivered as ordinary events and the consumer folds them. (For the *backfill* path, the same coverage is provided in the archive by the DID-marker sentinel index described in Section 3.3, so a collection-filtered backfill selects those marker blocks too.)

Jetstream respects sync 1.1. In the case where a `#sync` event indicates a repo has diverged and requires a full resync, we store the `KindSync` row followed by the authoritative replacement records returned by the verifier. The `KindSync` row is a DID tombstone for compaction (see Section 3.3), so older pre-divergence record rows are physically removed once the relevant sealed segment is compacted.

## 5. Client Protocol and Libraries

We ship client libraries in TypeScript and Go. They are relatively "thick" in the sense that clients require substantial amounts of logic in order to use the system. All the code is public and well-documented, so community members can maintain client libraries in other languages.

For the live-tail use-case, clients are simple: it's compatible with the existing Jetstream v1 WebSocket JSON payload and query parameters. Existing Jetstream v1 consumers will continue to work as-is (i.e. no client wrapper library is even needed for those simple use-cases).

It gets more complicated when the caller requests data that is older than the current active segment. The client asks the server something like "I want all likes since 2024", pages `planBackfill` over the sealed segment files (downloading and emitting each page's blocks in order, including the inline deletion/update markers), and once it has consumed the sealed range it seamlessly cuts over to the live websocket payload. That cutover is transparent to callers, who use a Go `iter.Seq2` or a TypeScript async iterable to provide an excellent devex.

Most callers will want to use the client wrapper (even for the trivial use case) so they don't need to repeatedly implement the same websocket logic, and may seamlessly handle the case where they want to perform backfill some day.

### 5.1 Simple JSON Payload (default)

The default websocket stream delivers events in the same JSON shape as Jetstream v1 today. It's small, easy to consume from any language, and carries a decoded form of each record so callers don't need a CBOR decoder. This is what the vast majority of end-user clients will use.

An example commit event looks like:

```json
{
  "did": "did:plc:eygmaihciaxprqvxpfvl6flk",
  "time_us": 1725911162329308,
  "cursor": 12345,
  "kind": "commit",
  "commit": {
    "rev": "3l3qo2vutsw2b",
    "operation": "create",
    "collection": "app.bsky.feed.like",
    "rkey": "3l3qo2vuowo2b",
    "cid": "bafyreidwaivazkwu67xztlmuobx35hs2lnfh3kolmgfmucldvhd3sgzcqi",
    "record": {
      "$type": "app.bsky.feed.like",
      "createdAt": "2024-09-09T19:46:02.102Z",
      "subject": {
        "cid": "bafyreidc6sydkkbchcyg62v77wbhzvb2mvytlmsychqgwf2xojjtirmzj4",
        "uri": "at://did:plc:wa7b35aakoll7hugkrjtf3xf/app.bsky.feed.post/3l3pte3p2e325"
      }
    }
  }
}
```

The `time_us` field is the event's display timestamp in microseconds since the unix epoch: the `indexed_at` value if a timestamp import set one, otherwise the `witnessed_at` time jetstream first saw the event (see Section 8). Until an operator runs an import, every event's `time_us` is just its `witnessed_at`. The `cursor` field is jetstream v2's monotonic per-event sequence number (a JSON number); clients that want to resume from a saved point pass `?cursor=N` on reconnect.

For backwards compatibility with jetstream v1, the server also accepts a v1-style unix-microsecond timestamp on the same `?cursor=` query parameter. The two namespaces are distinguished by magnitude: a value strictly less than 1×10^15 is interpreted as a v2 sequence number; a value greater than or equal to 1×10^15 is interpreted as a v1 unix-microsecond timestamp. The split is provably non-overlapping under our 36h lookback ceiling (any legitimate v1 timestamp within 36h of "now" is well above 10^15, and v2 seq won't approach 10^15 for centuries).

Cursor lookback is bounded to the most recent 36 hours by default (matching jetstream v1), tunable via `--cursor-lookback`. The two endpoints handle a too-old cursor differently, on purpose:

- `/subscribe` (v1) clamps a below-floor cursor **silently** and starts at the oldest event in the window, preserving wire parity with jetstream-legacy (real legacy consumers depend on this; a v1 `/subscribe` never rejects an old cursor). The clamp is made operator-visible via a distinct metric label.
- `/subscribe-v2` **rejects** a below-floor seq cursor with a pre-upgrade HTTP 400 whose body carries the floor seq ("cursor … below lookback floor …"), rather than silently dropping the `(cursor, floor]` gap. This is what lets a backfilling client detect a slow handoff and re-backfill from its last seq (Section 2.1). The v2 timestamp-cursor path still clamps (legacy timestamp translation).

Cursors in the future drop into live-tip mode (no replay) on both endpoints.

### 5.2 The /subscribe-v2 JSON Payload

The `/subscribe-v2` endpoint delivers a strict superset of the v1 shape: all the same fields are present (including the decoded `commit.record` JSON), plus:

- `seq`: Jetstream's own monotonic 64-bit cursor assigned to this event at ingestion time (the same value as `cursor`; v1 only carries `cursor`).
- `commit.record_cbor`: the raw DAG-CBOR payload of the record, base64-encoded. Only populated for `kind: "commit"`. This is the byte-exact form written to the segment file, suitable for verifying against a PDS or reconstructing the MST.
- `sync`: the archived `#sync` event, which v1 never emits. `sync.blocks` is the raw CAR payload, base64-encoded. Only populated for `kind: "sync"`.
- TODO: `prevRev`  on all events (Fig suggestion)

The bundled Go client consumes this wire on its live tail; the raw DAG-CBOR is what lets a typed consumer decode records with its own lexicon codegen, byte-exactly.

`/subscribe-v2` also diverges from v1 in presentation policy (both are deliberate, per-endpoint contracts): it emits Sync 1.1 resync replacement rows (v1 advances over them silently for wire parity), and it rejects a below-floor seq cursor with a pre-upgrade HTTP 400 as described in Section 5.1 (v1 silently clamps).

`/subscribe-v2` subscriptions are not authenticated, but they're more expensive to produce (base64-encoded CBOR, heavier per-event payload) so we may more strictly rate limit them compared to the v1 firehose on the Bluesky-hosted instance.

> Historical note: earlier drafts specified an `?extended=true` opt-in carrying `upstream_relay_cursor` and interleaved control events (`segment_sealed`, `segment_compacted`, `heartbeat`) to serve the Section 6 replication protocol. That replication design was dropped before shipping and the extended mode was removed with it; `/subscribe-v2` always carries the superset payload described above.

## 6. Replication

> **STATUS: DROPPED — to be redesigned.** An earlier draft of this section specified an asynchronous active-passive replication protocol built on an `?extended=true` websocket payload (carrying `upstream_relay_cursor` and `segment_sealed`/`segment_compacted`/`heartbeat` control events). That design — and the extended wire mode it required — has been removed; nothing of it was ever implemented. High availability will be addressed by a different mechanism designed later; see `specs/notes/2026-06-27-high-availability-clustering.md` for the current exploration. Jetstream today runs single-node.

## 7. Rate Limits

For production readiness, we ensure that we have reasonable rate limits for requests that make it to the system. We'll put in place per-IP limits on the CDN as well as the origin to ensure that HTTP segment file downloads are limited as well as the number of subscribers and events to the live tail.

These will be configurable over time as we scale.

This is of course an implementation detail of the Bluesky-hosted instance. Others can do what they please.

### 7.1 Operational Freshness and Crash Diagnostics

The debug listener exposes `/healthz`, `/readyz`, `/metrics`, and pprof.
`/readyz` only means both HTTP listeners are bound and serving; it is not an
ingest-health or steady-state signal. Use `/status` and Prometheus metrics for
the ingestion view.

Normal steady-state relay freshness is exported as two gauges:

- `jetstream_current_timestamp_seconds` — current Unix timestamp at scrape time.
- `jetstream_livestream_last_seen_upstream_event_timestamp_seconds` — the last
  local time the steady-state live consumer observed a normal upstream
  `subscribeRepos` event. Bootstrap, backfill, merge replay, failed-repo retry,
  sync-triggered or synthetic data resyncs, and import/compaction work do not
  update it.

A typical alert is:

```promql
jetstream_livestream_last_seen_upstream_event_timestamp_seconds > 0
and
jetstream_current_timestamp_seconds
  - jetstream_livestream_last_seen_upstream_event_timestamp_seconds > 30
```

Jetstream preserves crash-loud behavior for internal corruption and persistence
failures. Set `GOTRACEBACK=all` in production service units so a crash includes
all goroutines; the runtime also logs panics from long-lived goroutine roots in
structured logs with build metadata before re-panicking.

## 8. Timestamp Import

Any new firehose indexer has the same problem: it stamps every record with roughly the time it backfilled, so a post from 2022 looks like it was made today. `createdAt` doesn't save us — it's client-supplied and spoofable, which is both a bad product experience and a trust-and-safety hole. To build a real AppView you have to carry over the original indexer's timestamps. Bluesky's dataplane has been running since 2022 and has them; a fresh jetstream does not.

So we keep two timestamps per event:

- `witnessed_at` — when *this* jetstream first saw the event. We assign it at ingestion time and never change it. It's what our range scans and the `?cursor=<timestamp>` lookback are built on, so it has to stay honest and monotonic with the sequence number.
- `indexed_at` — the timestamp we hand to clients as `time_us`, i.e. the one they should actually display. By default it's just `witnessed_at`, but an operator can overwrite it with the value the old indexer recorded. This is the "display" timestamp.

Only the operator can change `indexed_at`, and it's off by default: the import endpoint is disabled and returns 401 unless a bearer token was configured at startup. We're not letting random callers rewrite timestamps.

Imports run against a steady-state live server with no downtime. They are deliberately not a bootstrap or merge-phase operation: before steady state there is no stable serving archive to repair. The operator stages a plain (uncompressed) CSV of AT URIs and timestamps somewhere on the box, then kicks off an import job pointing at that path. Uncompressed on purpose: the import records a byte offset for each valid row during its single streaming validation pass and seeks straight back to that row when it's time to patch, which a compressed stream can't do without re-scanning or spilling a plaintext copy — and the box already holds the multi-terabyte segment archive, so the extra disk is a cheap trade. Each row can say whether the timestamp applies to every version of the record (the default) or one specific version by CID. We key on AT URI rather than CID because the URI contains the DID, which lets the segment-level DID bloom do almost all the filtering for free; operators who only have CIDs can resolve them to URIs first.

The job first ingests the CSV into a durable imported-timestamp rule map and reloads that map into the steady writer's append path. Once the rules are active, it force-rotates the current active segment so rows that were already buffered become sealed and visible to the patch pass. It then buckets the URIs by DID, uses the segment and per-block DID blooms to find candidate blocks, decompresses each one, and patches the `indexed_at` column for the matching rows. Rows appended after rule activation are stamped with their imported `indexed_at` before they enter the segment or live read log, so they do not depend on a later patch. Bad rows are skipped and reported rather than failing the whole import, and re-running is safe: an already-applied file just produces no changes.

Once the import has rewritten the affected segments, the manifest lists the new files and backfilling clients pick them up on their next segment listing. (The dropped Section 6 replication design would have pushed `segment_compacted` notifications to replicas; whatever HA mechanism replaces it must account for compacted-segment propagation.)

### 8.1 Operating an import

**Enable it.** Set a bearer token at startup: `--timestamp-import-token` (or `JETSTREAM_TIMESTAMP_IMPORT_TOKEN`). With no token the two endpoints always return 401 and are indistinguishable from "disabled" — that's the secure default. Stage the CSV under the confinement directory, which defaults to `<data-dir>/imports` and is overridable with `--timestamp-import-dir` (`JETSTREAM_TIMESTAMP_IMPORT_DIR`). The submitted path is resolved and confined to that directory; `..` traversal and symlinks that escape it are rejected.

**Front it with TLS.** The token is a bearer secret. Jetstream serves plain HTTP and expects TLS to be terminated by your proxy, so terminate TLS in front of the import endpoint — jetstream does not enforce it in-process (an in-process check would inspect a connection that is already plaintext).

**The two endpoints** (both bearer-gated, under `/xrpc/`):

- `network.bsky.jetstream.importTimestamps` (procedure) — body `{ "path": "<file>" }`, where `<file>` is relative to the import directory (or an absolute path inside it). Returns `{ "job": "<id>" }`. Only one import runs at a time; a concurrent submit gets `409 ImportInProgress`. A valid submit before the steady-state writer is running gets `503 ImportNotReady`. A bad path gets `400 InvalidPath`.
- `network.bsky.jetstream.getImportStatus` (query) — `?job=<id>` (or omit it for the current/most-recent job) reports lifecycle state, phase, per-phase progress, and, on completion, the parse/mutation totals. The same summary appears on the operator `/status` page.

**CSV schema.** Header row `uri,timestamp,scope,cid`. Column order is read from the header, so it need not be canonical, and the optional `scope`/`cid` columns can be omitted entirely. The header is strict: an unrecognized or duplicate column name fails the whole file up front (it's almost always a typo of a real column, and mis-mapping every row is worse than a loud error).

- `uri` — `at://<did>/<collection>/<rkey>`. Required.
- `timestamp` — RFC3339 (e.g. `2022-01-02T03:04:05Z`), parsed to microseconds. Required.
- `scope` — `all_versions` (default when empty or absent) patches every create/update/resync sharing the URI; `specific_version` patches only the version whose stored DAG-CBOR payload recomputes to `cid`.
- `cid` — required iff `scope=specific_version`, ignored otherwise.

Bad data rows are skipped and counted, but a bare/unclosed quote aborts the file: it makes everything after it unparseable, so silently treating it as one bad row would drop the rest of the import.

Sorting the CSV by DID is *recommended, not required*: it keeps the bucketer's per-DID cache warm (roughly one bloom lookup per distinct DID). Unsorted input is still correct, just with more cache misses.

**`specific_version` needs per-version CIDs.** Only collections whose source kept full per-version history with CIDs can use it (e.g. `site.standard.document`). Sources that keep only the latest version of a record (like the `posts` table) can't supply a CID for historical versions — those use the `all_versions` default, which smears one timestamp across every version of the URI.

**Crash safety.** Progress is checkpointed in the metadata store per segment, so a process restart auto-resumes the same job without re-submission and skips segments it already patched. Even if the checkpoint is lost the job is safe to re-run: an already-applied segment produces zero mutations and is skipped. There is no dry-run mode — a real run is safe to start and watch via `getImportStatus`.

**When a job fails: re-submit the same CSV.** A job that hits a real error (disk full, a shutdown that raced the job's setup) lands in `failed` state on `getImportStatus` and the `/status` page, and deliberately does *not* auto-resume — re-running a deterministically failing job would loop. A failed run may have installed part of its rule map, so new events can be stamped from an incomplete rule set until the import is completed. The remedy is simple: fix the cause and re-submit the exact same CSV. Jetstream never modifies or deletes the staged file (only per-job scratch under `<data-dir>/import-scratch` is cleaned up), and every phase is idempotent — rule ingestion is last-write-wins over the full file and already-patched segments produce zero mutations — so the re-run converges to the same end state as an uninterrupted import.

## 9. FAQ

1. **Why store data by indexed timestamp rather than collection?**

> I did a v0 of this system storing data by collection, and it has some nice properties. If you only want to download `app.bsky.feed.post`, it's simple and efficient to do so. However, when creating real-world AppViews, the requirement to replay events in order for a single DID would require a k-way sort if we ordered by collection. That's a deal breaker. Ordering by witnessed at timestamp has the property that events within a single DID are ordered in the order in which they were created (though there is no global ordering).
> 
1. **What is the latency between a record being read from the live firehose and it being sent to client consumers during steady-state?**

> Close to real-time. It's essential for AppView product experience that we deliver records with very low latency (tens of milliseconds).
> 
1. **This is the next evolution of Jetstream v1? What happens to existing Jetstream v1? How does Tap play in to this?**

> This system uses the same JSON+websocket API as Jetstream v1, so we will swap out the underlying Jetstream v1 processes for instances of jetstream (ensuring that cursors only go forward). Clients will not be impacted.
> 

> Tap is solving a bit of a different problem than Jetstream. Tap helps manage cursors via webhooks and ACKs, whereas jetstream really is just about archiving and forwarding the whole network. Tap will eventually be upgraded to support jetstream as an upstream data source (in addition to the current full-fat firehose mode).
> 
1. **What about permissioned data?**

> This remains to be seen as the permissioned data sync mechanism is designed and implemented. If there is a use-case for jetstream and permissioned data, we will strongly consider adding support for it.
> 
1. **What about off-protocol data such as the data currently stored in bsync (mutes, bookmarks, etc.)?**

> Off-protocol data is not a concern of this project. There is no support for it.
> 
1. **Why not build a more robust HA mechanism that's hands-off like kafka?**

> This system is already fairly complex. Adding a distributed consensus protocol like raft to do multi-master HA is more complexity than we feel we can handle given the team and time constraints.
> 

> I'm a big fan of active/active systems over active/passive, but we need to make tradeoffs so we can ship quickly and without bugs.
> 

## 10. References and Further Reading

1. https://atproto.com/specs/sync
2. https://atproto.com/specs/event-stream
3. https://atproto.com/specs/repository
4. https://atproto.com/specs/account
5. https://github.com/bluesky-social/jetstream
6. https://github.com/bluesky-social/indigo/tree/main/cmd/relay
7. https://github.com/bluesky-social/indigo/tree/main/cmd/tap
8. https://github.com/jcalabro/gloom

The existing Jetstream HTTP API surface is maintained for backward
compatibility with Jetstream v1 clients. New control-plane APIs should be XRPC,
and existing HTTP endpoints should gain XRPC equivalents before the legacy
surface expands.
